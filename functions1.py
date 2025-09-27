import io
import os
import time
import json
import pathlib
import requests
import pandas as pd
from config import CACHE_DIR, START, DEBUG


def cache_path( ticker: str) -> pathlib.Path:
    safe_ticker = ticker.replace("/", "_").replace(":", "_").replace(" ", "_")
    fname = f"{safe_ticker}.csv"
    return CACHE_DIR / fname

def is_fresh(path: pathlib.Path, max_age: int) -> bool:
    if not path.exists():
        print(f"Cache file {path} does not exist")
        return False
    age_hours = (time.time() - path.stat().st_mtime) / 3600
    # print(f"Cache file {path} age: {age_hours:.2f} hours (max age {max_age} hours)")
    result = age_hours <= max_age
    return result

def looks_like_json(payload: bytes) -> bool:
    return payload.lstrip()[:1] in (b"{", b"[")

def fetch_csv_robust(url: str, params: dict, ticker: str, max_age: int = 24) -> pd.DataFrame:
    """
        Robust CSV fetch with:
      - on-disk cache (TTL),
      - JSON throttle/error detection (does NOT overwrite cache),
      - atomic write on success.
        Returns a parsed DataFrame (index on first column).
        """
    def check_start_date(df):
        earliest = df.index.min().date()
        # print('data start date:', earliest)
        # print('required START:', START)
        gap = (earliest - pd.to_datetime(START).date()).days
        # print('gap days:', gap)

        # if gap != 0:
        #     print('required START:', START,'\ndata start:', earliest)
        if gap > 5:
            print(f"WARNING: {ticker} data starts at {earliest}, after global start {START}")
    # print(f'params: {params}')
    path = cache_path( ticker)

    # if cache is fresh return it
    if is_fresh(path, max_age):
        # print(f"{ticker} - using cached data")
        df = pd.read_csv(path, header=0, parse_dates=[0], index_col=0).sort_index()
        check_start_date(df)
        return df
    print(f"{ticker} - downloading fresh data")
    resp = requests.get(url, params=params)
        
    resp.raise_for_status()
    raw = resp.content


    # Detect JSON throttle/error; do not poison cache
    if looks_like_json(raw):
        # Try to show a concise message
        try:
            msg = json.loads(raw.decode("utf-8", errors="ignore"))
        except Exception:
            msg = {"body_head": raw[:200].decode("utf-8", errors="ignore")}
        raise RuntimeError(f"... returned JSON (throttle/error) for :{ticker} -> {str(msg)[:180]}")

    # Parse CSV and normalize
    df = pd.read_csv(io.BytesIO(raw), header=0, parse_dates=[0], index_col=0).sort_index()
    check_start_date(df)
    print('saving ', path)
    # Atomic-ish write
    tmp = path.with_suffix(".tmp")
    with open(tmp, "wb") as f:
        f.write(raw)
    os.replace(tmp, path)
    return df

def pick_close_column(df: pd.DataFrame) -> pd.Series:
    """Pick the most appropriate close-like column.
    Prefer adjusted_close when present; else fall back to close. Case-insensitive.
    Returns a float Series.
    """
    if df is None or df.empty:
        raise ValueError("Empty DataFrame passed to pick_close_column")
    colmap = {c.lower(): c for c in df.columns}
    s = None
    if "adjusted_close" in colmap:
        s = df[colmap["adjusted_close"]]
        # If close also present and differs, keep adjusted_close but note it
        if "close" in colmap:
            try:
                a = pd.to_numeric(s, errors="coerce")
                b = pd.to_numeric(df[colmap["close"]], errors="coerce")
                if not a.fillna(method="ffill").equals(b.fillna(method="ffill")):
                    print("note: adjusted_close != close; using adjusted_close")
            except Exception:
                pass
    elif "adj_close" in colmap:
        s = df[colmap["adj_close"]]
    elif "close" in colmap:
        s = df[colmap["close"]]
    else:
        raise ValueError("DataFrame does not contain 'adjusted_close' or 'close' column.")
    return pd.to_numeric(s, errors="coerce")


def sort_cols(df):
    """Normalize time index and return a float close-like Series.
    """
    if not df.index.is_monotonic_increasing: 
        print('index wasnt sorted')
        df = df.sort_index()
    df = df[~df.index.duplicated(keep='last')]
    df = df[df.index.dayofweek < 5] # ?????
    df.index = pd.to_datetime(df.index)
    # s = pick_close_column(df).astype('float64')
    return df['Adjusted_close'].astype('float64')


def shift_usd_fx_next_day(fx_series: pd.Series) -> pd.Series:
    """
    Given a daily USD/CHF Series (index is dates), shift by -1 so that
    the value at date T comes from T+1. Leaves non-USD series unchanged
    if you choose to guard externally by currency.
    """
    if not isinstance(fx_series, pd.Series):
        raise TypeError("fx_series must be a pandas Series")
    return fx_series.shift(-1)


