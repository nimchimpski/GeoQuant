import io
import os
import time
import json
import pathlib
import requests
import pandas as pd
from config import *
import functions2 as f2


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

def check_start_date(df, ticker: str, start: str, ) -> None:
    earliest = df.index.min().date()
    gap = (earliest - pd.to_datetime(start).date()).days
    print('gap days:', gap)
    if gap != 0:
        print('required start:', start,'\ndata start:', earliest)


def _normalize_ohlc_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Return a copy with canonical OHLC column names: Open, High, Low, Close, Adjusted_close (when present).
    Column matching is case-insensitive; raises if Close is missing.
    """
    if df is None or df.empty:
        return df
    colmap = {c.lower(): c for c in df.columns}
    def pick(*names):
        for name in names:
            key = name.lower()
            if key in colmap:
                return colmap[key]
        return None
    c_open  = pick("open")
    c_high  = pick("high")
    c_low   = pick("low")
    c_close = pick("close")
    c_adj   = pick("adjusted_close", "adj_close", "adjustedclose")
    if c_close is None:
        raise ValueError("Downloaded frame does not contain a 'Close' column (case-insensitive).")
    out = df.copy()
    rename = {}
    if c_open:  rename[c_open]  = "Open"
    if c_high:  rename[c_high]  = "High"
    if c_low:   rename[c_low]   = "Low"
    if c_close: rename[c_close] = "Close"
    if c_adj:   rename[c_adj]   = "Adjusted_close"
    out = out.rename(columns=rename)
    return out

def clean_ohlc_flatbar_spikes(df: pd.DataFrame, *, ret_spike: float = 0.10, eps: float = 1e-6,
                              update_adjusted: bool = True) -> tuple[pd.DataFrame, list]:
    """Clean vendor bad prints at the raw OHLC level.

    Rules (simple and conservative):
    - Identify days where the bar is flat: Open==High==Low==Close (within eps), AND
      the jump vs prior Close is large (>= ret_spike in absolute value).
    - Optionally force-include specific dates via `force_dates`.
    - Repair only those days by interpolating Close in time; set Open/High/Low of the
      repaired day equal to the repaired Close to keep the bar consistent. Optionally
      update Adjusted_close to match Close.

    Returns: (cleaned_df, changes)
      where changes is a list of tuples (timestamp, old_close, new_close).
    """
    if df is None or df.empty:
        return df, []

    # Normalize index and columns
    out = df.copy()
    if not isinstance(out.index, pd.DatetimeIndex):
        try:
            out.index = pd.to_datetime(out.index)
        except Exception:
            pass
    out = out.sort_index()
    out = _normalize_ohlc_columns(out)

    # Require Close
    if "Close" not in out.columns:
        return out, []

    close = pd.to_numeric(out["Close"], errors="coerce")
    prev_close = close.shift(1)
    ret = (close / prev_close) - 1

    # Flat bar predicate: all of High/Low/Open/Close within tolerance.
    flat = (
        (out.get("High", close) - out.get("Low", close)).abs() < eps
    ) & (
        (out.get("Open", close) - out.get("Close", close)).abs() < eps
    ) & (
        (out.get("High", close) - out.get("Open", close)).abs() < eps
    )

    candidates = flat & (ret.abs() >= float(ret_spike))

    spike_days = candidates[candidates.fillna(False)].index
    if len(spike_days) == 0:
        return out, []

    # Interpolate Close on spike days
    cleaned = out.copy()
    changes: list = []
    s = close.copy()
    for dt in spike_days:
        if dt in s.index:
            old = s.loc[dt]
            s.loc[dt] = pd.NA
            changes.append((dt, old))

    s = s.astype("float64").interpolate(method="time", limit=2).bfill().ffill()

    # Apply repaired Close and propagate to O/H/L for flagged days
    cleaned.loc[spike_days, "Close"] = s.loc[spike_days].values
    if "Open" in cleaned.columns:
        cleaned.loc[spike_days, "Open"] = s.loc[spike_days].values
    if "High" in cleaned.columns:
        cleaned.loc[spike_days, "High"] = s.loc[spike_days].values
    if "Low" in cleaned.columns:
        cleaned.loc[spike_days, "Low"] = s.loc[spike_days].values
    if update_adjusted and "Adjusted_close" in cleaned.columns:
        cleaned.loc[spike_days, "Adjusted_close"] = s.loc[spike_days].values

    # Build change records with new values
    changes = [(dt, float(old), float(cleaned.loc[dt, "Close"])) for (dt, old) in changes]
    return cleaned, changes

def build_url(datasource: str, ticker: str, params: dict) -> str:
    start = params['start']
    end = params['end']
    if datasource == 'stooq':
        # remove '-' from start and end date formatting'
        start = start.replace('-', '')
        end = end.replace('-', '')

        url = f"https://stooq.com/q/d/l/?s={ticker}&d1={start}&d2={end}&i=d"
    elif datasource == 'eodhd':
        url = f'https://eodhd.com/api/eod/{ticker}?api_token={params["api_token"]}&from={start}&to={end}'
    else:
        raise ValueError(f"Unsupported datasource: {datasource}")
    return url

# def fetch_csv_robust1(ticker: str, params: dict=None) -> pd.DataFrame:
    """
        Robust CSV fetch with:
      - on-disk cache (TTL),
      - JSON throttle/error detection (does NOT overwrite cache),
      - atomic write on success.
        Returns a parsed DataFrame (index on first column).
        """
    # start = params['from']
    start = params['from']
    datasource = params['datasource']
    url = build_url(datasource)
    # url = f'{url}{ticker}'
    max_age = params['max_age']
    

    # print(f'params: {params}')
    path = cache_path( ticker)

    # if cache is fresh return it (optionally post-clean)
    if is_fresh(path, max_age):
        # print(f"{ticker} - using cached data")
        df = pd.read_csv(path, header=0, parse_dates=[0], index_col=0).sort_index()
        check_start_date(df, ticker, start)

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
    check_start_date(df, ticker, start)
    # Optional cleaning on fresh download
    do_clean = True
    if do_clean:
        cleaned, changes = clean_ohlc_flatbar_spikes(
            df,
            ret_spike = 0.10,
        )
        if changes:
            print(f"{ticker} - cleaned {len(changes)} flat-bar spike(s) on download")
        df_to_save = cleaned
    else:
        df_to_save = df
    print('data start date b4 saving:', df_to_save.index.min().date())
    # Atomic-ish write: save the (possibly cleaned) CSV
    tmp = path.with_suffix(".tmp")
    df_to_save.to_csv(tmp, date_format="%Y-%m-%d")
    os.replace(tmp, path)
    return df_to_save

def url_builder(datasource: str, ticker: str, params: dict) -> str:
    start = params['start']
    start = pd.to_datetime(start).strftime('%Y%m%d')
    end = params['end']
    end = pd.to_datetime(end).strftime('%Y%m%d')
    if datasource == 'stooq':
        url = f"https://stooq.com/q/d/l/?s={ticker}&d1={start}&d2={end}&i=d"
    elif datasource == 'eodhd':
        url = f'https://eodhd.com/api/eod/{ticker}?api_token={params["api_token"]}&from={start}&to={end}'
    else:
        raise ValueError(f"Unsupported datasource: {datasource}")
    return url

def fetch_csv_robust(ticker: str, params: dict=None) -> pd.DataFrame:
    """
        Robust CSV fetch with:
      - on-disk cache (TTL),
      - JSON throttle/error detection (does NOT overwrite cache),
      - atomic write on success.
        Returns a parsed DataFrame (index on first column).
        """
    # start = params['from']
    start = params['start']
    datasource = params['datasource']
    url = url_builder(datasource, ticker, params)

    # url = f'{url}{ticker}'
    max_age = params['max_age']
    

    # print(f'params: {params}')
    path = cache_path( ticker)

    # if cache is fresh return it (optionally post-clean)
    if is_fresh(path, max_age):
        # print(f"{ticker} - using cached data")
        df = pd.read_csv(path, header=0, parse_dates=[0], index_col=0).sort_index()

        check_start_date(df, ticker, start)
        return df

    print(f"{ticker} - downloading fresh data")

    # GET THE DATA
    resp = requests.get(url)
    resp.raise_for_status()
    #  EXAMINE RAW BYTES TO SEE IF THERE IS A PROBLEM
    raw = resp.content

    # Detect JSON throttle/error; do not poison cache
    if looks_like_json(raw):
        # Try to show a concise message
        try:
            msg = json.loads(raw.decode("utf-8", errors="ignore"))
        except Exception:
            msg = {"body_head": raw[:200].decode("utf-8", errors="ignore")}
        raise RuntimeError(f"... returned JSON (throttle/error) for :{ticker} -> {str(msg)[:180]}")
    
    # PAYLOAD CHECKS
    if not raw or len(raw) < 20:
        raise ValueError(f"{ticker}: empty/tiny payload")
    head = raw[:200].lstrip().lower()
    if b"<html" in head:
        raise ValueError(f"{ticker}: got HTML page, not CSV")

    # TURN THE RAW BYTES INTO A DATAFRAME
    try:
        df = pd.read_csv(io.BytesIO(raw), header=0, parse_dates=[0], index_col=0).sort_index()
    except Exception as e:
        raise ValueError(f"{ticker}: failed to parse CSV -> {e}")
    df.index = pd.to_datetime(df.index, errors="coerce")
    df = df.sort_index()

    # df['Date'] = pd.to_datetime(df['Date'])
    # df = df.sort_values('Date').set_index('Date')

    check_start_date(df, ticker, start)
    # Optional cleaning on fresh download
    do_clean = True
    if do_clean:
        cleaned, changes = clean_ohlc_flatbar_spikes(
            df,
            ret_spike = 0.10,
        )
        if changes:
            print(f"{ticker} - cleaned {len(changes)} flat-bar spike(s) on download")
        df_to_save = cleaned
    else:
        df_to_save = df
    # print('data start date b4 saving:', df_to_save.index.min().date())
    # ATOMIC-ISH WRITE: SAVE THE (POSSIBLY CLEANED) CSV
    tmp = path.with_suffix(".tmp")
    # TURN DF INTO CSV FOR SAVING
    df_to_save.to_csv(tmp, date_format="%Y-%m-%d")
    os.replace(tmp, path)
    return df_to_save

def pick_close_column(df: pd.DataFrame) -> pd.Series:
    """Pick the most appropriate close-like column.
    Prefer adjusted_close when present; else fall back to close. Case-insensitive.
    Returns a float Series.
    """
    if df is None or df.empty:
        raise ValueError("Empty DataFrame passed to pick_close_column")
    colmap = {c.lower(): c for c in df.columns}

    s = df[colmap["close"]]

    return pd.to_numeric(s, errors="coerce")


def sort_cols(df, ohlc=None):
    """Normalize time index and return a float close-like Series.
    """
    if ohlc is None:
        print('sort_cols: ohlc not set. True only needed for ATR calculations for vol stops. Defaulting to False')
    if not df.index.is_monotonic_increasing: 
        print('sort_cols: index wasnt sorted')
        df = df.sort_index()
    
    df = df[~df.index.duplicated(keep='last')]
    df = df[df.index.dayofweek < 5] # ?????
    df.index = pd.to_datetime(df.index)
    if ohlc:
        return df.astype('float64')
    else:
        adjclose_s = pick_close_column(df).astype('float64')
        return adjclose_s



def shift_usd_fx_next_day(fx_series: pd.Series) -> pd.Series:
    """
    Given a daily USD/CHF Series (index is dates), shift by -1 so that
    the value at date T comes from T+1. Leaves non-USD series unchanged
    if you choose to guard externally by currency.
    """
    # if not isinstance(fx_series, pd.Series):
    #     raise TypeError("fx_series must be a pandas Series")
    return fx_series.shift(-1)

