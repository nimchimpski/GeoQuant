import io
import os
import time
import json
import hashlib
import pathlib
import requests
import pandas as pd
from datetime import datetime, timezone, timedelta
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
    start = pd.to_datetime(params['start']).strftime('%Y%m%d')
    end = pd.to_datetime(params['end']).strftime('%Y%m%d')
    if datasource == 'stooq':
        url = f"https://stooq.com/q/d/l/?s={ticker}&d1={start}&d2={end}&i=d"
    elif datasource == 'eodhd':
        url = f'https://eodhd.com/api/eod/{ticker}?api_token={params["api_token"]}&from={start}&to={end}'
    else:
        raise ValueError(f"Unsupported datasource: {datasource}")
    return url


def build_runtime_config(
    base_params: dict,
    *,
    run_mode: str,
    ticker: str,
    window_start: str,
    window_end: str | None = None,
    max_age_research: float = 22.0,
    max_age_production: float = 0.0,
) -> tuple[dict, dict]:
    """Build explicit runtime params and deterministic run metadata."""
    if run_mode not in {"research", "production"}:
        raise ValueError("run_mode must be 'research' or 'production'")

    max_age = max_age_research if run_mode == "research" else max_age_production
    force_refresh = run_mode == "production"

    runtime_params = dict(base_params)
    runtime_params["max_age"] = float(max_age)
    runtime_params["start"] = window_start
    if window_end is not None:
        runtime_params["end"] = window_end

    run_meta = {
        "run_mode": run_mode,
        "ticker": ticker,
        "window_start": window_start,
        "window_end": window_end,
        "max_age": float(max_age),
        "force_refresh": force_refresh,
        "created_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    }
    run_meta["run_id"] = hashlib.sha1(
        json.dumps(run_meta, sort_keys=True).encode("utf-8")
    ).hexdigest()[:12]

    return runtime_params, run_meta


def cache_meta_path(ticker: str) -> pathlib.Path:
    """Sidecar metadata file used to track refresh cadence for each cached ticker."""
    return cache_path(ticker).with_suffix(".meta.json")


def print_cache_status(ticker: str) -> dict:
    """Print a compact cache status banner and return parsed fields."""
    meta_path = cache_meta_path(ticker)
    if not meta_path.exists():
        print("CACHE STATUS [MISSING]", ticker)
        print("no cache sidecar metadata found for", ticker)
        return {
            "status": "MISSING",
            "mode": None,
            "last_update_utc": None,
            "last_full_refresh_utc": None,
            "meta_path": str(meta_path),
        }

    meta = _read_cache_meta(ticker)
    mode = str(meta.get("last_update_mode") or "unknown")
    last_update = meta.get("last_update_utc")
    last_full = meta.get("last_full_refresh_utc")

    if mode == "full":
        status = "OK"
    elif mode in {"incremental", "incremental_noop"}:
        status = "WARN"
    else:
        status = "UNKNOWN"

    print(f"CACHE STATUS [{status}] {ticker}")
    print("cache meta path:", meta_path)
    print("last_update_mode:", mode)
    print("last_update_utc:", last_update)
    print("last_full_refresh_utc:", last_full)

    return {
        "status": status,
        "mode": mode,
        "last_update_utc": last_update,
        "last_full_refresh_utc": last_full,
        "meta_path": str(meta_path),
    }


def _read_cache_meta(ticker: str) -> dict:
    p = cache_meta_path(ticker)
    if not p.exists():
        return {}
    try:
        with open(p, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _write_cache_meta(ticker: str, meta: dict) -> None:
    p = cache_meta_path(ticker)
    tmp = p.with_suffix(".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(meta, f, indent=2, sort_keys=True)
    os.replace(tmp, p)


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _parse_iso_utc(ts: str | None) -> datetime | None:
    if not ts:
        return None
    try:
        return datetime.fromisoformat(ts.replace("Z", "+00:00")).astimezone(timezone.utc)
    except Exception:
        return None


def _is_monthly_full_refresh_due(meta: dict, interval_days: int = 30) -> bool:
    last = _parse_iso_utc(meta.get("last_full_refresh_utc"))
    if last is None:
        return True
    return (datetime.now(timezone.utc) - last) >= timedelta(days=interval_days)


def _download_csv_frame(url: str, ticker: str) -> pd.DataFrame:
    """Download and parse CSV payload from provider endpoint."""
    resp = requests.get(url)
    resp.raise_for_status()
    raw = resp.content

    if looks_like_json(raw):
        try:
            msg = json.loads(raw.decode("utf-8", errors="ignore"))
        except Exception:
            msg = {"body_head": raw[:200].decode("utf-8", errors="ignore")}
        raise RuntimeError(f"... returned JSON (throttle/error) for :{ticker} -> {str(msg)[:180]}")

    if not raw or len(raw) < 20:
        raise ValueError(f"{ticker}: empty/tiny payload")

    head = raw[:200].lstrip().lower()
    if b"<html" in head:
        raise ValueError(f"{ticker}: got HTML page, not CSV")

    try:
        df = pd.read_csv(io.BytesIO(raw), header=0, parse_dates=[0], index_col=0).sort_index()
    except Exception as e:
        raise ValueError(f"{ticker}: failed to parse CSV -> {e}")

    df.index = pd.to_datetime(df.index, errors="coerce")
    df = df[~df.index.isna()].sort_index()
    return df

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
    # Backward-compatible wrapper; keep callers stable while using one implementation.
    return build_url(datasource, ticker, params)

def fetch_csv_robust(ticker: str, params: dict=None, force_refresh: bool = False) -> pd.DataFrame:
    """
        Robust CSV fetch with:
      - on-disk cache (TTL),
      - JSON throttle/error detection (does NOT overwrite cache),
      - atomic write on success.
        Returns a parsed DataFrame (index on first column).
        """
    start = params['start']
    datasource = params['datasource']
    max_age = params['max_age']
    end = params.get('end', datetime.now(timezone.utc).strftime('%Y%m%d'))

    path = cache_path(ticker)
    meta = _read_cache_meta(ticker)
    monthly_full_due = _is_monthly_full_refresh_due(meta, interval_days=30)
    need_full_refresh = force_refresh or (not path.exists()) or monthly_full_due

    if force_refresh and path.exists():
        print(f"{ticker} - force_refresh=True, bypassing cache")

    # Fast path: fresh cache and no mandatory full refresh.
    if (not need_full_refresh) and is_fresh(path, max_age):
        print(f"{ticker} - using cached data")
        df_cached = pd.read_csv(path, header=0, parse_dates=[0], index_col=0).sort_index()
        check_start_date(df_cached, ticker, start)
        return df_cached

    # Decide between full refresh and incremental merge.
    if need_full_refresh:
        params_full = dict(params)
        params_full['start'] = start
        params_full['end'] = end
        print(f"{ticker} - monthly/forced full refresh")
        df_raw = _download_csv_frame(build_url(datasource, ticker, params_full), ticker)
        mode = 'full'
    else:
        # Incremental update: fetch from day after cached max date to current end.
        df_cached = pd.read_csv(path, header=0, parse_dates=[0], index_col=0).sort_index()
        if df_cached.empty:
            params_full = dict(params)
            params_full['start'] = start
            params_full['end'] = end
            print(f"{ticker} - cache empty, fallback full refresh")
            df_raw = _download_csv_frame(build_url(datasource, ticker, params_full), ticker)
            mode = 'full'
        else:
            last_cached = pd.to_datetime(df_cached.index.max())
            inc_start_dt = (last_cached + pd.Timedelta(days=1)).date()
            inc_end_dt = pd.to_datetime(end).date()

            if inc_start_dt > inc_end_dt:
                print(f"{ticker} - no new dates to fetch; serving cache")
                check_start_date(df_cached, ticker, start)
                return df_cached

            params_inc = dict(params)
            params_inc['start'] = str(inc_start_dt)
            params_inc['end'] = str(inc_end_dt)
            print(f"{ticker} - incremental update {params_inc['start']} -> {params_inc['end']}")
            try:
                df_inc = _download_csv_frame(build_url(datasource, ticker, params_inc), ticker)
            except ValueError as exc:
                # Some providers return empty payloads for same-day requests before EOD print.
                if 'empty/tiny payload' in str(exc):
                    print(f"{ticker} - incremental fetch returned no rows; serving cache")
                    now_iso = _utc_now_iso()
                    meta['last_update_utc'] = now_iso
                    meta['last_update_mode'] = 'incremental_noop'
                    _write_cache_meta(ticker, meta)
                    check_start_date(df_cached, ticker, start)
                    return df_cached
                raise

            df_raw = pd.concat([df_cached, df_inc], axis=0)
            df_raw = df_raw[~df_raw.index.duplicated(keep='last')].sort_index()
            mode = 'incremental'

    check_start_date(df_raw, ticker, start)

    cleaned, changes = clean_ohlc_flatbar_spikes(df_raw, ret_spike=0.10)
    if changes:
        print(f"{ticker} - cleaned {len(changes)} flat-bar spike(s) on {mode} update")
    df_to_save = cleaned

    # Atomic write of merged frame.
    tmp = path.with_suffix(".tmp")
    df_to_save.to_csv(tmp, date_format="%Y-%m-%d")
    os.replace(tmp, path)

    # Persist refresh metadata for monthly full-refresh policy.
    now_iso = _utc_now_iso()
    meta['last_update_utc'] = now_iso
    meta['last_update_mode'] = mode
    if mode == 'full':
        meta['last_full_refresh_utc'] = now_iso
    _write_cache_meta(ticker, meta)

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

