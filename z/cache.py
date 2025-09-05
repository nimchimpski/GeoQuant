# NAMING SCHEME: cach/{function}_{symbol or pair}
import os, io, time, hashlib, datetime as dt
import pandas as pd
import requests
from pathlib import Path
from dotenv import load_dotenv
import os
load_dotenv()

api_key = os.getenv("api_key")

CACHE_DIR =  Path("cache")
CACHE_DIR.mkdir(exist_ok=True)

def _cache_path(function: str, key: str) ->  Path:
    # Keep names readable; hash only if very long.
    safe_key = key.replace("/", "").replace(":", "").replace(" ", "_")
    fname = f"{function}_{safe_key}.csv"
    return CACHE_DIR / fname

def _is_fresh(path:  Path, ttl_hours: int) -> bool:
    if not path.exists():
        return False
    age = (dt.datetime.now() - dt.datetime.fromtimestamp(path.stat().st_mtime)).total_seconds() / 3600.0
    return age <= ttl_hours

def _looks_like_json(payload: bytes) -> bool:
    return payload.lstrip()[:1] in (b"{", b"[")

def _valid_columns(df: pd.DataFrame, expected: list[str]) -> bool:
    return all(col in df.columns for col in expected)

def fetch_csv_cached(url: str, function: str, key: str, ttl_hours: int, expected_cols: list[str]) -> pd.DataFrame:
    """Download CSV with TTL cache; validate before overwriting cache."""
    path = _cache_path(function, key)
    # print(f"Using cache path: {path}")

    # Serve fresh cache if valid and not stale
    if _is_fresh(path, ttl_hours):
        # print(f"Using cached {key} copy")
        df = pd.read_csv(path, parse_dates=["timestamp"])
        if _valid_columns(df, expected_cols):
            return df

    # Otherwise, Fetch from network
    print("Fetching from network")
    r = requests.get(url, timeout=30)
    r.raise_for_status()
    raw = r.content

    # If it smells like JSON (rate limit / error), fall back to stale cache if present
    if _looks_like_json(raw):
        print(f"Warning: fetched data looks like JSON, not CSV. URL: {key}")
        if path.exists():  # use last good copy, even if stale
            df = pd.read_csv(path, parse_dates=["timestamp"])
            if _valid_columns(df, expected_cols):
                return df
        # No usable cache – surface the API message
        raise RuntimeError(f"Alpha Vantage returned JSON (likely rate-limit). Body head: {raw[:200]!r}")

    # Parse CSV
    df = pd.read_csv(io.BytesIO(raw), parse_dates=["timestamp"])
    if not _valid_columns(df, expected_cols):
        # Don’t poison cache with malformed CSV
        raise RuntimeError(f"Unexpected CSV columns: got {df.columns.tolist()}, expected at least {expected_cols}")

    # Atomic write
    tmp = path.with_suffix(".tmp")
    with open(tmp, "wb") as f: # wb = write bytes
        f.write(raw)
    os.replace(tmp, path)

    return df

