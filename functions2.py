import os, io, time, json, hashlib, pathlib, sys
import requests
import pandas as pd
import numpy as np
from dotenv import load_dotenv
import re
import matplotlib.pyplot as plt
from urllib.parse import urlparse
from datetime import datetime, timedelta
from config import *
from functions1 import *

def make_fx_map(url, holdings, params, max_age, no_fx, usd_shift) -> dict[str, pd.Series]   :
    # Pre-fetch FX once per currency (excluding CHF)
    fx_map: dict[str, pd.Series] = {}
    needed_ccy = sorted({h["ccy"].upper() for h in holdings if h["ccy"].upper() != "CHF"})
    print('-------------fetching currencies-------------')
    for ccy in needed_ccy:
        ticker = f'{ccy}CHF.FOREX'
        # Fetch EODHD daily FX and build a Series
        fx_df = fetch_csv_robust(f'{url}{ticker}', params=params, ticker=ticker, max_age=max_age)
        # Normalize and pick close
        fx_s = sort_cols(fx_df).rename(f"{ccy}CHF")
        if (ccy == "USD" and not no_fx and usd_shift):
            fx_s = shift_usd_fx_next_day(fx_s)
            print("    Applied USDCHF T+1 shift")
        fx_map[f'{ccy}CHF'] = fx_s
    return fx_map


def deal_with_cash(ccy, fx_map, lookback_days):
    if ccy == 'CHF':
        if fx_map:
            # print('fx_map')
            idx = max((s.index for s in fx_map.values()), key=len)
            # print(f'idx length {len(idx)}')
        else:
            idx = pd.date_range(end=pd.to_datetime(datetime.now().strftime('%Y-%m-%d')), periods=lookback_days, freq="B")
        # return a constant series all 1's
        cash_series= pd.Series(1.0, index=idx, name = "CASH_CHF")
        # print(f'length cash_series {len(cash_series)}   ')
        return cash_series
    else:
        cash_series = fx_map.get(f'{ccy}CHF').copy().rename(f'CASH_{ccy}')
        return cash_series


    
def create_asset_close_chf_s(asset_close_local_s: pd.Series, holding: dict, fx_map: dict, no_fx: bool) -> pd.DataFrame:
    '''
    take the holding local series and return a chf series
    this is for risk calc, so ERNS should not have FX applied, otherwise its takes on currency vol - really
    '''
    name = holding["name"]
    ccy = holding["ccy"].upper()
    chf_close_s = pd.Series()
    last_asset_close = {}
    gbx   = holding.get("gbx",False)
    assert isinstance(gbx, bool), f"gbx should be bool, got {type(gbx)}"
    if gbx:
        asset_close_local_s= asset_close_local_s/ 100.0
    assert float(asset_close_local_s.median()) < 1000, f"Looks like pence still (median {float(asset_close_local_s.median())}); did you divide by 100?"
    if 'VEU.US' in name:
        asset_close_chf_s= asset_close_local_s* 0.9
        print('VEU (XWMX proxy) * 0.9 to remove EM element')
    last_asset_close[name] = asset_close_local_s.iloc[-1]
    # DONT CONVERT FOR CHF CASH, OR IF NO_FX FLAG SET, OR IF RISK_FX SET TO 'NONE'
    if (ccy == "CHF") or no_fx or holding.get('risk_fx', '') == None:
        print("    Skipping FX conversion for", name)
        asset_close_chf_s = asset_close_local_s.rename(name)
    else:
        fx = fx_map[f'{ccy}CHF']
        # Align FX to price dates and forward-fill FX only (never prices)
        before = fx.reindex(asset_close_local_s.index)
        fx_aligned = before.ffill()
        filled = int(before.isna().sum() - fx_aligned.isna().sum())
        if DEBUG and filled > 0:
            print(f"    {name}: FX ffill filled {filled} missing FX points on price dates")
        asset_close_chf_s = (asset_close_local_s * fx_aligned).dropna().rename(name)
    return asset_close_chf_s

def fetch_asset_series(url: str, holding: dict, fx_map: dict, params: dict, max_age: int, lookback_days: int) -> pd.Series:

    ccy   = holding["ccy"].upper()
    # gbx   = h["gbx"]
    # HANDLE CASH AS SPECIAL CASE
    if holding.get("type", "").lower() == "cash":
        return deal_with_cash(ccy, fx_map, lookback_days)
    else:
        ticker   = holding.get("ticker")
        px_df = fetch_csv_robust(f'{url}{ticker}', params=params, ticker=ticker, max_age=max_age)
        # Normalize, de-dup, and pick close-like series
        asset_close_local_s = sort_cols(px_df)
        return asset_close_local_s
    
def get_holding_value_chf(h: dict, fx_map: dict, assets_close_local_df: pd.DataFrame, assets_close_chf_df: pd.DataFrame, asof: str) -> float:
        name = h['name']
        typ = h.get('type', '').lower()
        ccy = h.get('ccy', '').upper()
        position = float(h.get('position', 0.0))
        # CASH ASSETS
        if 'CASH' in name:
        # if typ == 'cash':
            if ccy == 'CHF':
                return float(h['amount'])
            else:
                pair = f"{ccy}CHF"
                fx = fx_map.get(pair, pd.Series(dtype=float))
                if fx.empty:
                    raise KeyError(f"Missing FX series {pair} for cash {name}")
                # ALIGN FX TO PORTFOLIO AS-OF DATE AND FFILL FX ONLY
                last_fx = fx.reindex([asof]).ffill().iloc[-1]
                if np.isnan(last_fx):
                    raise ValueError(f"FX {pair} has no value on/before {asof} for cash {name}")
                return float(h['amount']) * float(last_fx)
        else:
            # NON-CASH ASSETS: ALWAYS VALUE IN CHF AT THE COMMON AS-OF DATE
            if ccy == 'CHF':
                last_local = assets_close_local_df[name].reindex([asof]).iloc[-1]
                return float(last_local) * position
            elif h.get('risk_fx', '') == False:
                # HEDGED IN RISK (KEPT IN LOCAL FOR RETURNS), BUT STILL VALUED IN CHF
                last_local = assets_close_local_df[name].reindex([asof]).iloc[-1]
                pair = f"{ccy}CHF"
                fx = fx_map.get(pair, pd.Series(dtype=float))
                if fx.empty:
                    raise KeyError(f"Missing FX series {pair} for asset {name}")
                last_fx = fx.loc[:asof].iloc[-1]
                if np.isnan(last_fx):
                    raise ValueError(f"FX {pair} has no value on/before {asof} for asset {name}")
                return  float(last_local) * float(last_fx) * position
            else:
                # ALREADY CHF-CONVERTED SERIES (AND ALIGNED) – USE THE AS-OF PRICE
                return float(assets_close_chf_df[name].reindex([asof]).iloc[-1]) * position
            
def _log_returns(s: pd.Series) -> pd.Series:
    return np.log(s).diff()

def standardize_fx_daily_index(s: pd.Series) -> pd.Series:
    """Ensure Mon–Fri daily bars. Your index is date-only; drop Sundays/Saturdays."""
    s = s.sort_index().astype(float).copy()
    s.index = pd.to_datetime(s.index)
    # Monday=0 ... Sunday=6; keep 0..4
    s = s[s.index.dayofweek < 5]
    # if provider emitted duplicates, keep last
    s = s[~s.index.duplicated(keep='last')]
    return s