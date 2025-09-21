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
            print('fx_map')
            idx = max((s.index for s in fx_map.values()), key=len)
            print(f'idx length {len(idx)}')
        else:
            idx = pd.date_range(end=pd.to_datetime(datetime.now().strftime('%Y-%m-%d')), periods=lookback_days, freq="B")
        # return a constant series all 1's
        cash_series= pd.Series(1.0, index=idx, name = "CASH_CHF")
        print(f'length cash_series {len(cash_series)}   ')
        return cash_series
    else:
        cash_series = fx_map.get(f'{ccy}CHF').copy().rename(f'CASH_{ccy}')
        return cash_series

def fetch_asset_series(url: str, holding: dict, fx_map: dict, params: dict, max_age: int, lookback_days: int) -> pd.Series:
    ticker   = holding["ticker"]
    name  = holding["name"]
    ccy   = holding["ccy"].upper()
    # gbx   = h["gbx"]
    # HANDLE CASH AS SPECIAL CASE
    if holding.get("type", "").lower() == "cash":
        return deal_with_cash(ccy, fx_map, lookback_days)
    else:
        px_df = fetch_csv_robust(f'{url}{ticker}', params=params, ticker=ticker, max_age=max_age)
        # Normalize, de-dup, and pick close-like series
        asset_close_local_s = sort_cols(px_df)
        return asset_close_local_s