import os, io, time, json, hashlib, pathlib, sys
import requests
import pandas as pd
import numpy as np
from dotenv import load_dotenv
import re
import matplotlib.pyplot as plt
from typing import Tuple, Dict

from urllib.parse import urlparse
from datetime import datetime, timedelta
from config import *
import functions1 as f1
import functions2 as f2
import config

def make_fx_map(holdings, params, no_fx, usd_shift) -> dict[str, pd.Series]   :
    # Pre-fetch FX once per currency (excluding CHF)
    fx_map: dict[str, pd.Series] = {}
    needed_ccy = sorted({h["ccy"].upper() for h in holdings if h["ccy"].upper() != "CHF"})
    # print('-------------fetching currencies-------------')
    for ccy in needed_ccy:
        ticker = f'{ccy}CHF.FOREX'
        # Fetch EODHD daily FX and build a Series
        fx_df = f1.fetch_csv_robust( params=params, ticker=ticker)
        # Normalize and pick close
        fx_s = f1.sort_cols(fx_df).rename(f"{ccy}CHF")
        if (ccy == "USD" and not no_fx and usd_shift):
            fx_s = f1.shift_usd_fx_next_day(fx_s)
            print("    Applied USDCHF T+1 shift")
        fx_map[f'{ccy}CHF'] = fx_s
    return fx_map


def make_cash_series(ccy, fx_map):
    if ccy == 'CHF':
        # MAKE THE CHF 1'S SERIES AT LEAST AS LONG AS THE LONGEST FX SERIES
        idx = max((s.index for s in fx_map.values()), key=len)
        return  pd.Series(1.0, index=idx, name = "CASH_CHF")
    else:
        return  fx_map.get(f'{ccy}CHF').copy().rename(f'CASH_{ccy}')


    
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

    
def get_holding_value_chf(h: dict, fx_map: dict, assets_close_local_df: pd.DataFrame, assets_close_chf_df: pd.DataFrame, asof: str) -> float:
        # print('++++++++in get_holding_value_chf ++++++++')
        # print(f'Valuing {h["name"]} as of {asof}')
        name = h['name']
        typ = h.get('type', '').lower()
        ccy = h.get('ccy', '').upper()
        position = float(h.get('position', 0.0))
        # CASH ASSETS
        if 'CASH' in name:
            # print('    Cash asset')
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
            # print('    Non-cash asset')
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
                # print('    Unhedged in risk, so use CHF series')
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

def norm_risk_fx(val) -> bool:
    """
    Normalize various user inputs to a boolean with this convention:
    - False => strip FX volatility (hedged in risk)
    - True  => include FX volatility (unhedged in risk)
    Defaults to True when missing/unknown.
    """
    if isinstance(val, bool):
        return val
    if val is None:
        return True
    if isinstance(val, str):
        v = val.strip().lower()
        if v in ("none", "hedged", "no", "false", "0", "off"):
            return False
        if v in ("true", "yes", "1", "on", "unhedged"):
            return True
    if isinstance(val, (int, float)):
        return bool(val)
    return True

# function to trim series to specified dates
def trim_series(s: pd.Series, start: str, end: str=None) -> pd.Series:
    if start:
        s = s[s.index >= pd.to_datetime(start)]
    if end:
        s = s[s.index <= pd.to_datetime(end)]
    return s

def get_window_dates(s: pd.Series) -> Tuple[pd.Timestamp, pd.Timestamp]:
    end_date = s.index[-1]
    start_date = s.index[1]
    return start_date, end_date

def get_series(ticker, params=config.params, window_start=None, window_end=None) -> pd.Series:
    s= f1.fetch_csv_robust(params=params, ticker=ticker)
    s = f1.sort_cols(s)
    s = f2.standardize_fx_daily_index(s)
    s = trim_series(s, window_start, window_end)
    return s


def plotter(ticker, prices, gate_stateon=None, TAIL_BARS=1000,):
    plt.style.use('dark_background')   

    # Select tail for plotting
    s_plot = prices.tail(TAIL_BARS) if TAIL_BARS else prices
    fig, ax = plt.subplots(figsize=(11, 6))
    # Base price plot
    s_plot.plot(ax=ax, color='steelblue', lw=1.2, label=ticker)

    if gate_stateon:
 
        # Align gate_state to price index (gate is Mon–Fri too)
        g = gate_stateon.reindex(s_plot.index).fillna(False).astype(bool)
        # print(f'gateon aligned to price (last 20 rows):\n{gate_stateon}')
        # Overlay markers colored by gate_state state on the price series
        colors = np.where(g.values, 'crimson', 'blue')
        ax.scatter(s_plot.index, s_plot.values, c=colors, s=12, zorder=3)
        # Legend: include price and gate_state state keys
        from matplotlib.lines import Line2D
        handles, labels = ax.get_legend_handles_labels()
        gate_true = Line2D([0],[0], marker='o', color='w', label='Gate True', markerfacecolor='crimson', markersize=6)
        gate_false = Line2D([0],[0], marker='o', color='w', label='Gate False', markerfacecolor='blue', markeredgecolor='gray', markersize=6)
        ax.legend(handles + [gate_true, gate_false], labels + ['Gate True','Gate False'], loc='upper left')
        ax.set_title(f'{ticker}CHF with gate_state True/False markers (Mon–Fri)')
    ax.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.show()


def deal_with_gbx(close_local_s: pd.Series, ccy: str, gbx:bool) -> pd.Series:
    if gbx and ccy == 'GBP':
        return close_local_s / 100.0
    return close_local_s

def base_ccy_assets_px_df(holdings, fx_map, params, ohcl):
    # ------------BUILD CHF CLOSE SERIES PER ASSET-------------------


    assets_close_local_df = pd.DataFrame()
    assets_close_chf_df = pd.DataFrame()
    for h in holdings:
        name = h['name']
        ccy = h.get('ccy','').upper()
        gbx   = h.get("gbx",False)
        include_fx = h.get('include_fx_vol', True)
        is_cash = h.get('type', '').lower() == 'cash'

        # MAKE THE LOCAL SERIES
        #  CASH
        if is_cash:
            chf_s = f2.make_cash_series(local_s, ccy, fx_map)
            if not include_fx:
                chf_s = pd.Series(1.0, index=chf_s.index, name=name)

        # DECIDE IF LEAVE IN LOCAL CCY (REMOVE FX VOL)
        #  NOT CASH + fx = true + ccy != chf
        else: 
            ticker   = h.get("ticker")
            px_df = f1.fetch_csv_robust(ticker, params=params)
            local_s = f1.sort_cols(px_df, ohcl) 

            local_s = deal_with_gbx(local_s)
        if ccy != 'CHF' and include_fx:
            fx = fx_map.get(f"{ccy}CHF", pd.Series(dtype=float))
            fx_aligned = fx.reindex(local_s.index).ffill()
            # ensure last value available even if FX lags one or two days
            if fx_aligned.iloc[-1] != fx_aligned.dropna().iloc[-1]:
                fx_aligned.iloc[-1] = fx_aligned.dropna().iloc[-1]

            # MULTIPLY LOCAL BY FX
            chf_s = (local_s * fx_aligned).rename(name)

        else:
             # OTHERWISE INCLUDE FX VOL
            print(f'No FX conversion/vol for {name} ({ccy})')
            chf_s =local_s.rename(name)

        # STORE THE LOCAL AND CHF SERIES
        assets_close_chf_df[name] = chf_s


    # ALIGN ON COMMON DATES
    prices_df = assets_close_chf_df.dropna(how="any")   
    # TRIM
    prices_df = f2.trim_series(prices_df, params['from'], params['to'])
    return prices_df