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
import functions1
import config

def make_fx_map(holdings, params, no_fx, usd_shift) -> dict[str, pd.Series]   :
    # Pre-fetch FX once per currency (excluding CHF)
    fx_map: dict[str, pd.Series] = {}
    needed_ccy = sorted({h["ccy"].upper() for h in holdings if h["ccy"].upper() != "CHF"})
    # print('-------------fetching currencies-------------')
    for ccy in needed_ccy:
        ticker = f'{ccy}CHF.FOREX'
        # Fetch EODHD daily FX and build a Series
        fx_df = functions1.fetch_csv_robust( params=params, ticker=ticker)
        # Normalize and pick close
        fx_s = functions1.sort_cols(fx_df).rename(f"{ccy}CHF")
        if (ccy == "USD" and not no_fx and usd_shift):
            fx_s = functions1.shift_usd_fx_next_day(fx_s)
            print("    Applied USDCHF T+1 shift")
        fx_map[f'{ccy}CHF'] = fx_s
    return fx_map


def deal_with_cash(ccy, fx_map, window_start, window_end):
    if ccy == 'CHF':
        if fx_map:
            # print('fx_map')
            idx = max((s.index for s in fx_map.values()), key=len)
            if window_start:
                idx = idx[idx >= pd.to_datetime(window_start)]
            if window_end:
                idx = idx[idx <= pd.to_datetime(window_end)]
            # print(f'idx length {len(idx)}')
        else:
            print('no fx_map in deal with cash()')
            # idx = pd.date_range(end=pd.to_datetime(datetime.now().strftime('%Y-%m-%d')), periods=lookback_days, freq="B")
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

def fetch_asset_series( holding: dict, fx_map: dict, params: dict, max_age: int, lookback_days: int) -> pd.Series:

    ccy   = holding["ccy"].upper()
    # gbx   = h["gbx"]
    # HANDLE CASH AS SPECIAL CASE
    if holding.get("type", "").lower() == "cash":
        return deal_with_cash(ccy, fx_map, lookback_days)
    else:
        ticker   = holding.get("ticker")
        px_df = fetch_csv_robust(ticker, params=params, max_age=max_age)
        # Normalize, de-dup, and pick close-like series
        asset_close_local_s = sort_cols(px_df)
        return asset_close_local_s
    
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



def _k_confirm(signal: pd.Series, k: int) -> pd.Series:
    if k <= 1:
        return signal.astype(bool)
    return (
        signal.astype("int8")
        .rolling(k, min_periods=k)
        .sum()
        .ge(k)
        .astype(bool)
    )

def _compose_entry_signal(slope: pd.Series,
                          slope_entry_threshold: float,
                          carry_ok: pd.Series,
                          require_carry: bool) -> pd.Series:
    slope_down = (slope <= slope_entry_threshold)
    return slope_down & (carry_ok if require_carry else True)

def _compose_reconfirm(slope: pd.Series,
                       slope_exit_threshold: float,
                       carry_ok: pd.Series,
                       require_carry: bool) -> pd.Series:
    # Require down (or strictly below exit threshold) slope to maintain position.
    slope_still_down = slope < slope_exit_threshold
    return slope_still_down & (carry_ok if require_carry else True)

# function to trim series to specified dates
def trim_series(s: pd.Series, start: str, end: str) -> pd.Series:
    if start:
        s = s[s.index >= pd.to_datetime(start)]
    if end:
        s = s[s.index <= pd.to_datetime(end)]
    return s

def get_window_dates(s: pd.Series) -> Tuple[pd.Timestamp, pd.Timestamp]:
    end_date = s.index[-1]
    start_date = s.index[1]
    return start_date, end_date

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