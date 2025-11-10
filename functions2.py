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

def make_fx_map(holdings, params, usd_shift=True, ohlc=False) -> dict[str, pd.Series]   :
    # Pre-fetch FX once per currency (excluding CHF)
    fx_map: dict[str, pd.Series] = {}
    needed_ccy = sorted({h["ccy"].upper() for h in holdings if h["ccy"].upper() != "CHF"})
    # print('-------------fetching currencies-------------')
    for ccy in needed_ccy:
        ticker = f'{ccy}CHF.FOREX'
        # Fetch EODHD daily FX and build a Series
        fx_df = f1.fetch_csv_robust( params=params, ticker=ticker)
        # Normalize and pick close
        fx_s = f1.sort_cols(fx_df, ohlc)
        if not ohlc:
            fx_s = fx_s.rename(f"{ccy}CHF")
        if (ccy == "USD"  and usd_shift):
            fx_s = f1.shift_usd_fx_next_day(fx_s)
            print("    Applied USDCHF T+1 shift")
        fx_map[f'{ccy}CHF'] = fx_s
    return fx_map


def cash_series(ccy, fx_map):
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
    print('+++Create_asset_close_chf', holding["name"])
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
    if (ccy == "CHF") or no_fx or holding.get('include_fx_vol', '') == True:
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
            elif h.get('include_fx_vol', '') != False:
                # HEDGED IN RISK (KEPT IN LOCAL FOR RETURNS), BUT STILL VALUED IN CHF
                last_local = assets_close_local_df[name].reindex([asof]).iloc[-1]
                if name == 'EMIM':
                    print('asset close chf', name, last_local)
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
                print('ignroe_fx for', name)
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
    print(f'++++ get_series{ticker}')
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

# ...existing imports...

def base_ccy_assets_px_df(holdings, fx_map, params, ohlc=False):
    """
    Build CHF risk series.
    - ohlc=False: returns a single Close-only DataFrame (Adjusted Close preferred)
    - ohlc=True:  returns (high_df, low_df, close_df) using unadjusted OHLC, all CHF-aligned
    Conversion rules:
      * Non-cash: if include_fx and ccy != CHF -> multiply by FX Close; else keep local (no FX vol)
      * Cash:     if include_fx:
                     CHF -> flat 1.0; non-CHF -> FX Close; (OHLC all equal to Close)
                  else flat 1.0 (hedged cash)
    """
    close_cols = {}
    high_cols, low_cols = ({}, {}) if ohlc else (None, None)

    from_dt = params.get("from")
    to_dt = params.get("to")
    local_close_df = pd.DataFrame()
    for h in holdings:
        name = h["name"]
        ccy = h.get("ccy", "").upper()
        is_cash = h.get("type", "").lower() == "cash"
        gbx = bool(h.get("gbx", False))
        include_fx = bool(h.get("include_fx_vol", True))

        if is_cash:
            # Cash path
            if ccy == "CHF":
                # Build a business-day index if possible; will be realigned later anyway
                idx = pd.date_range(from_dt, to_dt, freq="B") if (from_dt and to_dt) else None
                base_close = pd.Series(1.0, index=idx, name=name)
            else:
                fx = fx_map.get(f"{ccy}CHF", pd.DataFrame())
                if fx is None or len(fx) == 0:
                    raise KeyError(f"Missing FX series {ccy}CHF for cash {name}")
                fx_close = fx["Close"] if isinstance(fx, pd.DataFrame) and "Close" in fx.columns else fx
                fx_close = fx_close.astype(float).rename(name)
                base_close = fx_close if include_fx else pd.Series(1.0, index=fx_close.index, name=name)

            close_cols[name] = base_close
            if ohlc:
                high_cols[name] = base_close
                low_cols[name] = base_close

        else:
            # Instrument path
            ticker = h.get("ticker")
            px_df = f1.fetch_csv_robust(ticker, params=params)

            if ohlc:
                # Use unadjusted OHLC consistently
                local = f1.sort_cols(px_df, ohlc=True)
                local_close = local["Close"].astype(float).rename(name)
                local_high = local["High"].astype(float).rename(name)
                local_low = local["Low"].astype(float).rename(name)
                # for return: local close in (inc GBp)
                local_close_df[name] = px_df["Close"].astype(float).rename(name)             
                # Pence handling for LSE pence tickers (scale all)
                local_close = deal_with_gbx(local_close, ccy, gbx)
                local_high = deal_with_gbx(local_high, ccy, gbx)
                local_low = deal_with_gbx(local_low, ccy, gbx)

                if include_fx and ccy != "CHF":
                    fx = fx_map.get(f"{ccy}CHF", pd.DataFrame())
                    if fx is None or len(fx) == 0:
                        raise KeyError(f"Missing FX series {ccy}CHF for {name}")
                    fx_close = fx["Close"] if isinstance(fx, pd.DataFrame) and "Close" in fx.columns else fx
                    fx_close = fx_close.astype(float)
                    fx_close_aln = fx_close.reindex(local_close.index).ffill()

                    close_cols[name] = (local_close * fx_close_aln).rename(name)
                    high_cols[name] = (local_high * fx_close_aln).rename(name)
                    low_cols[name] = (local_low * fx_close_aln).rename(name)
                else:
                    close_cols[name] = local_close
                    high_cols[name] = local_high
                    low_cols[name] = local_low

            else:
                # Close-only path: prefer Adjusted Close
                local_close = f1.pick_close_column(px_df).rename(name).astype(float)
                local_close = deal_with_gbx(local_close, ccy, gbx)
                if include_fx and ccy != "CHF":
                    fx = fx_map.get(f"{ccy}CHF", pd.DataFrame())
                    if fx is None or len(fx) == 0:
                        raise KeyError(f"Missing FX series {ccy}CHF for {name}")
                    fx_close = fx["Close"] if isinstance(fx, pd.DataFrame) and "Close" in fx.columns else fx
                    fx_close = fx_close.astype(float).reindex(local_close.index).ffill()
                    close_cols[name] = (local_close * fx_close).rename(name)
                else:
                    close_cols[name] = local_close

    # Assemble and align
    close_df = pd.concat(close_cols.values(), axis=1) if close_cols else pd.DataFrame()
    close_df.columns = list(close_cols.keys())

    if not ohlc:
        prices_df = close_df.dropna(how="any")
        prices_df = trim_series(prices_df, from_dt, to_dt)

        return prices_df

    high_df = pd.concat(high_cols.values(), axis=1) if high_cols else pd.DataFrame(index=close_df.index)
    high_df.columns = list(high_cols.keys())
    low_df = pd.concat(low_cols.values(), axis=1) if low_cols else pd.DataFrame(index=close_df.index)
    low_df.columns = list(low_cols.keys())

    # Inner-align on common dates, then trim
    common_idx = close_df.index
    high_df = high_df.reindex(common_idx).dropna(how="any")
    low_df = low_df.reindex(common_idx).dropna(how="any")
    close_df = close_df.reindex(common_idx).dropna(how="any")

    high_df = trim_series(high_df, from_dt, to_dt)
    low_df = trim_series(low_df, from_dt, to_dt)
    close_df = trim_series(close_df, from_dt, to_dt)

    return high_df, low_df, close_df, local_close_df