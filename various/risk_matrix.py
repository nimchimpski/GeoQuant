# %%

import os, io, time, json, hashlib, pathlib, sys
import requests
import pandas as pd
import numpy as np
import importlib
from dotenv import load_dotenv
import re
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import seaborn as sns

from urllib.parse import urlparse
from datetime import datetime, timedelta
import config
import scripts.series_utils as f2
import scripts.data_io as f1
import scripts.portfolio as portfolio
import books

# print(holdings.IBKR_live)

importlib.reload(books)
importlib.reload(f2)
importlib.reload(config)

def build_returns_weights(
    holdings: list[dict],
    params: dict = {},
    window_start: str = None,
    window_end: str = None,
    no_fx: bool = False,
    usd_shift: bool = False,
    ohlc: bool = False,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.Series]:
    """
    Build CHF daily returns matrix for the provided holdings.
        holdings: list of dicts with tickers:
      - name: row/column label in outputs
      - ticker: EODHD tickerbol (e.g., 'SWDA.LON', 'IBM')
      - ccy: base currency of the asset price series (GBP/USD/EUR/CHF)
      - gbx: bool; if True, divide close by 100.0 (LSE pence)
      - position: number of shares held (float)
      - include_fx_vol: bool; when False, strip FX volatility (i.e., hedged in risk). When True or missing, include FX volatility.

        Returns:
      rets_df: DataFrame of CHF log returns, T x N
      prices_df: DataFrame of CHF closes, T x N
      weights: Series aligned to columns in rets_df
    """
    # SORT WINDOW DATES
    if window_end is None:
        window_end = pd.to_datetime(datetime.now().strftime('%Y-%m-%d'))
    else:
        window_end = pd.to_datetime(window_end)
    if window_start is not None:
        window_start = pd.to_datetime(window_start)
        if window_start >= window_end:
            raise ValueError("window_start must be before window_end")

    
    fx_map = portfolio.make_fx_map(holdings, params, no_fx, usd_shift)


    # ------------BUILD CHF CLOSE SERIES PER ASSET-------------------
    assets_close_local_df = pd.DataFrame()
    assets_close_chf_df = pd.DataFrame()

    for h in holdings:
        name = h['name']
  
        # print(f'>>>>>>>>>>>>>> Processing {name} >>>>>>>>')
        ccy   = h["ccy"].upper()

        if h.get("type", "").lower() == "cash":
            asset_close_local_s = portfolio.cash_series(ccy, fx_map)
        else:
            ticker   = h.get("ticker")
            px_df = f1.fetch_csv_robust(ticker, params=params)
            print(px_df.tail(20))
            asset_close_local_s = f1.sort_cols(px_df, ohlc)

            # *********** DEBUG PLOTTING ***************
            sa = asset_close_local_s
            sx = f2.trim_series(sa, "2025-10-20",)
            print('px local earliest date', sx.index[0].date())
            print('px local latest', sx.iloc[-1])
            print(sx.tail)
            # CHECK FOR LARGE GAPS IN DATA
            date_diffs = sx.index.to_series().diff().dt.days.dropna()
            max_gap = date_diffs.max()
            print(f'Max data gap (days) for {name}: {max_gap}')
            r = sx.pct_change()
            print('r, r*100 ', r.std(), (r*100).std()  )

            # PLOT
            fig, ax = plt.subplots(figsize=(10,4))
            ax.plot(sx.index, sx, label=f'{name} local close')
            # Formatter: month-day only
            ax.xaxis.set_major_formatter(mdates.DateFormatter('%m-%d'))

            ax.xaxis.set_major_locator(mdates.AutoDateLocator())
            plt.setp(ax.get_xticklabels(), rotation=45, ha='right')

            plt.tight_layout()

            plt.title(f'{name} local close price series')
            plt.xlabel('Date')
            plt.ylabel('Price')
            plt.legend()
            plt.grid()
            plt.show()



        # print('asset close local s', asset_close_local_s.iloc[-1])    
        ccy = h.get('ccy','').upper()
        # DEAL WITH GBX
        gbx = bool(h.get('gbx', False))
        if gbx and ccy != 'GBP':
            raise ValueError(f"gbx=True only valid for GBP assets, got {name} in {ccy}")
        asset_close_local_s = asset_close_local_s / 100.0 if gbx else asset_close_local_s
        assets_close_local_df[name] = asset_close_local_s

        # DECIDE IF TO REMOVE FX VOL IN RETURNS
        include_fx_vol_bool = h.get('include_fx_vol', True)
        if include_fx_vol_bool == False:
            print(f"Removing FX vol for {name} ({ccy}), is this hedged?")
        if ccy == 'CHF' or no_fx or (not include_fx_vol_bool) or h.get('type','').lower()=='cash':
            # print(f'No FX conversion for {name} ({ccy})')
            asset_close_chf_s = asset_close_local_s.rename(name)
            print('chf equiv latest', asset_close_chf_s.iloc[-1])
        else:
            # OTHERWISE INCLUDE FX VOL
            # print(f'Converting {name} from {ccy} to CHF')
            fx = fx_map.get(f"{ccy}CHF", pd.Series(dtype=float))

            fx_aligned = fx.reindex(asset_close_local_s.index).ffill()
            # ensure last value available even if FX lags one or two days
            if fx_aligned.iloc[-1] != fx_aligned.dropna().iloc[-1]:
                fx_aligned.iloc[-1] = fx_aligned.dropna().iloc[-1]
            asset_close_chf_s = (asset_close_local_s * fx_aligned).rename(name)

            # print(f'CHF close last for {name}: {asset_close_chf_s.iloc[-1]}')
        assets_close_chf_df[name] = asset_close_chf_s

    # ---------HEDGED CASH---------
    hedged_cash = [
        h['name'] for h in holdings
        if h.get('type','').lower() == 'cash' and h.get('include_fx_vol')
    ]
    for n in hedged_cash:
        if n in assets_close_chf_df.columns:
            assets_close_chf_df[n] = 1.0


    # ALIGN ON COMMON DATES AND RESTRICT TO LOOKBACK WINDOW
    prices_df = assets_close_chf_df.dropna(how="any")   
    
    prices_df = f2.trim_series(prices_df, window_start, window_end)
    rets_df = np.log(prices_df / prices_df.shift(1)).dropna()
    if window_start or window_end:
        window = window_end - window_start
        # convert window to int
        if prices_df.shape[0] < (window.days * 0.73):
            print(
                f"After alignment only {prices_df.shape[0]} rows remain "
                f"(expected {window}). Data source may not have full history."
            )
    
    if rets_df.isna().any().any():
        raise ValueError("NaNs remained in returns after shift/drop; check data alignment.")
    if (prices_df <= 0).any().any():
        raise ValueError("Non-positive prices encountered; check source data.")


    # GET CHF VALUE FOR EACH HOLDING (valuation always in CHF at as-of)
    chf_values = {}
    asof = prices_df.index[-1]
    for h in holdings:
        name = h['name']
        size = h.get('position', 0.0)

        chf_value = portfolio.get_holding_value_chf(h, fx_map, assets_close_local_df, assets_close_chf_df, asof)
  
        # print(f'CHF value {size} of {h["name"]}: {chf_value:.2f}')
        if chf_value is not None:
            chf_values[name] = chf_value
        
    total_val = sum(chf_values.values())
    print(f'LOOKBACK DAYS/REGIME: {window_start} to {window_end}  ({(window_end - window_start).days} days)')
    print(f"Total portfolio value (CHF): {total_val:.2f}")

    # CALCULATE WEIGHTS (CHF)
    weights = pd.Series()
    for h in holdings:
        name = h["name"]
        size = h.get('position', 0.0)
        value = float(chf_values[name])
        weight = value / total_val
        weights[name] = weight

        # JUST FOR THE PRINTING
        last = assets_close_local_df[name].iloc[-2]

        print(f"{name}: value CHF{value:.2f},  last {last:.2f} *fx* {size}")


    if not np.isclose(weights.sum(), 1.0, atol=1e-6):
        raise ValueError(f"Weights must sum to 1. Got {weights.sum():.6f}" "check postions input in holdings.")
    return rets_df, prices_df, weights



def portfolio_risk(rets_df: pd.DataFrame, weights: pd.Series) -> dict:
    """
    Compute annualized vols, correlation, covariance, portfolio vol,
    marginal risk contribution (MRC), and percent risk contribution (PRC).
    """
    print('++++++ portfolio_risk()')
    # Annualized stats
    cov_daily = rets_df.cov()
    cov_annual = cov_daily * 252.0
    vol_ann = rets_df.std() * np.sqrt(252.0)
    corr = rets_df.corr()

    # Align weights
    w = weights.reindex(rets_df.columns).astype(float)
    Sigma_w = cov_annual @ w
    port_var = float(w @ Sigma_w)
    port_vol = float(np.sqrt(port_var)) if port_var > 0 else 0.0

    # Contributions
    mrc = Sigma_w / port_vol if port_vol > 0 else Sigma_w * 0.0
    prc = (w * Sigma_w) / port_var if port_var > 0 else w * 0.0

    summary = pd.DataFrame({
        "Weight": w,
        "Vol_1Y_CHF": vol_ann,
        "MRC": mrc,           # ∂σ_p/∂w_i (absolute marginal contribution)
        "PRC_%": prc * 100.0  # percent contribution to total variance (sums ~100%)
    }).sort_values("PRC_%", ascending=False)

    return {
        "port_vol": port_vol,
        "cov_annual": cov_annual,
        "corr": corr,
        "vol_ann": vol_ann,
        "mrc": mrc,
        "prc": prc,
        "summary": summary,
    }


def eod_search(quey: str, token: str):
    import requests, pandas as pd
    url = f"https://eodhd.com/api/search/{quey}?api_token={token}&fmt=json"
    r = requests.get(url, timeout=30); r.raise_for_status()
    hits = r.json()
    # Return a small table to pick from
    return pd.DataFrame([{
        "code": h.get("Code"),
        "exchange": h.get("Exchange"),
        "name": h.get("Name"),
        "currency": h.get("Currency"),
        "type": h.get("Type"),
        "startdate": h.get("StartDate"),
        # earliet date

    } for h in hits])

