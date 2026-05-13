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
import logging


from urllib.parse import urlparse
from datetime import datetime, timedelta
import geoquant.configs.config as config
import geoquant.series_utils as f2
import geoquant.data_io as f1
import geoquant.portfolio as portfolio
import geoquant.books as books
import geoquant.plotting as gplot


# logger.info(holdings.IBKR_live)

importlib.reload(books)
importlib.reload(f2)
importlib.reload(config)

logger = logging.getLogger(__name__)
# logging.getLogger().setLevel(logging.DEBUG)  # or INFO, ERROR, etc.

# --- Book total return calculation ---
def book_total_returns(rets_df, weights):
    """Return the compounded book return plus each position's percentage contribution.

    The per-position return is the compounded return of that position's weighted log
    return stream over the full period. The result is a tuple of:

    - total book return as a float
    - per-position returns as a pandas Series of percentage strings aligned to
      ``rets_df.columns``
    """
    if rets_df.empty:
        raise ValueError("rets_df is empty; cannot compute book return.")

    w = weights.reindex(rets_df.columns).astype(float)
    weighted_log_returns = rets_df.mul(w, axis=1)
    position_returns = np.expm1(weighted_log_returns.sum(axis=0)).reindex(rets_df.columns)
    position_returns = position_returns.map(lambda value: f"{value:.2%}")
    total_return = float(np.expm1(weighted_log_returns.sum(axis=1).sum()))

    return total_return, position_returns


def book_total_return(rets_df, weights):
    """Backward-compatible wrapper for the scalar book return."""
    total_return, _ = book_total_returns(rets_df, weights)
    return total_return





def build_returns_weights(
    holdings: list[dict],
    data_params: dict = {},
    no_fx: bool = False,
    usd_shift: bool = False,
    ohlc: bool = False,
    use_target_weights: bool = False,
    include_cash: bool = False,
    plot_spikes: bool = False,

) -> tuple[pd.DataFrame, pd.DataFrame, pd.Series]:
    """
    Build CHF daily returns matrix for the provided holdings.
        holdings: list of dicts with tickers:
      - name: row/column label in outputs
      - ticker: dependent on datasource. EODHD 'SWDA.LON/US'. stoooq 'SWDA.LSE/US'. For cash, omit ticker and set type='cash' and ccy.
      - ccy: base currency of the asset price series (GBP/USD/EUR/CHF)
      - gbx: bool; if True, divide close by 100.0 (LSE pence)
      - position: number of shares held (float)
      - include_fx_vol: bool; when False, strip FX volatility (i.e., hedged in risk). When True or missing, include FX volatility.

        Returns:
            rets_df: DataFrame of CHF log returns, T x N (cash excluded by default)
            prices_df: DataFrame of CHF closes, T x N (cash excluded by default)
            weights: Series aligned to columns in rets_df
            when use_target_weights=True, weights come from holding['target_weight']
    """
    logger.debug('++++++ build_returns_weights()')

    risk_holdings = holdings if include_cash else [
        h for h in holdings if h.get('type', '').lower() != 'cash'
    ]
    if len(risk_holdings) == 0:
        raise ValueError("No non-cash holdings available for risk calculation.")

    fx_map = portfolio.make_fx_map(holdings, data_params, no_fx, usd_shift)

    # ------------BUILD CHF CLOSE SERIES PER ASSET-------------------
    assets_close_local_df = pd.DataFrame()
    assets_close_chf_df = pd.DataFrame()
    logger.debug('===========building price series df===============')
    for h in risk_holdings:
        name = h['name']
        logger.debug(f'--------holdings-loop-{name}--------')

  
        logger.debug(f'>>>>>>>>>>>>>> Processing {name} >>>>>>>>')
        ccy   = h["ccy"].upper()

        if h.get("type", "").lower() == "cash":
            asset_close_local_s = portfolio.cash_series(ccy, fx_map)
        else:
            ticker   = h.get("ticker")
            px_df = f1.fetch_csv(ticker, data_params=data_params)
            logger.debug(px_df.tail(1))
            asset_close_local_s = f1.sort_cols(px_df, ohlc)

            sa = asset_close_local_s
            sx = f2.trim_series(sa, data_params)

            logger.debug('px local earliest date', sx.index[0].date())
            logger.debug('px local latest', sx.iloc[-1])
            logger.debug(sx.tail)
            # CHECK FOR LARGE GAPS IN DATA
            date_diffs = sx.index.to_series().diff().dt.days.dropna()
            max_gap = date_diffs.max()
            logger.debug(f'Max data gap (days) for {name}: {max_gap}')
            r = sx.pct_change()
            logger.debug('r, r*100 ', r.std(), (r*100).std()  )

            # PLOT
            # PLOT
            if plot_spikes or logger.isEnabledFor(logging.DEBUG):
                gplot.plot_spike_inspection(
                    sx,
                    name=name,
                    max_logret=0.07,
                    top_n=10,
                    show=True,
                )





        logger.debug('asset close local s', asset_close_local_s.iloc[-1])    
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
            logger.debug(f"Removing FX vol for {name} ({ccy}), is this hedged?")
        if ccy == 'CHF' or no_fx or (not include_fx_vol_bool) or h.get('type','').lower()=='cash':
            # logger.debug(f'No FX conversion for {name} ({ccy})')
            asset_close_chf_s = asset_close_local_s.rename(name)
            logger.debug('>chf equiv latest', asset_close_chf_s.iloc[-1])
        else:
            # OTHERWISE INCLUDE FX VOL
            logger.debug(f'Converting {name} from {ccy} to CHF')
            fx = fx_map.get(f"{ccy}CHF", pd.Series(dtype=float))

            fx_aligned = fx.reindex(asset_close_local_s.index).ffill()
            # ensure last value available even if FX lags one or two days
            if fx_aligned.iloc[-1] != fx_aligned.dropna().iloc[-1]:
                fx_aligned.iloc[-1] = fx_aligned.dropna().iloc[-1]
            asset_close_chf_s = (asset_close_local_s * fx_aligned).rename(name)

            logger.debug(f'CHF close last for {name}: {asset_close_chf_s.iloc[-1]}')
        assets_close_chf_df[name] = asset_close_chf_s
    logger.debug('===========df built===============\n')
    # ---------HEDGED CASH---------
    if include_cash:
        hedged_cash = [
            h['name'] for h in holdings
            if h.get('type','').lower() == 'cash' and h.get('include_fx_vol')
        ]
        for n in hedged_cash:
            if n in assets_close_chf_df.columns:
                assets_close_chf_df[n] = 1.0


    # ALIGN ON COMMON DATES AND RESTRICT TO LOOKBACK WINDOW
    prices_df = assets_close_chf_df.dropna(how="any")   
    
    prices_df = f2.trim_series(prices_df, data_params)
    rets_df = np.log(prices_df / prices_df.shift(1)).dropna()
    window = data_params['end'] -  data_params['start']
    # convert window to int
    if prices_df.shape[0] < (window.days * 0.73):
        logger.info(
            f"After alignment only {prices_df.shape[0]} rows remain "
            f"(expected {window.days}). Data source may not have full history."
        )
    
    if rets_df.isna().any().any():
        raise ValueError("NaNs remained in returns after shift/drop; check data alignment.")
    if (prices_df <= 0).any().any():
        raise ValueError("Non-positive prices encountered; check source data.")


    # GET CHF VALUE FOR EACH HOLDING (valuation always in CHF at as-of)
    chf_values = {}
    local_values = {}

    asof = prices_df.index[-1]
    for h in risk_holdings:
        name = h['name']
        size = h.get('position', 0.0)

        local_value = portfolio.get_holding_value_local(h, assets_close_local_df, asof)
        local_values[name] = local_value
        chf_value = portfolio.get_holding_value_chf(h, fx_map, assets_close_local_df, assets_close_chf_df, asof)
  
        logger.debug(f'CHF value {size} of {h["name"]}: {chf_value:.2f}')
        if chf_value is not None:
            chf_values[name] = chf_value
    
        
    total_val = sum(chf_values.values())
    
    logger.info(f'LOOKBACK DAYS/REGIME: {pd.to_datetime(data_params["start"]).date()} to {pd.to_datetime(data_params["end"]).date()}  ({(pd.to_datetime(data_params["end"]) - pd.to_datetime(data_params["start"])).days} days)')
    logger.debug(f"Total portfolio value (CHF): {total_val:.2f}")


    # CALCULATE WEIGHTS (book target weights or value-based)
    if use_target_weights:
        logger.info("Using target weights from book for risk calculation.")
        target_weights = books.extract_target_weights(risk_holdings)
        # Use target weights from the book (should sum to 1.0)
        weights = pd.Series(dtype=float)
        for h in risk_holdings:
            name = h["name"]
            if name in target_weights:
                weights[name] = target_weights[name]
            else:
                weights[name] = 0.0
        if not include_cash:
            wsum = float(weights.sum())
            if wsum <= 0:
                raise ValueError("Non-cash target weights must sum to a positive value.")
            weights = weights / wsum
        if not np.isclose(weights.sum(), 1.0, atol=1e-6):
            raise ValueError(f"Book target weights must sum to 1. Got {weights.sum():.6f}")
        # calculate local value, so we can get target position size
        
        rebalance_rows = []
        for h in risk_holdings:
            name = h['name']
            weight = float(weights.get(name, 0.0))
            current_chf = float(chf_values.get(name, 0.0))
            desired_chf = weight * total_val
            delta_chf = desired_chf - current_chf
            current_position = float(h.get('position', 0.0))
            # last local price (GBX-adjusted already via assets_close_local_df)
            last_local = assets_close_local_df[name].dropna().iloc[-1]
            last_local_adj = last_local  # assets_close_local_df already GBX-adjusted
            # FX rate local→CHF
            ccy = h.get('ccy', '').upper()
            if ccy == 'CHF' or ccy == '':
                fx_rate = 1.0
            else:
                fx_s = fx_map.get(f"{ccy}CHF", pd.Series(dtype=float))
                fx_rate = float(fx_s.dropna().iloc[-1]) if not fx_s.empty else 1.0
            price_in_chf = last_local_adj * fx_rate
            desired_position = desired_chf / price_in_chf if price_in_chf > 0 else float('nan')
            delta_shares = desired_position - current_position
            logger.info(
                f"{name}: target weight {weight:.2%}, current CHF{current_chf:.0f},\n"
                f"desired CHF{desired_chf:.0f}, delta CHF{delta_chf:+.0f},\n"
                f"current pos {current_position:.0f}, desired pos {desired_position:.1f},\n"
                f"delta shares {delta_shares:+.1f}"
            )
            rebalance_rows.append({
                'name': name,
                'target_weight': weight,
                'current_chf': current_chf,
                'desired_chf': desired_chf,
                'delta_chf': delta_chf,
                'current_position': current_position,
                'desired_position': desired_position,
                'delta_shares': delta_shares,
                'ccy': ccy,
                'last_local_price': last_local_adj,
            })
        rebalance_df = pd.DataFrame(rebalance_rows).set_index('name')
    else:
        weights = pd.Series()
        for h in risk_holdings:
            name = h["name"]
            size = h.get('position', 0.0)
            value = float(chf_values[name])
            weight = value / total_val
            weights[name] = weight
            last = assets_close_local_df[name].iloc[-2]
            logger.debug(f"{name}: value CHF{value:.2f},  last {last:.2f} *fx* {size}")
        rebalance_df = None
        if not np.isclose(weights.sum(), 1.0, atol=1e-6):
            raise ValueError(f"Weights must sum to 1. Got {weights.sum():.6f}" "check postions input in holdings.")
    return rets_df, prices_df, weights, rebalance_df



def portfolio_risk(rets_df: pd.DataFrame, weights: pd.Series) -> dict:
    """
    Compute annualized vols, correlation, covariance, portfolio vol,
    marginal risk contribution (MRC), and percent risk contribution (PRC).
    """
    logger.info('++++++ portfolio_risk()')
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

