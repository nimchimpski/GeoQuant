import os, io, time, json, hashlib, pathlib, sys
import requests
import pandas as pd
import numpy as np
from dotenv import load_dotenv
import re
import matplotlib.pyplot as plt
from urllib.parse import urlparse
from datetime import datetime, timedelta



def analyze_gate_trades(price: pd.Series,
                        gate: pd.Series,
                        position: str = "short",
                        use_log_return: bool = False) -> tuple[pd.DataFrame, dict]:
    """
    Quantify gate performance.

    price: price series (aligned index, no future leakage).
    gate: boolean Series (True = in position for that day). If you used shift_for_signal=True
          then entry occurs at the CLOSE of the first True bar (or next day's open; here we use close).
    position: 'short' or 'long'
    use_log_return: if True, returns column is log exit/entry; else pct.

    Returns:
      trades_df: one row per continuous True segment.
      summary: aggregate metrics.
    """
    price = price.astype(float)
    gate = gate.astype(bool).reindex(price.index).fillna(False)

    # Transitions
    prev = gate.shift(1, fill_value=False)
    entries = gate & (~prev)
    exits   = (~gate) & prev

    entry_dates = list(price.index[entries])
    exit_dates  = list(price.index[exits])

    # If last trade still open, close at last available price
    if len(exit_dates) < len(entry_dates):
        exit_dates.append(price.index[-1])

    records = []
    for ent, ex in zip(entry_dates, exit_dates):
        if ex < ent:
            continue
        segment = price.loc[ent:ex]
        if segment.empty:
            continue
        entry_price = segment.iloc[0]
        exit_price  = segment.iloc[-1]

        # Path extremes
        high = segment.max()
        low  = segment.min()

        holding_days = (ex - ent).days  # calendar days
        bars = len(segment)

        if position == "short":
            # PnL positive if price falls
            pct_ret = (entry_price - exit_price) / entry_price
            # Max favorable excursion (price drop)
            mfe_pct = (entry_price - low) / entry_price
            # Max adverse excursion (price rise)
            mae_pct = (high - entry_price) / entry_price
        else:
            pct_ret = (exit_price - entry_price) / entry_price
            mfe_pct = (high - entry_price) / entry_price
            mae_pct = (entry_price - low) / entry_price

        log_ret = np.log(exit_price) - np.log(entry_price)

        records.append({
            "entry_date": ent,
            "exit_date": ex,
            "bars": bars,
            "holding_days": holding_days,
            "entry_price": entry_price,
            "exit_price": exit_price,
            "pct_return": pct_ret,
            "log_return": log_ret,
            "MFE_pct": mfe_pct,
            "MAE_pct": mae_pct,
        })

    trades_df = pd.DataFrame(records)
    if trades_df.empty:
        return trades_df, {"trades": 0}

    # Aggregates
    side_mult = -1 if position == "short" else 1  # already applied above; kept for clarity
    ret_col = "log_return" if use_log_return else "pct_return"
    wins = trades_df[ret_col] > 0
    losses = trades_df[ret_col] <= 0
    gross = trades_df[ret_col].sum()
    avg_win = trades_df.loc[wins, ret_col].mean() if wins.any() else 0.0
    avg_loss = trades_df.loc[losses, ret_col].mean() if losses.any() else 0.0
    expectancy = (wins.mean() * avg_win + (1 - wins.mean()) * avg_loss)

    summary = {
        "trades": len(trades_df),
        "win_rate": float(wins.mean()),
        f"total_{ret_col}": gross,
        f"avg_{ret_col}": trades_df[ret_col].mean(),
        "avg_win": avg_win,
        "avg_loss": avg_loss,
        "expectancy_per_trade": expectancy,
        "median_holding_days": float(trades_df["holding_days"].median()),
        "avg_MFE_pct": trades_df["MFE_pct"].mean(),
        "avg_MAE_pct": trades_df["MAE_pct"].mean(),
        "max_draw_trade_pct": trades_df["pct_return"].min(),
        "best_trade_pct": trades_df["pct_return"].max(),
    }

    return trades_df, summary

import itertools, math
import pandas as pd
import numpy as np
from typing import Iterable, Callable

# Import from the notebook context if run inside VS Code interactive;
# else adapt to your module path.
# from fxshort_asym_copi import (
#     standardize_fx_daily_index,
#     fxshort_gate,
#     fxshort_gate_simple,
#     analyze_gate_trades,
# )

def _coerce_float(d: dict) -> dict:
    out = {}
    for k,v in d.items():
        if isinstance(v, (np.floating,)):
            out[k] = float(v)
        else:
            out[k] = v
    return out

# def sweep_fxshort_gate(
#     price: pd.Series,
#     gate_fn: Callable = fxshort_gate,
#     carry_ann_vals: Iterable[float] = (0.0, 0.02, 0.04),
#     slope_window_vals: Iterable[int] = (5, 6, 8, 10, 15, 20),
#     consec_vals: Iterable[int] = (1, 2, 3),
#     slope_entry_thr_vals: Iterable[float] = (0.0, -1e-4, -5e-4),
#     slope_exit_thr_offsets: Iterable[float] = (0.0, 1e-4, 3e-4),
#     require_carry_vals: Iterable[bool] = (False, True),
#     consec_rises_kill_vals: Iterable[int] = (0, 1, 2),
#     buffer20_vals: Iterable[float] = (0.000, 0.001, 0.002),
#     max_combos: int | None = None,
#     rank_key: str = "expectancy_per_trade",
#     min_trades: int = 30,
# ) -> pd.DataFrame:
#     """
#     Parameter sweep for fxshort gates.
#     slope_exit_threshold = slope_entry_threshold + offset (hysteresis).
#     """
#     s = standardize_fx_daily_index(price)
#     combos = itertools.product(
#         carry_ann_vals,
#         slope_window_vals,
#         consec_vals,
#         slope_entry_thr_vals,
#         slope_exit_thr_offsets,
#         require_carry_vals,
#         consec_rises_kill_vals,
#         buffer20_vals,
#     )
#     records = []
#     for idx, (carry_ann, slope_w, consec, ent_thr, exit_off, req_carry, rises_kill, buf20) in enumerate(combos):
#         if max_combos and idx >= max_combos:
#             break
#         exit_thr = ent_thr + exit_off
#         if exit_thr < ent_thr:  # safety (should not happen with offsets >=0)
#             continue
#         try:
#             gate = gate_fn(
#                 s,
#                 carry_ann=carry_ann,
#                 slope_window=slope_w,
#                 consec=consec,
#                 buffer20=buf20,
#                 slope_entry_threshold=ent_thr,
#                 slope_exit_threshold=exit_thr,
#                 require_carry=req_carry,
#                 shift_for_signal=True,
#                 consec_rises_kill=rises_kill,
#             )
#             trades, stats = analyze_gate_trades(s, gate, position="short")
#         except Exception as e:
#             continue
#         if stats.get("trades", 0) < min_trades:
#             continue
#         rec = {
#             "carry_ann": carry_ann,
#             "slope_window": slope_w,
#             "consec": consec,
#             "slope_entry_thr": ent_thr,
#             "slope_exit_thr": exit_thr,
#             "require_carry": req_carry,
#             "consec_rises_kill": rises_kill,
#             "buffer20": buf20,
#         }
#         rec.update(_coerce_float(stats))
#         records.append(rec)
#     if not records:
#         return pd.DataFrame()
#     df = pd.DataFrame(records)
#     # Ranking: primary rank_key desc, then total_pct_return, then win_rate
#     rk = rank_key
#     if rk not in df.columns:
#         raise ValueError(f"rank_key {rk} not in results")
#     df = df.sort_values(
#         [rk, "total_pct_return", "win_rate"],
#         ascending=[False, False, False],
#         kind="mergesort"
#     ).reset_index(drop=True)
#     return df

# def summarize_top(df: pd.DataFrame, top: int = 10) -> pd.DataFrame:
#     cols = [
#         "carry_ann","slope_window","consec",
#         "slope_entry_thr","slope_exit_thr",
#         "require_carry","consec_rises_kill","buffer20",
#         "trades","win_rate","expectancy_per_trade","total_pct_return",
#         "avg_pct_return","avg_win","avg_loss","median_holding_days",
#         "max_draw_trade_pct","best_trade_pct"
#     ]
#     cols = [c for c in cols if c in df.columns]
#     return df.head(top)[cols]