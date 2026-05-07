from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
import hashlib
import json
import subprocess
from typing import Iterable

import pandas as pd
import numpy as np


@dataclass(frozen=True)
class SweepConfig:
    position: float = 1000.0
    trim_pct: float = 0.10
    take_profit_pct: float = 0.10
    min_months: int = 6
    roll_start_step_months: int | None = None
    frequency: str = "1M"


def ols_slope_logprice(series: pd.Series) -> float:
    """Compute OLS slope of log-price against centered time index."""
    y = np.log(pd.Series(series).astype(float).values)
    if len(y) < 3:
        return np.nan
    x = np.arange(len(y), dtype=float)
    x = x - x.mean()
    y = y - y.mean()
    denom = np.dot(x, x)
    if denom == 0:
        return np.nan
    return float(np.dot(x, y) / denom)


def minmax_normalize(series: pd.Series) -> pd.Series:
    """Min-max normalize a series; return all-ones when degenerate."""
    s = pd.Series(series)
    lo, hi = s.min(), s.max()
    if pd.isna(lo) or pd.isna(hi) or hi == lo:
        return pd.Series(np.ones(len(s)), index=s.index)
    return (s - lo) / (hi - lo)


def expanding_month_windows(index: pd.DatetimeIndex, min_months: int = 6) -> list[tuple[pd.Timestamp, pd.Timestamp, int]]:
    """Create anchored expanding windows from earliest date to max, stepped monthly."""
    idx = pd.DatetimeIndex(index).sort_values().unique()
    if len(idx) < 2:
        return []

    start = idx[0]
    data_end = idx[-1]
    max_months = (data_end.year - start.year) * 12 + (data_end.month - start.month)

    windows: list[tuple[pd.Timestamp, pd.Timestamp, int]] = []
    for months in range(min_months, max_months + 1):
        target_end = start + pd.DateOffset(months=months)
        pos = idx.searchsorted(target_end, side="left")
        end = idx[-1] if pos >= len(idx) else idx[pos]
        if end <= start:
            continue
        windows.append((start, end, months))
        if end == data_end:
            break
    return windows


def rolling_expanding_month_windows(
    index: pd.DatetimeIndex,
    min_months: int = 6,
    roll_start_step_months: int = 1,
) -> list[tuple[pd.Timestamp, pd.Timestamp, int, int]]:
    """Generate rolling monthly start anchors, each with expanding windows.

    Returns tuples: (window_start, window_end, window_months, roll_seq).
    """
    idx = pd.DatetimeIndex(index).sort_values().unique()
    if len(idx) < 2:
        return []
    if roll_start_step_months <= 0:
        raise ValueError("roll_start_step_months must be > 0")

    data_end = idx[-1]
    all_windows: list[tuple[pd.Timestamp, pd.Timestamp, int, int]] = []
    seen: set[tuple[pd.Timestamp, pd.Timestamp]] = set()

    roll_seq = 0
    anchor_start = idx[0]

    while True:
        min_target_end = anchor_start + pd.DateOffset(months=min_months)
        if min_target_end > data_end:
            break

        max_months = (data_end.year - anchor_start.year) * 12 + (data_end.month - anchor_start.month)
        for months in range(min_months, max_months + 1):
            target_end = anchor_start + pd.DateOffset(months=months)
            end_pos = idx.searchsorted(target_end, side="left")
            window_end = idx[-1] if end_pos >= len(idx) else idx[end_pos]
            if window_end <= anchor_start:
                continue

            key = (anchor_start, window_end)
            if key in seen:
                continue
            seen.add(key)
            all_windows.append((anchor_start, window_end, months, roll_seq))

            if window_end == data_end:
                break

        next_target_start = anchor_start + pd.DateOffset(months=roll_start_step_months)
        next_pos = idx.searchsorted(next_target_start, side="left")
        if next_pos >= len(idx):
            break

        next_anchor = idx[next_pos]
        if next_anchor <= anchor_start:
            break

        anchor_start = next_anchor
        roll_seq += 1

    return all_windows


def evaluate_take_profit_window(
    prices: pd.Series,
    position: float,
    trim_pct: float,
    take_profit_pct: float,
) -> dict:
    """Run the same trim-on-rebound logic currently used in various/backtest.ipynb."""
    s = pd.Series(prices).dropna().sort_index()
    if len(s) < 2:
        raise ValueError("Window has insufficient observations")

    entry_price = float(s.iloc[0])
    current_position = float(position)
    initial_position = float(position)
    low = entry_price

    total_cash = 0.0
    total_realized_pnl = 0.0
    trim_count = 0

    for _, price_val in s.items():
        price = float(price_val)
        low = min(price, low)
        if low <= 0:
            continue

        rebound = price / low - 1.0
        if rebound > take_profit_pct and current_position > 0:
            shares_to_trim = current_position * trim_pct
            pnl_this_trim = ((price - entry_price) * shares_to_trim) / 100.0
            total_realized_pnl += pnl_this_trim
            total_cash += (price * shares_to_trim) / 100.0
            current_position -= shares_to_trim
            low = price
            trim_count += 1

    final_price = float(s.iloc[-1])
    nav_after_trims = total_cash + (final_price * current_position) / 100.0
    nav_hold = (final_price * initial_position) / 100.0

    return {
        "entry_price": entry_price,
        "final_price": final_price,
        "trim_count": int(trim_count),
        "position_left": float(current_position),
        "nav_profit_take": float(nav_after_trims),
        "nav_hold": float(nav_hold),
        "pnl_profit_take_vs_hold": float(nav_after_trims - nav_hold),
        "return_profit_take": float(nav_after_trims / ((entry_price * initial_position) / 100.0) - 1.0),
        "return_hold": float(nav_hold / ((entry_price * initial_position) / 100.0) - 1.0),
    }


def get_git_version(default: str = "unknown") -> str:
    try:
        proc = subprocess.run(
            ["git", "rev-parse", "--short", "HEAD"],
            check=True,
            capture_output=True,
            text=True,
        )
        return proc.stdout.strip() or default
    except Exception:
        return default


def make_run_id(ticker: str, start: pd.Timestamp, end: pd.Timestamp, params: dict) -> str:
    payload = {
        "ticker": ticker,
        "window_start": str(start.date()),
        "window_end": str(end.date()),
        "params": params,
    }
    digest = hashlib.sha1(json.dumps(payload, sort_keys=True).encode("utf-8")).hexdigest()
    return digest[:12]


def sweep_sample_windows(
    prices: pd.Series,
    ticker: str,
    config: SweepConfig = SweepConfig(),
    strategy_name: str = "profit_take_trim",
) -> pd.DataFrame:
    """Sweep sample windows and compare profit-take vs hold.

    If config.roll_start_step_months is set (>0), uses rolling monthly starts and
    expanding windows from each start; otherwise uses the earliest-date anchored sweep.
    """
    s = pd.Series(prices).dropna().sort_index()
    if config.roll_start_step_months and config.roll_start_step_months > 0:
        rolling_windows = rolling_expanding_month_windows(
            s.index,
            min_months=config.min_months,
            roll_start_step_months=config.roll_start_step_months,
        )
        windows = [(ws, we, wm) for ws, we, wm, _ in rolling_windows]
    else:
        rolling_windows = None
        windows = expanding_month_windows(s.index, min_months=config.min_months)

    if not windows:
        return pd.DataFrame()

    params_dict = asdict(config)
    code_version = get_git_version(default="unknown")
    created_at = datetime.utcnow().isoformat(timespec="seconds") + "Z"

    records = []
    for i, (ws, we, window_months) in enumerate(windows):
        s_win = s.loc[(s.index >= ws) & (s.index <= we)]
        stats = evaluate_take_profit_window(
            s_win,
            position=config.position,
            trim_pct=config.trim_pct,
            take_profit_pct=config.take_profit_pct,
        )

        rec = {
            "run_id": make_run_id(ticker=ticker, start=ws, end=we, params=params_dict),
            "created_at": created_at,
            "code_version": code_version,
            "strategy": strategy_name,
            "ticker": ticker,
            "window_start": ws,
            "window_end": we,
            "window_months": int(window_months),
            "params_json": json.dumps(params_dict, sort_keys=True),
            "outperform_hold": bool(stats["nav_profit_take"] > stats["nav_hold"]),
        }

        if rolling_windows is not None:
            rec["roll_seq"] = int(rolling_windows[i][3])

        rec.update(stats)
        records.append(rec)

    return pd.DataFrame(records).sort_values("window_months").reset_index(drop=True)


def save_sweep_results(df: pd.DataFrame, out_path: str | Path) -> Path:
    path = Path(out_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.suffix.lower() == ".parquet":
        try:
            df.to_parquet(path, index=False)
            return path
        except Exception:
            # Keep sweeps runnable even when parquet backends are missing.
            csv_path = path.with_suffix(".csv")
            df.to_csv(csv_path, index=False)
            return csv_path
    elif path.suffix.lower() == ".csv":
        df.to_csv(path, index=False)
        return path
    else:
        raise ValueError("Use .parquet or .csv output path")


def summary_by_window_month(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame()

    out = (
        df.groupby("window_months", as_index=False)
        .agg(
            runs=("run_id", "count"),
            pct_outperform=("outperform_hold", "mean"),
            avg_diff_nav=("pnl_profit_take_vs_hold", "mean"),
            median_diff_nav=("pnl_profit_take_vs_hold", "median"),
            avg_trims=("trim_count", "mean"),
        )
        .sort_values("window_months")
    )
    out["pct_outperform"] = out["pct_outperform"] * 100.0
    return out


def sweep_takeprofit_trim_grid(
    prices: pd.Series,
    ticker: str,
    take_profit_vals: Iterable[float],
    trim_pct_vals: Iterable[float],
    min_months: int = 6,
    roll_start_step_months: int | None = None,
    position: float = 1000.0,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Run a 2D parameter sweep over take-profit and trim pct across sample windows.

    Returns:
      detail_df: one row per (param combo, sample window)
      ranked_df: one row per (param combo), ranked by pct_outperform then avg_diff_nav
    """
    detail_frames: list[pd.DataFrame] = []

    for take_profit_pct in take_profit_vals:
        for trim_pct in trim_pct_vals:
            cfg = SweepConfig(
                position=position,
                trim_pct=float(trim_pct),
                take_profit_pct=float(take_profit_pct),
                min_months=int(min_months),
                roll_start_step_months=roll_start_step_months,
            )
            combo_df = sweep_sample_windows(
                prices=prices,
                ticker=ticker,
                config=cfg,
                strategy_name="profit_take_trim",
            )
            if combo_df.empty:
                continue
            combo_df["take_profit_pct"] = float(take_profit_pct)
            combo_df["trim_pct"] = float(trim_pct)
            detail_frames.append(combo_df)

    if not detail_frames:
        return pd.DataFrame(), pd.DataFrame()

    detail_df = pd.concat(detail_frames, ignore_index=True)

    ranked_df = (
        detail_df.groupby(["take_profit_pct", "trim_pct"], as_index=False)
        .agg(
            windows=("run_id", "count"),
            pct_outperform=("outperform_hold", "mean"),
            avg_diff_nav=("pnl_profit_take_vs_hold", "mean"),
            median_diff_nav=("pnl_profit_take_vs_hold", "median"),
            avg_trims=("trim_count", "mean"),
            avg_return_profit_take=("return_profit_take", "mean"),
            avg_return_hold=("return_hold", "mean"),
        )
        .sort_values(["pct_outperform", "avg_diff_nav"], ascending=[False, False])
        .reset_index(drop=True)
    )

    ranked_df["pct_outperform"] = ranked_df["pct_outperform"] * 100.0
    return detail_df, ranked_df


def run_grid_sweep(
    prices: pd.Series,
    run_ticker: str,
    take_profit_vals: Iterable[float],
    trim_pct_vals: Iterable[float],
    min_months: int = 6,
    roll_start_step_months: int | None = None,
    position: float = 1000.0,
    save_outputs: bool = True,
) -> tuple[pd.DataFrame, pd.DataFrame, str | None, str | None]:
    """Run and optionally persist a 2D take-profit/trim parameter sweep."""
    detail_df, ranked_df = sweep_takeprofit_trim_grid(
        prices=prices,
        ticker=run_ticker,
        take_profit_vals=take_profit_vals,
        trim_pct_vals=trim_pct_vals,
        min_months=min_months,
        roll_start_step_months=roll_start_step_months,
        position=position,
    )

    detail_path = None
    ranked_path = None
    if save_outputs and not ranked_df.empty:
        detail_path = str(
            save_sweep_results(
                detail_df,
                f"../cache/sweeps/{run_ticker.replace('.', '_')}_param_grid_detail.parquet",
            )
        )
        ranked_path = str(
            save_sweep_results(
                ranked_df,
                f"../cache/sweeps/{run_ticker.replace('.', '_')}_param_grid_ranked.parquet",
            )
        )

    return detail_df, ranked_df, detail_path, ranked_path
