import numpy as np
import pandas as pd
from typing import Callable, Iterable, Tuple
import matplotlib.pyplot as plt
import itertools

import functions2

def _enforce_min_run(gate: pd.Series, min_run: int) -> pd.Series:
    """
    Keep only True segments with length >= min_run.
    """
    g = gate.astype(bool).fillna(False)
    if not min_run or min_run <= 1:
        return g
    seg = g.ne(g.shift()).cumsum()
    sizes = g.groupby(seg).transform("size")
    return (g & (sizes >= min_run)).reindex(g.index, fill_value=False)


def wave_rider(
    entry_confirm: pd.Series,
    reconfirm: pd.Series,
    grace_days: int,
    pos_ret: pd.Series | None = None,
    consec_rises_kill: int | None = None,
    returns: pd.Series | None = None,
    avg_rise_window: int | None = None,
    avg_rise_threshold: float | None = None,
 ) -> pd.Series:

    # print('wave_rider')
    idx = entry_confirm.index
    out = pd.Series(False, index=idx)
    in_pos = False
    no_reconf = 0
    rises_run = 0
    rmean = None
    if returns is not None and avg_rise_window is not None and avg_rise_window > 0:
        rmean = returns.rolling(avg_rise_window, min_periods=avg_rise_window).mean()
    for i in range(len(idx)):
        # Detect rise today (ignore NaNs as False)
        is_rise = False
        if pos_ret is not None:
            try:
                val = pos_ret.iloc[i]
                is_rise = bool(val) if pd.notna(val) else False
            except Exception:
                is_rise = False
        if not in_pos:
            if bool(entry_confirm.iloc[i]):
                in_pos = True
                no_reconf = 0
                rises_run = 0
        else:
            # Optional avg-of-N-days drift kill
            if rmean is not None and avg_rise_threshold is not None:
                try:
                    mval = rmean.iloc[i]
                except Exception:
                    mval = np.nan
                if pd.notna(mval) and (mval >= avg_rise_threshold):
                    in_pos = False
                    no_reconf = 0
                    rises_run = 0
                    out.iloc[i] = in_pos
                    continue
            # Optional immediate kill on N consecutive rises
            if consec_rises_kill and consec_rises_kill > 0:
                rises_run = (rises_run + 1) if is_rise else 0
                if rises_run >= consec_rises_kill:
                    in_pos = False
                    no_reconf = 0
                    rises_run = 0
                    out.iloc[i] = in_pos
                    continue
            # Grace-days logic based on reconfirmation
            if bool(reconfirm.iloc[i]):
                no_reconf = 0
            else:
                no_reconf += 1
                if no_reconf > grace_days:
                    in_pos = False
                    no_reconf = 0
                    rises_run = 0
        out.iloc[i] = in_pos
    return out

def _carry_edges_2060(returns: pd.Series, carry_ann: float, buffer20: float) -> pd.Series:
    """Original 20/60 carry edge conjunction (secondary filter)."""
    R20 = returns.rolling(20, min_periods=20).sum()
    R60 = returns.rolling(60, min_periods=60).sum()
    idx = pd.Series(returns.index, index=returns.index)
    span20_days = (idx - idx.shift(20)).dt.days
    span60_days = (idx - idx.shift(60)).dt.days
    span20_carry = carry_ann * (span20_days / 365.0)
    span60_carry = carry_ann * (span60_days / 365.0)
    signal20on = R20 < -(span20_carry + buffer20)
    signal60on = R60 < -(span60_carry + 3.0 * buffer20)
    return (signal20on & signal60on).fillna(False)

def _rolling_ols_slope(log_price: pd.Series, window: int) -> pd.Series:
    """
    Rolling OLS slope of log_price vs time index (0..w-1).
    Positive => local up-slope (avoid entries), negative => down-slope (eligible).
    """
    if window <= 1:
        raise ValueError("window must be > 1")
    x = np.arange(window)
    denom=((x-x.mean())**2).sum()

    def _s(y: np.ndarray) -> float:
        if np.any(np.isnan(y)):
            return np.nan
        y_mean = y.mean()
        return np.dot(x - x.mean(), y - y_mean) / denom
    
    return (
        log_price
        .rolling(window, min_periods=window)
        .apply(_s, raw=True)
        .rename(f"slope_w{window}")
    )

def rolling_ols_fit(log_price: pd.Series, window: int=None) -> pd.DataFrame:
    """
    Returns both slope and intercept from rolling OLS fits.
    """
    if window is None:
        window = len(log_price)-1
    x = np.arange(window)
    x_centered = x - x.mean()
    denom = (x_centered ** 2).sum()

    def _ols_params(y):
        if np.any(np.isnan(y)):
            return np.nan, np.nan
        y_centered = y - y.mean()
        slope = np.dot(x_centered, y_centered) / denom
        intercept = y.mean() - slope * x.mean()
        return (slope, intercept)

    slopes = log_price.rolling(window, min_periods=window).apply(
        lambda y: _ols_params(y)[0], raw=False
    )
    intercepts = log_price.rolling(window, min_periods=window).apply(
        lambda y: _ols_params(y)[1], raw=False
    )

    return pd.DataFrame({"slope": slopes, "intercept": intercepts})



def fxshort_gate(
    gbpchf: pd.Series,
    slope_window: int = 15,
    consec: int = 2,
    slope_entry_threshold: float =-1e-4,
    slope_exit_threshold: float = 0.0,
    require_carry: bool = False,
    consec_rises_kill: int = 3,
    shift_for_signal: bool = False,
    carry_ann: float = 0.04,
    buffer20: float = 0.002,
    grace_days: int = 2,                      
    slope_source: str = "log_price", 
    min_run_days: int = 3,                        # NEW
    entry_mask: pd.Series | None = None,            # NEW: 
) -> pd.Series:
    """
    Minimal FX short gate:
      Entry base: slope < slope_entry_threshold (and carry if enabled)
      Confirmation: need 'consec' consecutive eligible days
      Exit: slope >= slope_exit_threshold (next day if shift_for_signal)
    Optional fast kill: N consecutive positive return days.

    slope_source:
      - "log_price": rolling OLS slope of log(price) (default)
      - "returns_mean": rolling mean of log returns
      - "returns_slope": rolling OLS slope of log returns
    """
    s = functions2.standardize_fx_daily_index(gbpchf).astype(float)
    log_p = np.log(s)
    returns = log_p.diff()

    if slope_source == "log_price":
        slope = _rolling_ols_slope(log_p, slope_window)
    elif slope_source == "returns_mean":
        slope = returns.rolling(slope_window, min_periods=slope_window).mean().rename(f"ret_mean_w{slope_window}")
    elif slope_source == "returns_slope":
        # Use OLS slope on returns themselves (captures drift change)
        slope = _rolling_ols_slope(returns, slope_window).rename(f"ret_slope_w{slope_window}")
    else:
        raise ValueError("slope_source must be one of: 'log_price', 'returns_mean', 'returns_slope'")

    carry_edges = (
        _carry_edges_2060(returns, carry_ann, buffer20)
        if require_carry else pd.Series(True, index=s.index)
    ).fillna(False)

    # DEFINE ENTRY EXIT PARAMS FOR WAVERIDER + ENTRY MASK
    entry_base = (slope < slope_entry_threshold) & carry_edges
    if entry_mask is None:
      entry_mask = pd.Series(True, index=s.index)
    entry_mask = entry_mask.reindex(s.index).fillna(False)
    entry_base = entry_base & entry_mask

    # ENTRY CONFIRMATION REQUIRES 'consec' DAYS
    entry_confirm = (
        entry_base.astype("int8")
        .rolling(consec, min_periods=consec)
        .sum()
        .ge(consec)
        .astype(bool)
        .reindex(s.index)
        .fillna(False)
    )
    reconfirm = ((slope < slope_exit_threshold) & carry_edges).reindex(s.index).fillna(False)
    pos_ret = (returns > 0).reindex(s.index).fillna(False)

    # GET THE GATE STATE SERIES
    gate = wave_rider(
        entry_confirm=entry_confirm,
        reconfirm=reconfirm,
        grace_days=grace_days,                 # was 0; now parameterized
        pos_ret=pos_ret,
        consec_rises_kill=consec_rises_kill,
        returns=returns,
        avg_rise_window=None,
        avg_rise_threshold=None,
    )
    # Suppress short spikes
    gate = _enforce_min_run(gate, min_run_days)

    if shift_for_signal:
        gate = gate.shift(1, fill_value=False)

    return gate.rename("fxshort_gate")


def plot_gate_state(ticker: str, s: pd.Series, gate_stateon: pd.Series) -> None:
    plt.style.use('dark_background')

    # Use Mon–Fri only for plotting to avoid weekend prints often present in FX feeds
    # s=s.tail(200)
    s_std_plot = functions2.standardize_fx_daily_index(s)

    # Select tail for plotting
    # s_plot = s_std_plot.tail(TAIL_BARS) –if TAIL_BARS else s_std_plot
    s_plot = s_std_plot
    fig, ax = plt.subplots(figsize=(11, 6))
    # Base price plot
    s_plot.plot(ax=ax, color='steelblue', lw=1.2, label=ticker)
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




def _coerce_float(d: dict) -> dict:
    out = {}
    for k,v in d.items():
        if isinstance(v, (np.floating,)):
            out[k] = float(v)
        else:
            out[k] = v
    return out

def sweep_fxshort_gate(
    ticker: str,
    price: pd.Series,
    gate_fn: Callable = fxshort_gate,
    slope_window_vals: Iterable[int] = (5, 6, 8, 10, 15, 20),
    consec_vals: Iterable[int] = (1, 2, 3),
    slope_entry_thr_vals: Iterable[float] = (0.0, -1e-4, -5e-4),
    slope_exit_thr_offsets: Iterable[float] = (0.0, 1e-4, 3e-4),
    require_carry_vals: Iterable[bool] = (False, True),
    consec_rises_kill_vals: Iterable[int] = (0, 1, 2),
    carry_ann_vals: Iterable[float] = (0.04,),
    buffer20_vals: Iterable[float] = (0.002,),
    max_combos: int | None = None,
    rank_key: str = "net_expectancy_per_trade",
    min_trades: int = 30,
    grace_days_vals: Iterable[int] = (0,),     # NEW: sweep grace_days if desired
    slope_source_vals: Iterable[str] = ("log_price",),  # NEW: sweep source
    debug: bool = False,                        # NEW: optional diagnostics
) -> Tuple[pd.DataFrame, pd.Timestamp, pd.Timestamp]:
    # print('+++sweep_fxshort_gate')
    s = functions2.standardize_fx_daily_index(price)


    combos = itertools.product(
        slope_window_vals,
        consec_vals,
        slope_entry_thr_vals,
        slope_exit_thr_offsets,
        require_carry_vals,
        consec_rises_kill_vals,
        carry_ann_vals,
        buffer20_vals,
        grace_days_vals,
        slope_source_vals,
    )

    records = []
    for idx, (slope_w, consec, ent_thr, exit_off, req_carry, rises_kill, carry_ann, buf20, gdays, ssource) in enumerate(combos):
        if max_combos and idx >= max_combos:
            break
        exit_thr = ent_thr + exit_off
        if exit_thr < ent_thr:
            continue
        try:
            gate = gate_fn(
                s,
                carry_ann=carry_ann,
                slope_window=slope_w,
                consec=consec,
                buffer20=buf20,
                slope_entry_threshold=ent_thr,
                slope_exit_threshold=exit_thr,
                require_carry=req_carry,
                shift_for_signal=True,
                consec_rises_kill=rises_kill,
                grace_days=gdays,
                slope_source=ssource,
            )
            trades, stats = analyze_gate_trades(s, gate, position="short")

            if stats.get("trades", 0) < min_trades:
                continue

            FEE_PER_TRADE = 0.00004
            trades["carry_cost"] = trades["holding_days"] * (carry_ann / 365)
            trades["fee_cost"] = FEE_PER_TRADE
            trades["net_pct_return"] = trades["pct_return"] - trades["carry_cost"] - trades["fee_cost"]
            stats["net_expectancy_per_trade"] = trades["net_pct_return"].mean()
        except Exception as e:
            if debug:
                print(f"[{idx}] combo failed: {e}")
            continue

        rec = {
            "ticker": ticker,

            "slope_window": slope_w,
            "consec": consec,
            "slope_entry_thr": ent_thr,
            "slope_exit_thr": exit_thr,
            "require_carry": req_carry,
            "consec_rises_kill": rises_kill,
            "carry_ann": carry_ann,
            "buffer20": buf20,
            "grace_days": gdays,
            "slope_source": ssource,
            "gate": gate,
        }
        rec.update(_coerce_float(stats))
        records.append(rec)

    if not records:
        print("No valid gate configurations found in sweep.")
        return pd.DataFrame()  # return triple consistently

    df = pd.DataFrame(records)
    rk = rank_key
    if rk not in df.columns:
        raise ValueError(f"rank_key {rk} not in results")

    df = df.sort_values(
        [rk, "total_pct_return", "win_rate"],
        ascending=[False, False, False],
        kind="mergesort"
    ).reset_index(drop=True)

    topgate = df.iloc[0]['gate']
    # Call local function directly (was fxshort_gates.plot_gate_state)
    plot_gate_state(ticker, s, topgate)

    df = df.head(1).drop(columns=['gate'])
    return df

def summarize_top(df: pd.DataFrame, top: int = 10) -> pd.DataFrame:
    cols = [
        "carry_ann","slope_window","consec",
        "slope_entry_thr","slope_exit_thr",
        "require_carry","consec_rises_kill","buffer20",
        "trades","win_rate","net_expectancy_per_trade","total_pct_return",
        "avg_pct_return","avg_win","avg_loss","median_holding_days",
        "max_draw_trade_pct","best_trade_pct"
    ]
    cols = [c for c in cols if c in df.columns]
    return df.head(top)[cols]