import numpy as np
from scipy import stats
from typing import Tuple, Dict
import pandas as pd




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
    denom = window * (x**2).sum() - (x.sum() ** 2)
    def _s(y: np.ndarray) -> float:
        if np.any(np.isnan(y)):
            return np.nan
        return (window * (x * y).sum() - x.sum() * y.sum()) / denom
    return (
        log_price
        .rolling(window, min_periods=window)
        .apply(_s, raw=True)
        .rename(f"slope_w{window}")
    )

def fxshort_gate(
    gbpchf: pd.Series,
    carry_ann: float = 0.04,
    slope_window: int = 40,
    consec: int = 1,
    buffer20: float = 0.002,
    slope_entry_threshold: float = 0.0,
    slope_exit_threshold: float = 0.0,
    require_carry: bool = False,
    shift_for_signal: bool = True,
    consec_rises_kill: int = 1,
) -> pd.Series:
    """
    Minimal FX short gate:
      Entry base: slope < slope_entry_threshold (and carry if enabled)
      Confirmation: need 'consec' consecutive eligible days
      Exit: slope >= slope_exit_threshold (next day if shift_for_signal)
    Optional fast kill: N consecutive positive return days.
    """
    s = standardize_fx_daily_index(gbpchf)
    log_p = np.log(s)
    returns = log_p.diff()

    slope = _rolling_ols_slope(log_p, slope_window)
    carry_edges = (
        _carry_edges_2060(returns, carry_ann, buffer20)
        if require_carry else pd.Series(True, index=s.index)
    )

    entry_base = (slope < slope_entry_threshold) & carry_edges
    entry_confirm = (
        entry_base.astype("int8")
        .rolling(consec, min_periods=consec)
        .sum()
        .ge(consec)
        .astype(bool)
    )

    reconfirm = (slope < slope_exit_threshold) & carry_edges

    pos_ret = (returns > 0).fillna(False)

    gate = wave_rider(
        entry_confirm=entry_confirm,
        reconfirm=reconfirm,
        grace_days=0,
        pos_ret=pos_ret,
        consec_rises_kill=consec_rises_kill,
        returns=returns,
        avg_rise_window=None,
        avg_rise_threshold=None,
    )

    if shift_for_signal:
        gate = gate.shift(1, fill_value=False)

    return gate.rename("fxshort_gate")



def fxshort_gate_simple_asym(
    gbpchf: pd.Series,
    carry_ann: float,
    consec_on: int = 1,
    consec_off: int = 1,
    buffer20: float = 0.002,
    shift_for_signal: bool = False,
    grace_days: int = 0,               # optional extra cap without reconfirmation; 0 disables
    consec_rises_kill: int = 0,        # immediate kill if N consecutive rises occur (0 disables)
    avg_rise_window: int = 0,          # rolling mean window for drift kill (0 disables)
    avg_rise_threshold: float = 0.0,   # threshold for rolling mean; kill if mean >= threshold
) -> pd.Series:
    """Asymmetric variant: entry needs 20&60 edges; exit on 20d edge fail."""
    s = standardize_fx_daily_index(gbpchf)
    r = np.log(s).diff()
    R20 = r.rolling(20, min_periods=20).sum()
    R60 = r.rolling(60, min_periods=60).sum()
    idx = pd.Series(s.index, index=s.index)
    span20_days = (idx - idx.shift(20)).dt.days
    span60_days = (idx - idx.shift(60)).dt.days
    span20_carry = carry_ann * (span20_days / 365.0)
    span60_carry = carry_ann * (span60_days / 365.0)
    # Edges and entry/exit conditions
    signal20on = R20 < -(span20_carry + (3.0 * buffer20 if consec_off > 1 else buffer20))
    signal60on = R60 < -(span60_carry + 3.0 * buffer20)
    entry_ok = (signal20on & signal60on).fillna(False)
    exit_break = (~signal20on).fillna(False)
    entry_ready = (
        entry_ok.astype('int8').rolling(consec_on, min_periods=consec_on).sum().ge(consec_on)
    )
    exit_ready = (
        exit_break.astype('int8').rolling(consec_off, min_periods=consec_off).sum().ge(consec_off)
    )
    # Rolling mean for avg-of-N drift kill (optional)
    rmean = r.rolling(avg_rise_window, min_periods=avg_rise_window).mean() if avg_rise_window and avg_rise_window > 0 else None
    gate_state = pd.Series(False, index=s.index)
    in_pos = False
    no_reconf = 0
    rises_run = 0
    for i in range(len(s.index)):
        # Evaluate current rise and avg drift
        cur_rise = False
        try:
            rv = r.iloc[i]
            cur_rise = bool(rv > 0) if pd.notna(rv) else False
        except Exception:
            cur_rise = False
        if not in_pos and bool(entry_ready.iloc[i]):
            in_pos = True
            no_reconf = 0
            rises_run = 0
        elif in_pos:
            # Avg-of-N-days drift kill
            if rmean is not None:
                try:
                    mval = rmean.iloc[i]
                except Exception:
                    mval = np.nan
                if pd.notna(mval) and (mval >= avg_rise_threshold):
                    in_pos = False
                    no_reconf = 0
                    rises_run = 0
                    gate_state.iloc[i] = in_pos
                    continue
            # Immediate kill on N consecutive rises
            if consec_rises_kill and consec_rises_kill > 0:
                rises_run = (rises_run + 1) if cur_rise else 0
                if rises_run >= consec_rises_kill:
                    in_pos = False
                    no_reconf = 0
                    rises_run = 0
                    gate_state.iloc[i] = in_pos
                    continue
            # Normal exit if 20d edge fails (with optional consecutive-off requirement)
            if bool(exit_ready.iloc[i]):
                in_pos = False
                no_reconf = 0
                rises_run = 0
            elif grace_days > 0:
                # Optional grace: cap days without entry_ok reconfirmation
                if not bool(entry_ok.iloc[i]):
                    no_reconf += 1
                    if no_reconf > grace_days:
                        in_pos = False
                        no_reconf = 0
                        rises_run = 0
                else:
                    no_reconf = 0
        gate_state.iloc[i] = in_pos
    gate_state = gate_state.shift(1, fill_value=False) if shift_for_signal else gate_state
    return gate_state.rename("GBPCHF_short_gate_simple_asym")
