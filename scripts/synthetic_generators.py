from __future__ import annotations

import numpy as np
import pandas as pd


def _business_index(n_days: int, start: str = "2018-01-01") -> pd.DatetimeIndex:
    return pd.bdate_range(start=start, periods=int(n_days))


def _to_price_series(returns: np.ndarray, price0: float, index: pd.DatetimeIndex, name: str) -> pd.Series:
    rets = np.asarray(returns, dtype=float)
    prices = float(price0) * np.exp(np.cumsum(rets))
    return pd.Series(prices, index=index, name=name)


def generate_student_t_path(
    n_days: int = 252 * 8,
    price0: float = 100.0,
    mu: float = 0.0,
    sigma: float = 0.012,
    df: int = 5,
    seed: int | None = 42,
    start: str = "2018-01-01",
) -> pd.Series:
    """Fat-tailed baseline path without explicit crash regimes."""
    rng = np.random.default_rng(seed)
    idx = _business_index(n_days, start=start)

    scale = sigma * np.sqrt((df - 2) / df) if df > 2 else sigma
    innovations = rng.standard_t(df=df, size=len(idx))
    rets = mu + scale * innovations
    return _to_price_series(rets, price0, idx, name="student_t")


def generate_jump_down_path(
    n_days: int = 252 * 8,
    price0: float = 100.0,
    mu: float = 0.0001,
    sigma: float = 0.010,
    jump_prob: float = 0.006,
    jump_min: float = 0.10,
    jump_max: float = 0.30,
    seed: int | None = 43,
    start: str = "2018-01-01",
) -> pd.Series:
    """Diffusion with rare large down jumps."""
    rng = np.random.default_rng(seed)
    idx = _business_index(n_days, start=start)

    base = rng.normal(loc=mu, scale=sigma, size=len(idx))
    jump_mask = rng.uniform(0.0, 1.0, size=len(idx)) < jump_prob
    jump_sizes = rng.uniform(jump_min, jump_max, size=len(idx))
    jump_shocks = np.where(jump_mask, np.log(np.clip(1.0 - jump_sizes, 1e-6, 1.0)), 0.0)
    rets = base + jump_shocks
    return _to_price_series(rets, price0, idx, name="jump_down")


def generate_crash_recovery_path(
    n_days: int = 252 * 8,
    price0: float = 100.0,
    mu_pre: float = 0.0002,
    mu_post: float = 0.00035,
    sigma_pre: float = 0.008,
    sigma_post: float = 0.013,
    crash_day: int | None = None,
    crash_size: float = 0.35,
    seed: int | None = 44,
    start: str = "2018-01-01",
) -> pd.Series:
    """Single large crash followed by higher-volatility recovery."""
    rng = np.random.default_rng(seed)
    idx = _business_index(n_days, start=start)

    n = len(idx)
    cday = n // 2 if crash_day is None else int(np.clip(crash_day, 10, n - 10))

    pre = rng.normal(loc=mu_pre, scale=sigma_pre, size=cday)
    post = rng.normal(loc=mu_post, scale=sigma_post, size=n - cday)
    rets = np.concatenate([pre, post])

    crash = float(np.clip(crash_size, 0.01, 0.90))
    rets[cday] += np.log(1.0 - crash)
    return _to_price_series(rets, price0, idx, name="crash_recovery")


def generate_flat_oscillation_path(
    n_days: int = 252 * 8,
    price0: float = 100.0,
    cycle_len: int = 24,
    amp: float = 0.020,
    noise_sigma: float = 0.004,
    seed: int | None = 45,
    start: str = "2018-01-01",
) -> pd.Series:
    """Near-flat trend with strong cyclical oscillations and low noise."""
    rng = np.random.default_rng(seed)
    idx = _business_index(n_days, start=start)

    t = np.arange(len(idx), dtype=float)
    cyc = amp * np.sin(2.0 * np.pi * t / max(2, int(cycle_len)))
    noise = rng.normal(0.0, noise_sigma, size=len(idx))
    rets = cyc + noise
    return _to_price_series(rets, price0, idx, name="flat_oscillation")


def build_synthetic_suite(
    n_days: int = 252 * 8,
    price0: float = 100.0,
    start: str = "2018-01-01",
) -> dict[str, pd.Series]:
    """Named synthetic scenarios for stress testing trim-vs-hold behavior."""
    return {
        "student_t": generate_student_t_path(n_days=n_days, price0=price0, start=start),
        "jump_down": generate_jump_down_path(n_days=n_days, price0=price0, start=start),
        "crash_recovery": generate_crash_recovery_path(n_days=n_days, price0=price0, start=start),
        "flat_oscillation": generate_flat_oscillation_path(n_days=n_days, price0=price0, start=start),
    }
