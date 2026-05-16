import pandas as pd


def compute_atr(df: pd.DataFrame, window: int = 14) -> pd.Series:
    """
    Wilder ATR using EWM (alpha = 1/window).
    Requires df to have High, Low, Close columns.
    Returns a Series indexed like df.
    """
    prev_close = df['Close'].shift(1)
    tr = pd.concat([
        df['High'] - df['Low'],
        (df['High'] - prev_close).abs(),
        (df['Low']  - prev_close).abs(),
    ], axis=1).max(axis=1)
    return tr.ewm(alpha=1 / window, adjust=False).mean()


def atr_ratchet_stop(
    close: pd.Series,
    atr: pd.Series,
    mult: float,
    initial_stop: float = None,
) -> pd.Series:
    """
    Ratcheting ATR stop — never decreases.

    At each bar: stop = max(previous_stop, close - mult * ATR)

    Parameters
    ----------
    close, atr : aligned Series (same index)
    mult       : ATR multiplier (e.g. 2.5)
    initial_stop : starting stop level. If None, derived from the first bar:
                   close.iloc[0] - mult * atr.iloc[0]

    Returns a Series of stop levels with the same index as close.
    """
    sp = (
        initial_stop
        if initial_stop is not None
        else float(close.iloc[0]) - mult * float(atr.iloc[0])
    )
    stops = []
    for c, a in zip(close, atr):
        sp = max(sp, float(c) - mult * float(a))
        stops.append(sp)
    return pd.Series(stops, index=close.index)
