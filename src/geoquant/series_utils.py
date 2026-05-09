import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from typing import Tuple
import geoquant.configs.config as config
import geoquant.data_io as f1
from geoquant.configs.config import data_params




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

# function to trim series to specified dates
def trim_series(s: pd.Series, data_params: dict, end: str=None) -> pd.Series:
    start = data_params.get('start')
    end = data_params.get('end')
    if start:
        s = s[s.index >= pd.to_datetime(start)]
    if end:
        s = s[s.index <= pd.to_datetime(end)]
    return s

def get_window_dates(s: pd.Series) -> Tuple[pd.Timestamp, pd.Timestamp]:
    end_date = s.index[-1]
    start_date = s.index[1]
    return start_date, end_date

# def get_series1(ticker, params=config.params, window_start=None, window_end=None) -> pd.Series:
#     print(f'++++ get_series{ticker}')
#     s= f1.fetch_csv(params=params, ticker=ticker)
#     s = f1.sort_cols(s)
#     s = f2.standardize_fx_daily_index(s)
#     s = trim_series(s, window_start, window_end)
#     return s

def get_series(ticker, window_start=None, window_end=None) -> pd.Series:
    print(f'++++ get_series{ticker}')
    s = f1.fetch_csv(ticker=ticker, params=params)
    s = f1.sort_cols(s)
    s = standardize_fx_daily_index(s)
    s = trim_series(s, window_start, window_end)
    return s

def plotter(ticker, prices, gate_stateon=None, TAIL_BARS=1000,):
    plt.style.use('dark_background')

    # Select tail for plotting
    s_plot = prices.tail(TAIL_BARS) if TAIL_BARS else prices
    fig, ax = plt.subplots(figsize=(11, 6))
    # Base price plot
    s_plot.plot(ax=ax, color='steelblue', lw=1.2, label=ticker)

    if gate_stateon is not None:
 
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