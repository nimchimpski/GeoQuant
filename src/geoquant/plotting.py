import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates


def plot_spike_inspection(
    series: pd.Series,
    *,
    name: str = "asset",
    max_logret: float = 0.07,
    cleaned: pd.Series | None = None,
    top_n: int = 10,
    show: bool = True,
):
    s = series.dropna().copy()
    r = np.log(s / s.shift(1))
    spikes = r[r.abs() > max_logret].sort_values(key=lambda x: x.abs(), ascending=False).head(top_n)

    nrows = 3 if cleaned is not None else 2
    fig, ax = plt.subplots(nrows, 1, figsize=(12, 8 if nrows == 3 else 6), sharex=True)

    ax[0].plot(s.index, s.values, label=f"{name} local close", linewidth=1.2)
    if len(spikes):
        ax[0].scatter(spikes.index, s.reindex(spikes.index).values, s=20, color="red", label="spikes")
    ax[0].set_title(f"{name} price")
    ax[0].legend()
    ax[0].grid(True, alpha=0.3)

    ax[1].plot(r.index, r.values, label="log returns", linewidth=1.0)
    ax[1].axhline(max_logret, color="red", linestyle="--", linewidth=1)
    ax[1].axhline(-max_logret, color="red", linestyle="--", linewidth=1)
    if len(spikes):
        ax[1].scatter(spikes.index, spikes.values, s=20, color="red", label="spikes")
    ax[1].set_title(f"{name} log returns (threshold={max_logret:.3f})")
    ax[1].legend()
    ax[1].grid(True, alpha=0.3)

    if cleaned is not None:
        c = cleaned.reindex(s.index).interpolate(limit_direction="both")
        ax[2].plot(s.index, s.values, label="original", alpha=0.7)
        ax[2].plot(c.index, c.values, label="cleaned", alpha=0.9)
        ax[2].set_title(f"{name} original vs cleaned")
        ax[2].legend()
        ax[2].grid(True, alpha=0.3)

    ax[-1].xaxis.set_major_formatter(mdates.DateFormatter("%m-%d"))
    ax[-1].xaxis.set_major_locator(mdates.AutoDateLocator())
    plt.setp(ax[-1].get_xticklabels(), rotation=45, ha="right")
    plt.tight_layout()

    if show:
        plt.show()

    return fig, ax, spikes