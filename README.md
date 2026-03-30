# risk_matrix

A personal portfolio risk management suite for a multi-currency equity/ETF portfolio held across UK (LSE), US, and Swiss exchanges via IBKR, AJBell, and Computershare. All valuations are denominated in **CHF**.

## What it does

- **NAV calculation** — values each holding in CHF using live FX rates
- **Risk matrix** — computes a full covariance/correlation matrix with per-asset volatility, marginal risk contribution (MRC), and percent risk contribution (PRC)
- **FX hedge signals** — rolling OLS trend-following gate for a GBP/CHF short position, with configurable entry/exit thresholds and parameter sweep
- **Geopolitical risk overlay** — Bayesian probability engine that updates P(catastrophic escalation) from scored news events and recommends a cash buffer target
- **Price data caching** — TTL-based disk cache for Stooq OHLC CSVs with flat-bar spike cleaning

## Project structure

```
books.py                    # Holdings definitions (IBKR, AJBell, Computershare)
config.py                   # Project-wide settings, API keys via .env
scripts/
  data_io.py                # Price fetching, caching, NAV computation
  portfolio.py              # FX conversion to CHF, portfolio construction
  series_utils.py           # Time-series utilities, plotting
  synthetic_generators.py   # Synthetic price paths for stress testing
various/
  risk_matrix.py            # Core risk engine (vol, correlations, PRC)
  risk_tracker.py           # Geopolitical Bayesian risk overlay
  scenario_configs.py       # Per-asset escalation sensitivities
fx_hedges/
  fxshort_gates.py          # GBP/CHF short entry/exit signal logic
  fxshort_run.ipynb         # FX hedge run notebook
  hedge_sizing_run.ipynb    # Hedge sizing and sweep notebook
```

## Setup

1. **Clone the repo**
   ```bash
   git clone https://github.com/nimchimpski/risk_matrix.git
   cd risk_matrix
   ```

2. **Install dependencies**
   ```bash
   pip install pandas numpy matplotlib seaborn requests python-dotenv
   ```

3. **API key** (optional — only needed for EODHD datasource)  
   Create a `.env` file in the project root:
   ```
   EOD_API=your_eodhd_api_key
   ```
   The default datasource is **Stooq**, which requires no key.

4. **Create `books.py`** from the provided template:
   ```bash
   cp books.example.py books.py
   ```
   Then edit `books.py` with your own holdings. This file is excluded from version control.

## Usage

### Risk matrix

Open and run `various/risk_matrix_run.ipynb` (or equivalent notebook). It fetches prices, converts to CHF, and outputs:
- Annualized per-asset volatility
- Correlation heatmap
- Marginal and percent risk contributions

### NAV tracking

Run the `various/risk_tracker.ipynb` notebook. It calls `compute_nav()` to value all holdings in CHF and optionally runs the geopolitical risk overlay.

### FX hedge

Run `fx_hedges/fxshort_run.ipynb` to generate the GBP/CHF short gate signal. Use `hedge_sizing_run.ipynb` to sweep parameters and size the position.

## Data sources

- **[Stooq](https://stooq.com)** — default, no API key required
- **[EODHD](https://eodhd.com)** — optional alternative, requires API key

Fetched CSVs are cached under `cache/` with a configurable TTL (default 24 hours).

## Disclaimer

This is a personal tool for tracking a real portfolio. It is not financial advice and is not designed for general use. Holdings and parameters reflect one individual's situation.
