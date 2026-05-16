# Copy this file to books.py and fill in your own holdings.
# books.py is NOT committed to the repository.

# Holdings at broker 1 (e.g. direct share plan)
computershare = [
    {"name": "Company A", "ticker": "AAA.LON", "ccy": "GBP", "gbx": True,  "position": 100},
    {"name": "Company B", "ticker": "BBB.LON", "ccy": "GBP", "gbx": True,  "position": 50},
]

# Holdings at broker 2
AJBell = [
    {"name": "Company C", "ticker": "CCC.LSE", "ccy": "GBP", "gbx": True,  "position": 200},
]

# Holdings at main broker (e.g. IBKR)
IBKR_live = [
    # Equity ETFs
    {"name": "ETF_A", "ticker": "ETFA.LSE", "ccy": "GBP", "GBP_exposure": 0.10, "gbx": False, "include_fx_vol": True, "position": 100},
    {"name": "ETF_B", "ticker": "ETFB.LSE", "ccy": "GBP", "USD_exposure": 0,    "gbx": True,  "position": 100},
    # Diversifiers
    {"name": "GOLD",  "ticker": "GOLD.LSE", "ccy": "GBP", "USD_exposure": 1.0,  "gbx": True,  "include_fx_vol": True, "position": 50},
    # Cash
    {"name": "CASH_CHF", "type": "cash", "ccy": "CHF", "amount": 10000, "include_fx_vol": True},
    {"name": "CASH_JPY", "ticker": "JPYCHF.FOREX", "type": "cash", "ccy": "JPY", "amount": 500000, "include_fx_vol": True},
]

# Optional: adjusted / simulation books
IBKR_live_adj = []
IBKR_sim = []

# ── BACKTEST TRADE LOG ────────────────────────────────────────────────────────
# One entry per completed trade. Mirrors the single-trade fields in
# tactical_backtest.ipynb so the book runner can iterate without modification.
#
# Required: ticker, entry_date, exit_date, entry_shares
# Optional: name, ccy, gbx, entry_price (None = first close), trim_fraction,
#           entry_spike_trim_pct  (defaults from policy if omitted)
backtest_trades = [
    {
        "name":                 "XMWX Oct24",
        "ticker":               "XMWX.LSE",
        "ccy":                  "GBP",
        "gbx":                  False,
        "entry_date":           "01/10/2024",   # DD/MM/YYYY
        "exit_date":            "30/04/2026",   # DD/MM/YYYY
        "entry_shares":         40,
        "entry_price":          None,           # None = use first close from data
        "trim_fraction":        0.5,
        "entry_spike_trim_pct": 0.08,
    },
    # Add more completed trades here, e.g.:
    # {
    #     "name": "BATG Nov24", "ticker": "BATG.LSE", "ccy": "GBP", "gbx": False,
    #     "entry_date": "01/11/2024", "exit_date": "30/04/2026",
    #     "entry_shares": 100, "entry_price": None,
    #     "trim_fraction": 0.5, "entry_spike_trim_pct": 0.08,
    # },
]
