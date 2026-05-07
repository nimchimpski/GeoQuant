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
