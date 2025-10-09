computershare = [
    {"name":"Unilever", "ticker":"ULVR.LON", "ccy":"GBP", "gbx":True,  "value_chf": 25000},
    {"name":"Shell",    "ticker":"SHEL.LON", "ccy":"GBP", "gbx":True,  "value_chf": 13000},
    {"name":"NatWest",  "ticker":"NWG.LON",  "ccy":"GBP", "gbx":True,  "value_chf":  5000},
    {"name":"Barclays", "ticker":"BARC.LON", "ccy":"GBP", "gbx":True,  "value_chf":  5000},
    {"name":"Tesco",    "ticker":"TSCO.LON", "ccy":"GBP", "gbx":True,  "value_chf":  5000},
    {"name":"SWDA",     "ticker":"SWDA.LON", "ccy":"GBP", "gbx":True,  "value_chf":  12000},
    {"name":"EMIM",     "ticker":"EMIM.LON", "ccy":"GBP", "gbx":True,  "value_chf":  8000},
    {"name":"IBM",      "ticker":"IBM",      "ccy":"USD", "gbx":False, "value_chf":  4000},
    {"name":"ERNS",     "ticker":"ERNS.LON", "ccy":"GBP", "gbx":True,  "value_chf":  5000},
]
IBKR_live = [
    {"name":"EMIM",     "ticker":"EMIM.LSE", "ccy":"GBP", "gbx":True, "position": 172},

    {"name":"HEAL",     "ticker":"HEAL.LSE", "ccy":"GBP", "USD_exposure": 0.65, "GBP_exposure": 0.03,"gbx":False, "position": 180},
    {"name":"IBM",     "ticker":"IBM.US", "ccy":"USD", "USD_exposure": 1.00,"gbx":False, "position": 9},

    {"name":"SGLN",      "ticker":"SGLN.LSE",      "ccy":"GBP", "USD_exposure": 1.0, "gbx":True, "position": 42},

    {"name":"VUAG",     "ticker":"VUAG.LSE", "ccy":"GBP","USD_exposure": 1.0 , "gbx":False, "position": 28},
    {"name":"WSML",     "ticker":"WSML.LSE", "ccy":"USD","USD_exposure": 0.58, "GBP_exposure": 0.04,"gbx":False, "position": 483},
    {"name": "SIKA", "ticker": "SIKA.SW", "ccy": "CHF", "gbx":False, "position": -9},
    {"name": "YCA", "ticker": "YCA.LSE", "ccy": "GBP", "USD_exposure": 1.0, "gbx":True, "position": 258},
    {"name":"VEU",     "ticker":"VEU.US", "ccy":"USD", "GBP_exposure": 0.13,"gbx":False, "position": 68,},


    {"name": "CASH_CHF", "type": "cash", "ccy": "CHF", "amount": 9698},
    {"name": "CASH_GBP", "type": "cash", "ccy": "GBP", "amount": 57, "risk_fx": True },
    {"name": "CASH_USD", "type": "cash", "ccy": "USD", "amount": 0},
]
IBKR_live_adj = [
    {"name":"EMIM",     "ticker":"EMIM.LSE", "ccy":"GBP", "gbx":True, "position": 172},
    {"name":"HEAL",     "ticker":"HEAL.LSE", "ccy":"GBP", "USD_exposure": 0.65, "GBP_exposure": 0.03,"gbx":False, "position": 180},
    {"name":"IBM",     "ticker":"IBM.US", "ccy":"USD", "USD_exposure": 1.00,"gbx":False, "position": 9},

    {"name":"SGLN",      "ticker":"SGLN.LSE",      "ccy":"GBP", "USD_exposure": 1.0, "gbx":True, "position": 42},

    {"name":"VUAG",     "ticker":"VUAG.LSE", "ccy":"GBP","USD_exposure": 1.0 , "gbx":False, "position": 28},
    {"name":"WSML",     "ticker":"WSML.LSE", "ccy":"USD","USD_exposure": 0.58, "GBP_exposure": 0.04,"gbx":False, "position": 483},
    {"name": "SIKA", "ticker": "SIKA.SW", "ccy": "CHF", "gbx":False, "position": -9},
    {"name": "YCA", "ticker": "YCA.LSE", "ccy": "GBP", "USD_exposure": 1.0, "gbx":True, "position": 258},
    {"name":"VEU",     "ticker":"VEU.US", "ccy":"USD", "GBP_exposure": 0.13,"gbx":False, "position": 68,},
    # {"name":"IWDC",     "ticker":"IWDC.SW", "ccy":"CHF", "USD_exposure": 0.72, "GBP_exposure": 0.035,"gbx":False, "position": 100, "risk_fx": False },

    {"name": "CASH_CHF", "type": "cash", "ccy": "CHF", "amount": 9698},
    {"name": "CASH_GBP", "type": "cash", "ccy": "GBP", "amount": 57, "risk_fx": True },
    {"name": "CASH_USD", "type": "cash", "ccy": "USD", "amount": 0},
]
IBKR_sim =[
    {"name":"EMIM",     "ticker":"EMIM.LSE", "ccy":"GBP", "gbx":True, "position": 172},
    {"name":"HEAL",     "ticker":"HEAL.LSE", "ccy":"GBP", "USD_exposure": 0.65, "GBP_exposure": 0.03,"gbx":False, "position": 180},
    {"name":"IBM",     "ticker":"IBM.US", "ccy":"USD", "USD_exposure": 1.00,"gbx":False, "position": 9},
    {"name":"SGLN",      "ticker":"SGLN.LSE",      "ccy":"GBP", "USD_exposure": 1.0, "gbx":True, "position": 42},
    {"name":"VUAG",     "ticker":"VUAG.LSE", "ccy":"GBP","USD_exposure": 1.0 , "gbx":False, "position": 28},
    {"name":"WSML",     "ticker":"WSML.LSE", "ccy":"USD","USD_exposure": 0.58, "GBP_exposure": 0.04,"gbx":False, "position": 483},
    {"name": "SIKA", "ticker": "SIKA.SW", "ccy": "CHF", "gbx":False, "position": -9},
    {"name": "YCA", "ticker": "YCA.LSE", "ccy": "GBP", "USD_exposure": 1.0, "gbx":True, "position": 258},
    {"name":"VEU",     "ticker":"VEU.US", "ccy":"USD", "GBP_exposure": 0.13,"gbx":False, "position": 68,},

    # {"name":"ICLN",     "ticker":"ICLN.US", "ccy":"USD", "USD_exposure": 0.32, "GBP_exposure": 0.05,"gbx":False, "position": 250, "risk_fx": True },
    # {"name":"REMX",     "ticker":"REMX.LSE", "ccy":"USD", "USD_exposure": 0.34, "GBP_exposure": 0.0,"gbx":False, "position": 53, "risk_fx": True },
    # {"name":"LIT",     "ticker":"LIT.LSE", "ccy":"USD", "USD_exposure": 0.16, "GBP_exposure": 0.0,"gbx":False, "position": 375, "risk_fx": True },
    # {"name":"IPLT",     "ticker":"IPLT.LSE", "ccy":"USD", "USD_exposure": 1, "GBP_exposure": 0.0,"gbx":False, "position": 160, "risk_fx": True },
    # {"name":"COPA",     "ticker":"COPA.LSE", "ccy":"USD", "USD_exposure": 1, "GBP_exposure": 0.0,"gbx":False, "position": 90, "risk_fx": True },
    {"name":"INFR",     "ticker":"INFR.LSE", "ccy":"GBP", "USD_exposure": 1, "GBP_exposure": 0.0,"gbx":False, "position": 90, "risk_fx": True },

    {"name": "CASH_CHF", "type": "cash", "ccy": "CHF", "amount": 9698},
    {"name": "CASH_GBP", "type": "cash", "ccy": "GBP", "amount": 0, "risk_fx": True },
    {"name": "CASH_USD", "type": "cash", "ccy": "USD", "amount": 0},
]



small_test = [
        {"name":"HEAL",     "ticker":"HEAL.LSE", "ccy":"GBP", "gbx":False, "position": 180},
        {"name":"EMIM",     "ticker":"EMIM.LSE", "ccy":"GBP", "gbx":True, "position": 172},
]