computershare = [
    {"name":"Unilever", "ticker":"ULVR.LON", "ccy":"GBP", "gbx":True,  "position": 1134},
    {"name":"Magnum",       "ticker":"MICC.LON",   "ccy":"GBP", "gbx":True,  "position": 255},

]
AJBell = [
    {"name":"NatWest",     "ticker":"NWG.LSE", "ccy":"GBP", "gbx":True,  "position": 377}
]
IBKR_live = [
    # CORE

    {"name":"XMWX",     "ticker":"XMWX.LSE", "ccy":"GBP", "GBP_exposure": 0.13,"gbx":False, "include_fx_vol":True, "position": 369},
    {"name":"EMIM",     "ticker":"EMIM.LSE", "ccy":"GBP", "USD_exposure": 0,"gbx":True, "position": 321},

    {"name": "GWX", "ticker": "GWX.US", "ccy": "GBP", "USD_exposure": 0, "GBP_exposure": 0.07, "gbx":False, "position": 203},
    # DVIERSIFIERS
    {"name":"SGLN",      "ticker":"SGLN.LSE",      "ccy":"GBP", "USD_exposure": 1.0, "gbx":True, "include_fx_vol":True, "position": 68},
    # TACTICAL
    {"name": "YCA", "ticker": "YCA.LSE", "ccy": "GBP", "USD_exposure": 1.0, "gbx":True, "position": 689},
    {"name": "BATG", "ticker": "BATG.LSE", "ccy": "GBP","USD_exposure": 1, "GBP_exposure": 0, "gbx":True, "position": 202},
    {"name":"COPPER",     "ticker":"COPA.LSE", "ccy":"GBP", "USD_exposure": 0.70, "gbx":False, "position": 40},
    
    {"name": "CASH_CHF", "type": "cash", "ccy": "CHF", "amount": 14709, "include_fx_vol": True },

    {"name": "CASH_JPY","ticker": "JPYCHF.FOREX", "type": "cash", "ccy": "JPY", "amount": 11416, "include_fx_vol": True },
]
IBKR_live_adj = [
    # CORE
    # {"name":"VEU",     "ticker":"VEU.US", "ccy":"USD", "GBP_exposure": 0.13,"gbx":False, "include_fx_vol":True, "position": 111,},
    {"name":"XMWX",     "ticker":"XMWX.LSE", "ccy":"GBP", "GBP_exposure": 0.13,"gbx":False, "include_fx_vol":False, "position": 324},
    # {"name":"EMIM",     "ticker":"EMIM.LSE", "ccy":"GBP", "gbx":True, "position": 62},
    # {"name":"VUAG",     "ticker":"VUAG.LSE", "ccy":"GBP","USD_exposure": 1.0 , "gbx":False, "position": 95},

    # {"name": "ISJP", "ticker": "ISJP.SW", "ccy": "JPY", "USD_exposure": 0, "GBP_exposure": .0, "gbx":False, "position": 120},
    # {"name": "IEMS", "ticker": "IEMS.LSE", "ccy": "GBP", "USD_exposure": 0, "GBP_exposure": .0, "gbx":False, "position": 24},
    # {"name": "XXSC", "ticker": "XXSC.LSE", "ccy": "GBP", "USD_exposure": 0, "GBP_exposure": .30, "gbx":True, "position": 60},
    # DVIERSIFIERS
    # {"name":"SGLN",      "ticker":"SGLN.LSE",      "ccy":"GBP", "USD_exposure": 1.0, "gbx":True, "include_fx_vol":True, "position": 42},
    # {"name": "YCA", "ticker": "YCA.LSE", "ccy": "GBP", "USD_exposure": 1.0, "gbx":True, "position": 273},
    # {"name": "IPLT", "ticker": "IPLT.LSE", "ccy": "USD","USD_exposure": 1, "GBP_exposure": 0, "gbx":False, "position": 40},
    # {"name": "ICLN", "ticker": "ICLN.US", "ccy": "USD","USD_exposure": 1, "GBP_exposure": 0, "gbx":False, "position": 135},
    # {"name":"SILG",     "ticker":"SILG.LSE", "ccy":"USD", "USD_exposure": 0.27, "GBP_exposure": 0.07,"gbx":False, "position": 38},

    # TACTICAL
    # {"name": "REMX", "ticker": "REMX.LSE", "ccy": "USD","USD_exposure": .30, "GBP_exposure": 0, "gbx":False, "position": 40},
    # {"name": "INRG", "ticker": "INRG.LSE", "ccy": "GBP", "USD_exposure": .33, "GBP_exposure": .05, "gbx":True, "position": 290},
    # {"name": "NOVN", "ticker": "NOVN.SW", "ccy": "CHF", "USD_exposure": 0, "GBP_exposure": .0, "gbx":False, "position": 30},

    # {"name": "CASH_CHF", "ticker": "", "type": "cash", "ccy": "CHF", "amount": 5121}, # leave out 10k savings?
    # {"name": "CASH_GBP","ticker": "GBPCHF.FOREX", "type": "cash", "ccy": "GBP", "amount": -1276, "include_fx_vol": True },
    # {"name": "CASH_USD","ticker": "USDCHF.FOREX", "type": "cash", "ccy": "USD", "amount": 0},
    # {"name": "CASH_JPY","ticker": "JPYCHF.FOREX", "type": "cash", "ccy": "JPY", "amount": -236170},
]

IBKR_sim =[
    {"name":"EMIM",     "ticker":"EMIM.LSE", "ccy":"GBP", "gbx":True, "position": 172},
    {"name":"XMWX",     "ticker":"XMWX.LSE", "ccy":"GBP", "GBP_exposure": 0.13,"gbx":False, "include_fx_vol":True, "position": 215},

    # {"name":"HEAL",     "ticker":"HEAL.LSE", "ccy":"GBP", "USD_exposure": 0.65, "GBP_exposure": 0.03,"gbx":False, "position": 180},
    # {"name":"IBM",     "ticker":"IBM.US", "ccy":"USD", "USD_exposure": 1.00,"gbx":False, "position": 9},
    # {"name":"SGLN",      "ticker":"SGLN.LSE",      "ccy":"GBP", "USD_exposure": 1.0, "gbx":True, "position": 42},
    {"name":"VUAG",     "ticker":"VUAG.LSE", "ccy":"GBP","USD_exposure": 1.0 , "gbx":False, "position": 56},
    # {"name":"WSML",     "ticker":"WSML.LSE", "ccy":"USD","USD_exposure": 0.58, "GBP_exposure": 0.04,"gbx":False, "position": 483},
    # {"name": "SIKA", "ticker": "SIKA.SW", "ccy": "CHF", "gbx":False, "position": -9},
    {"name": "YCA", "ticker": "YCA.LSE", "ccy": "GBP", "USD_exposure": 1.0, "gbx":True, "position": 500},
    # {"name":"VEU",     "ticker":"VEU.US", "ccy":"USD", "GBP_exposure": 0.13,"gbx":False, "position": 68, "include_fx_vol": True },

    {"name":"ICLN",     "ticker":"ICLN.US", "ccy":"USD", "USD_exposure": 0.32, "GBP_exposure": 0.05,"gbx":False, "position": 250, "include_fx_vol": True },
    # {"name":"INRG",     "ticker":"INRG.LSE", "ccy":"GBP", "USD_exposure": 0.3, "GBP_exposure": 0.05,"gbx":True, "position": 400, "include_fx_vol": True },
    {"name":"REMX",     "ticker":"REMX.LSE", "ccy":"USD", "USD_exposure": 0.34, "GBP_exposure": 0.0,"gbx":False, "position": 260, "include_fx_vol": True },
    # {"name":"LITG",     "ticker":"LITG.LSE", "ccy":"GBP", "USD_exposure": 0.16, "GBP_exposure": 0.0,"gbx":True, "position": 425, "include_fx_vol": True },
    # {"name":"LITM",     "ticker":"LITM.LSE", "ccy":"USD", "USD_exposure": 0.25, "GBP_exposure": 0.0,"gbx":False, "position": 800, "include_fx_vol": True },
    {"name":"IPLT",     "ticker":"IPLT.LSE", "ccy":"USD", "USD_exposure": 1, "GBP_exposure": 0.0,"gbx":False, "position": 160, "include_fx_vol": True },
    # {"name":"COPA",     "ticker":"COPA.LSE", "ccy":"USD", "USD_exposure": 1, "GBP_exposure": 0.0,"gbx":False, "position": 90, "include_fx_vol": True },
    # {"name":"INFR",     "ticker":"INFR.LSE", "ccy":"GBP", "USD_exposure": 1, "GBP_exposure": 0.0,"gbx":True, "position": 120, "include_fx_vol": True },

    # {"name": "CASH_CHF", "type": "cash", "ccy": "CHF", "amount": 9698},
    # {"name": "CASH_GBP", "type": "cash", "ccy": "GBP", "amount": 0, "include_fx_vol": True },
    # {"name": "CASH_USD", "type": "cash", "ccy": "USD", "amount": 0},
]



small_test = [
        {"name":"HEAL",     "ticker":"HEAL.LSE", "ccy":"GBP", "gbx":False, "position": 180},
        {"name":"EMIM",     "ticker":"EMIM.LSE", "ccy":"GBP", "gbx":True, "position": 172},
]