# ─────────────────────────────────────────────────────────────────────────────
# SCENARIO SENSITIVITY CONFIGS
# ─────────────────────────────────────────────────────────────────────────────
# Each scenario maps ticker name → sensitivity coefficient and note.
# Sensitivity: how strongly this position moves in response to the scenario.
#   +1.0 = fully correlated (position suffers worst)
#    0.0 = uncorrelated
#   -1.0 = fully inverse (position benefits)
#
# Names must match the 'name' field in books.py.
# ─────────────────────────────────────────────────────────────────────────────

GEO_ESCALATION = {
    'XMWX': {'sensitivity': 0.70, 'note': 'World ex-US, moderately exposed'},
    'EMIM': {'sensitivity': 0.85, 'note': 'EM, highly exposed'},
    'GWX':  {'sensitivity': 0.90, 'note': 'Small cap EM, most exposed'},
    'SGLN': {'sensitivity': -0.30, 'note': 'Gold, partial hedge - can move inversely'},
    'BATG': {'sensitivity': 0.65, 'note': 'Energy transition, medium exposed'},
    'YCA':  {'sensitivity': 0.65, 'note': 'Energy transition, medium exposed'},
    'COPPER': {'sensitivity': 0.60, 'note': 'Copper producers ETF, industrial/commodity cycle exposed'},
}

# Template for future scenarios:
# RECESSION = {
#     'XMWX':   {'sensitivity': 0.60, 'note': '...'},
#     'EMIM':   {'sensitivity': 0.75, 'note': '...'},
#     ...
# }
