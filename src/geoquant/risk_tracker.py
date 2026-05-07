"""
Geopolitical Risk Tracker
--------------------------
Bayesian update of P(catastrophic escalation) based on manually scored evidence.
Maps to portfolio exposure and triggers buffer-building recommendations.

Usage:
    python geo_risk_tracker.py

Author: built for personal portfolio risk management
"""

import sys
import json
import os
from datetime import datetime

# Allow imports from project root and scripts/
_HERE = os.path.dirname(os.path.abspath(__file__))
_ROOT = os.path.dirname(_HERE)
for _p in [_ROOT, os.path.join(_ROOT, 'scripts')]:
    if _p not in sys.path:
        sys.path.insert(0, _p)

from src.geoquant.books import IBKR_live, computershare, AJBell
from rename.scenario_configs import GEO_ESCALATION
from geoquant.data_io import compute_nav
from geoquant.configs.config import params as _BASE_PARAMS

# ─────────────────────────────────────────────
# FETCH LIVE NAV FROM BOOKS
# ─────────────────────────────────────────────

_ACTIVE_BOOKS = computershare + AJBell + IBKR_live

_fetch_params = dict(_BASE_PARAMS)
_fetch_params['max_age'] = 8  # hours — accept prices up to 8h old

print("Fetching live prices for NAV calculation...")
_nav = compute_nav(_ACTIVE_BOOKS, _fetch_params)

NAV_CHF         = _nav['nav_total']
NAV_INVESTED    = _nav['nav_invested']
CASH_BUFFER_CHF = _nav['cash_chf']

print(f"NAV total:    CHF {NAV_CHF:,.0f}")
print(f"NAV invested: CHF {NAV_INVESTED:,.0f}")
print(f"Cash buffer:  CHF {CASH_BUFFER_CHF:,.0f} ({CASH_BUFFER_CHF/NAV_CHF:.1%} of NAV)")

# ─────────────────────────────────────────────
# BUILD PORTFOLIO FROM BOOKS + SCENARIO CONFIG
# Weights derived from live values; only positions in scenario config are included.
# ─────────────────────────────────────────────

PORTFOLIO = {}
for _name, _scenario in GEO_ESCALATION.items():
    _val = _nav['positions'].get(_name, 0.0)
    _weight = _val / NAV_INVESTED if NAV_INVESTED > 0 else 0.0
    PORTFOLIO[_name] = {
        'weight': _weight,
        'escalation_sensitivity': _scenario['sensitivity'],
        'note': _scenario['note'],
    }

# ─────────────────────────────────────────────
# EVIDENCE SCORING GUIDE (remind yourself before scoring)
# ─────────────────────────────────────────────

SCORING_GUIDE = """
EVIDENCE SCORING GUIDE
-----------------------
Score each piece of evidence on how much it updates P(catastrophic escalation):

  +0.05 to +0.15  = mildly escalatory
    examples: rhetoric hardens, minor military movement, proxy attacks

  +0.15 to +0.30  = significantly escalatory  
    examples: major troop deployment, critical infrastructure threatened,
              rare earth signals, both sides reject ceasefire simultaneously

  +0.30 to +0.50  = severely escalatory
    examples: decapitation strike, Hormuz closure, power plant attacks,
              China/Russia signal direct involvement

  -0.05 to -0.20  = de-escalatory
    examples: ceasefire signals, back-channel talks confirmed,
              Trump winddown language, Hormuz reopening

IMPORTANT:
  - Score the EVIDENCE, not the price action
  - Price action is NOT valid evidence for this prior
  - Market calm is NOT evidence of low risk
  - Your prior should reflect YOUR research, not consensus
"""

# ─────────────────────────────────────────────
# PERSISTENCE - saves state between sessions
# ─────────────────────────────────────────────

STATE_FILE = os.path.join(os.path.dirname(__file__), 'geo_risk_state.json')

def load_state():
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE, 'r') as f:
            return json.load(f)
    return {
        'prior': 0.15,  # your initial P(catastrophic escalation) - set honestly
        'history': [],
        'created': datetime.now().isoformat()
    }

def save_state(state):
    with open(STATE_FILE, 'w') as f:
        json.dump(state, f, indent=2)

# ─────────────────────────────────────────────
# BAYESIAN UPDATE
# P(A|B) = P(B|A) * P(A) / P(B)
#
# A = catastrophic escalation
# B = observed evidence (your scored news event)
#
# P(A)   = current prior
# P(B|A) = how likely is this evidence IF catastrophe was coming
#           = prior + score (evidence consistent with catastrophe)
# P(B)   = marginal likelihood (normaliser)
#           = P(B|A)*P(A) + P(B|~A)*P(~A)
# ─────────────────────────────────────────────

def bayesian_update(prior, evidence_score):
    """
    prior          : current P(catastrophic escalation), 0-1
    evidence_score : how much this evidence shifts probability
                     positive = escalatory, negative = de-escalatory
                     range roughly -0.20 to +0.50

    Returns updated posterior.
    """
    # P(B|A): probability of seeing this evidence if catastrophe IS coming
    # High score = this evidence is very consistent with catastrophic path
    p_b_given_a = min(max(0.50 + evidence_score, 0.01), 0.99)

    # P(B|~A): probability of seeing this evidence if catastrophe is NOT coming
    # Inverse - escalatory evidence is less likely in a non-catastrophic world
    p_b_given_not_a = min(max(0.50 - evidence_score, 0.01), 0.99)

    # P(A) and P(~A)
    p_a = prior
    p_not_a = 1 - prior

    # Normaliser: P(B)
    p_b = (p_b_given_a * p_a) + (p_b_given_not_a * p_not_a)

    # Posterior: P(A|B)
    posterior = (p_b_given_a * p_a) / p_b

    return min(max(posterior, 0.01), 0.99)

# ─────────────────────────────────────────────
# PORTFOLIO RISK MAPPING
# ─────────────────────────────────────────────

def portfolio_risk(p_catastrophe):
    """
    Maps P(catastrophic escalation) to expected portfolio impact.
    Uses each position's escalation_sensitivity as exposure coefficient.
    """
    weighted_exposure = sum(
        pos['weight'] * pos['escalation_sensitivity']
        for pos in PORTFOLIO.values()
    )
    # Expected drawdown = P(catastrophe) * weighted exposure * assumed catastrophe drawdown
    assumed_catastrophe_drawdown = 0.45  # your earlier estimate of worst case
    expected_drawdown = p_catastrophe * weighted_exposure * assumed_catastrophe_drawdown
    return expected_drawdown, weighted_exposure

def most_exposed_positions(p_catastrophe):
    """Returns positions ranked by escalation sensitivity."""
    ranked = sorted(
        PORTFOLIO.items(),
        key=lambda x: x[1]['escalation_sensitivity'],
        reverse=True
    )
    return [(k, v) for k, v in ranked if v['escalation_sensitivity'] > 0]

# ─────────────────────────────────────────────
# BUFFER RECOMMENDATION
# ─────────────────────────────────────────────

def buffer_recommendation(p_catastrophe, cash_buffer, nav):
    """
    Generates buffer building recommendation based on posterior.
    Thresholds based on your own stated risk tolerance.
    """
    buffer_pct = cash_buffer / nav

    # Dynamic target: as P(catastrophe) rises, target buffer rises
    if p_catastrophe < 0.20:
        target_pct = 0.10
        urgency = "LOW"
    elif p_catastrophe < 0.35:
        target_pct = 0.15
        urgency = "MODERATE"
    elif p_catastrophe < 0.50:
        target_pct = 0.20
        urgency = "HIGH"
    elif p_catastrophe < 0.65:
        target_pct = 0.28
        urgency = "URGENT"
    else:
        target_pct = 0.35
        urgency = "CRITICAL"

    target_chf = nav * target_pct
    shortfall = max(target_chf - cash_buffer, 0)

    return {
        'urgency': urgency,
        'current_buffer_pct': buffer_pct,
        'target_buffer_pct': target_pct,
        'target_chf': target_chf,
        'shortfall_chf': shortfall,
    }

# ─────────────────────────────────────────────
# PRE-SUBMIT CHECKLIST
# Hard gates before any trade
# ─────────────────────────────────────────────

def pre_submit_checklist():
    print("\n" + "="*50)
    print("PRE-SUBMIT CHECKLIST")
    print("="*50)

    checks = [
        ("Checked WatcherGuru in last 5 mins?", True),
        ("Checked Reuters in last 5 mins?", True),
        ("More than 60 mins since market open?", True),
        ("More than 2 hours since last major news?", False),
    ]

    all_clear = True
    for question, blocking in checks:
        answer = input(f"  {question} (y/n): ").strip().lower()
        if answer != 'y' and blocking:
            print(f"  ⛔ STOP - do not submit until this is resolved")
            all_clear = False
        elif answer != 'y':
            print(f"  ⚠️  Warning - proceed with caution")

    return all_clear

# ─────────────────────────────────────────────
# DISPLAY
# ─────────────────────────────────────────────

def display_state(state):
    prior = state['prior']
    expected_drawdown, weighted_exposure = portfolio_risk(prior)
    rec = buffer_recommendation(prior, CASH_BUFFER_CHF, NAV_CHF)

    print("\n" + "="*50)
    print("GEOPOLITICAL RISK TRACKER")
    print(f"  {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print("="*50)

    print(f"\nP(catastrophic escalation) : {prior:.1%}")
    print(f"Expected portfolio drawdown: {expected_drawdown:.1%}")
    print(f"Current buffer             : CHF {CASH_BUFFER_CHF:,.0f} ({CASH_BUFFER_CHF/NAV_CHF:.1%} of NAV)")
    print(f"Buffer target              : CHF {rec['target_chf']:,.0f} ({rec['target_buffer_pct']:.1%} of NAV)")

    print(f"\n⚠️  BUFFER STATUS: {rec['urgency']}")
    if rec['shortfall_chf'] > 0:
        print(f"   Shortfall: CHF {rec['shortfall_chf']:,.0f}")
        print(f"   Consider trimming most exposed positions:")
        exposed = most_exposed_positions(prior)
        positive_exposure = sum(
            pos['escalation_sensitivity'] for _, pos in exposed
        )
        for name, pos in exposed[:3]:
            trim_value = rec['shortfall_chf'] * (pos['escalation_sensitivity'] / positive_exposure)
            print(f"   → {name}: sensitivity {pos['escalation_sensitivity']:.0%} | "
                  f"trim ~CHF {trim_value:,.0f} | {pos['note']}")
    else:
        print(f"   Buffer adequate for current risk level")

    if state['history']:
        print(f"\nRECENT EVIDENCE LOG")
        print("-"*50)
        for entry in state['history'][-5:]:
            direction = "▲" if entry['score'] > 0 else "▼"
            print(f"  {entry['date'][:16]} {direction} {entry['score']:+.2f} | "
                  f"{entry['prior_before']:.1%} → {entry['prior_after']:.1%}")
            print(f"    \"{entry['event']}\"")

def display_scoring_guide():
    print(SCORING_GUIDE)

# ─────────────────────────────────────────────
# MAIN MENU
# ─────────────────────────────────────────────

def main():
    state = load_state()

    while True:
        display_state(state)

        print("\nOPTIONS")
        print("  1. Add new evidence and update prior")
        print("  2. Pre-submit checklist")
        print("  3. Show scoring guide")
        print("  4. Reset prior (new situation)")
        print("  5. Exit")

        choice = input("\nChoice: ").strip()

        if choice == '1':
            print(SCORING_GUIDE)
            event = input("Describe the evidence: ").strip()
            if not event:
                continue
            try:
                score = float(input("Score (-0.20 to +0.50): ").strip())
            except ValueError:
                print("Invalid score")
                continue
            if not (-0.20 <= score <= 0.50):
                print(f"  ⚠️  Score {score:+.2f} is outside expected range (-0.20 to +0.50). Clamping.")
                score = max(-0.20, min(0.50, score))

            prior_before = state['prior']
            posterior = bayesian_update(prior_before, score)

            entry = {
                'date': datetime.now().isoformat(),
                'event': event,
                'score': score,
                'prior_before': prior_before,
                'prior_after': posterior,
            }
            state['history'].append(entry)
            state['prior'] = posterior
            save_state(state)

            print(f"\n  Updated: {prior_before:.1%} → {posterior:.1%}")

        elif choice == '2':
            cleared = pre_submit_checklist()
            if cleared:
                print("\n  ✅ All checks passed - proceed with order")
            else:
                print("\n  ⛔ Do not submit - resolve flags first")

        elif choice == '3':
            print(SCORING_GUIDE)
            input("Press enter to continue...")

        elif choice == '4':
            try:
                new_prior = float(input("New prior (0-1): ").strip())
                if not (0.0 <= new_prior <= 1.0):
                    print("Prior must be between 0 and 1")
                    continue
                reason = input("Reason for reset: ").strip()
                state['history'].append({
                    'date': datetime.now().isoformat(),
                    'event': f"MANUAL RESET: {reason}",
                    'score': 0,
                    'prior_before': state['prior'],
                    'prior_after': new_prior,
                })
                state['prior'] = new_prior
                save_state(state)
                print(f"  Prior reset to {new_prior:.1%}")
            except ValueError:
                print("Invalid value")

        elif choice == '5':
            print("\nState saved. Exiting.")
            break

if __name__ == '__main__':
    main()