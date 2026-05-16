from pathlib import Path
import yaml


POLICY_PATH = Path(__file__).parent / "configs" / "decision_policy.yaml"


def load_decision_policy(path: Path | None = None) -> dict:
    """Load the portfolio decision policy and fail fast if required sections are missing."""
    policy_file = path or POLICY_PATH
    with open(policy_file, "r") as f:
        policy = yaml.safe_load(f)

    required_top_keys = ["mandate", "cadence", "risk_budget", "rules", "backtest"]
    missing = [k for k in required_top_keys if k not in policy]
    assert not missing, f"decision_policy missing sections: {missing}"

    return policy


def tactical_trim_signal(ret_3d: float, ret_5d: float, rules: dict) -> dict:
    """Return a simple tactical trim decision based on short-horizon spike rules."""
    trim_rules = rules["trim"]

    spike_5d = trim_rules["tactical_spike_5d"]
    if spike_5d["enabled"] and ret_5d >= float(spike_5d["threshold_return_5d"]):
        return {
            "action": "trim",
            "fraction": float(spike_5d["trim_fraction"]),
            "reason": "tactical_spike_5d",
        }

    spike_3d = trim_rules["tactical_spike_3d"]
    if spike_3d["enabled"] and ret_3d >= float(spike_3d["threshold_return_3d"]):
        return {
            "action": "trim",
            "fraction": float(spike_3d["trim_fraction"]),
            "reason": "tactical_spike_3d",
        }

    return {"action": "hold", "fraction": 0.0, "reason": "no_trim_signal"}



def policy_summary(policy: dict) -> str:
    """Human-readable one-line summary used in notebooks and reports."""
    cadence = policy["cadence"]
    risk = policy["risk_budget"]
    return (
        f"Style={policy['mandate']['style']}, "
        f"tactical_scan={cadence['tactical_scan']}, "
        f"strategic_review={cadence['strategic_review']}, "
        f"max_dd={risk['portfolio_max_drawdown']:.0%}, "
        f"tactical_budget={risk['tactical_weight_budget']:.0%}"
    )
