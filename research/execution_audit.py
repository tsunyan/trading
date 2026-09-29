"""Conservative spread sensitivity on an existing, unchanged sequence of paper fills.

This is NOT a rerun of sizing, signals, halts, or broker execution. Never treat this
diagnostic as an executable strategy result. It exposes fixed-spread optimism.
"""

import argparse
import json
from pathlib import Path

import numpy as np
import pandas as pd


def extra_fill_cost(
    units: float, observed_spread: float, fixed_spread: float, commission: float
) -> float:
    """Charge only spread widening; account for the changed buy/sell fee base."""
    widening = max(0.0, observed_spread - fixed_spread) / 2
    direction = np.sign(units)
    return abs(units) * widening * (1 + direction * commission)


def audit(run: Path) -> dict:
    report = json.loads((run / "report.json").read_text(encoding="utf-8"))
    cfg = json.loads((run / "config.json").read_text(encoding="utf-8"))
    bars = pd.read_parquet(run / "bars.parquet").set_index("timestamp")
    orders = pd.read_csv(run / "orders.csv")
    orders["timestamp"] = pd.to_datetime(orders.timestamp, utc=True)
    output = {
        "method": "unchanged_fill_path; max(fixed spread, observed side-open spread); "
        "stress multiplier also scales observed spreads; same final-close rule",
        "limitations": [
            "Not a backtest: sizing, risk halts, and subsequent orders are not recomputed.",
            "BID/ASK candle opens need not be simultaneous executable quotes.",
            "No latency, liquidity, partial fills, or order rejection model.",
        ],
        "scenarios": {},
    }
    for scenario, result in report["scenarios"].items():
        multiplier = result["cost_multiplier"]
        fee = cfg["commission_rate"] * multiplier
        fixed = cfg["spread"] * multiplier
        fills = orders[
            (orders.scenario == scenario)
            & (orders.evaluation_scope == "continuous")
            & (orders.status == "Completed")
        ]
        costs = []
        spreads = []
        for fill in fills.itertuples():
            bar = bars.loc[fill.timestamp]
            spread = float(bar.ask_open - bar.bid_open)
            expected = float(bar.open) + np.sign(fill.filled_units) * (
                fixed / 2 + cfg["slippage"] * multiplier
            )
            if not np.isclose(fill.price, expected, atol=1e-7, rtol=0):
                raise ValueError("fill does not match assumed next-open model")
            costs.append(extra_fill_cost(fill.filled_units, spread * multiplier, fixed, fee))
            spreads.append(spread)
        continuous = result["continuous"]
        open_units = continuous["open_units"]
        last = bars.iloc[-1]
        final_cost = extra_fill_cost(
            -open_units,
            float(last.ask_close - last.bid_close) * multiplier,
            fixed,
            fee,
        )
        extra = sum(costs) + final_cost
        adjusted = continuous["liquidation_equity_jpy"] - extra
        output["scenarios"][scenario] = {
            "fills": len(fills),
            "additional_cost_jpy": extra,
            "fills_with_wider_spread": sum(s > cfg["spread"] + 1e-9 for s in spreads),
            "fill_spread_median_jpy": float(np.median(spreads)) if spreads else None,
            "fill_spread_p90_jpy": float(np.quantile(spreads, 0.9)) if spreads else None,
            "path_adjusted_liquidation_return_pct": (adjusted / cfg["initial_cash"] - 1) * 100,
        }
    return output


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--batch", type=Path, required=True)
    args = parser.parse_args()
    manifest = json.loads((args.batch / "manifest.json").read_text(encoding="utf-8"))
    results = {}
    for job in manifest["jobs"]:
        results[job["name"]] = audit(Path(job["output"]))
    output = args.batch / "execution-audit.json"
    with output.open("x", encoding="utf-8") as handle:
        json.dump(results, handle, indent=2, ensure_ascii=False)
    print(output)
