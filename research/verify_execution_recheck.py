"""Read-only audit of saved backtests; write a separate, immutable verification file.

No strategy reruns or ledger mutations. Reconstruct economic cash, position, and swap
from saved fills, independently of the broker and execution-price helper.
"""

import argparse
import hashlib
import json
from pathlib import Path

import numpy as np
import pandas as pd


def read_json(path):
    return json.loads(path.read_text(encoding="utf-8"))


def assert_close(actual, expected):
    np.testing.assert_allclose(actual, expected, rtol=0, atol=5e-7)


def reconcile_path(bars, swaps, equity, fills, result, cfg, model, multiplier):
    equity = equity.copy()
    equity["timestamp"] = pd.to_datetime(equity.timestamp, utc=True)
    fills = fills.copy()
    fills["timestamp"] = pd.to_datetime(fills.timestamp, utc=True)
    frame = bars.set_index("timestamp").loc[equity.timestamp]
    spread_floor = cfg["spread"] * multiplier
    slip = cfg["slippage"] * multiplier
    fee = cfg["commission_rate"] * multiplier

    def half_spread(frame, field):
        if model["mode"] == "fixed":
            return np.full(len(frame), spread_floor / 2)
        observed = (frame[f"ask_{field}"] - frame[f"bid_{field}"]).to_numpy()
        return (
            np.maximum(spread_floor, observed * model["observed_spread_multiplier"] * multiplier)
            / 2
        )

    quote = bars.set_index("timestamp").loc[fills.timestamp]
    expected_price = quote.open.to_numpy() + np.sign(fills.filled_units) * (
        half_spread(quote, "open") + slip
    )
    assert_close(fills.price, expected_price)
    assert_close(fills.commission, np.abs(fills.filled_units) * expected_price * fee)
    # Economic cash is unlevered cash flow; unlike broker margin cash it includes short proceeds.
    flow = (
        pd.Series(
            -(fills.filled_units.to_numpy() * fills.price.to_numpy() + fills.commission.to_numpy()),
            index=fills.timestamp,
        )
        .groupby(level=0)
        .sum()
        .reindex(equity.timestamp, fill_value=0)
        .cumsum()
    )
    positions = pd.Series(fills.filled_units.to_numpy(), index=fills.timestamp)
    positions = positions.groupby(level=0).sum().reindex(equity.timestamp, fill_value=0).cumsum()
    assert_close(positions, equity.units)

    # Rollover belongs to the position held immediately BEFORE a same-time fill.
    close_times = equity.timestamp + pd.Timedelta(seconds=cfg["bar_seconds"])
    events = swaps[
        (swaps.timestamp > equity.timestamp.iloc[0]) & (swaps.timestamp <= close_times.iloc[-1])
    ]
    indices = fills.timestamp.searchsorted(events.timestamp, side="left") - 1
    held = np.zeros(len(events))
    if len(fills):
        mask = indices >= 0
        held[mask] = fills.filled_units.cumsum().to_numpy()[indices[mask]]
    credits = (
        np.abs(held) / 10000 * np.where(held > 0, events.long_jpy_per_10k, events.short_jpy_per_10k)
    )
    at_row = close_times.searchsorted(events.timestamp, side="left")
    carry = np.bincount(at_row, weights=credits, minlength=len(equity)).cumsum()
    assert_close(carry, equity.swap_pnl)
    marks = frame.close.to_numpy()
    if model["mode"] == "bid_ask":
        marks = marks - np.sign(positions.to_numpy()) * half_spread(frame, "close")
    expected_equity = cfg["initial_cash"] + flow.to_numpy() + positions.to_numpy() * marks + carry
    assert_close(equity.equity, expected_equity)
    assert_close(result["final_equity_jpy"], expected_equity[-1])
    size = positions.iloc[-1]
    exit_price = frame.close.iloc[-1] - np.sign(size) * (half_spread(frame, "close")[-1] + slip)
    liquidated = (
        cfg["initial_cash"]
        + flow.iloc[-1]
        + size * exit_price
        - abs(size) * exit_price * fee
        + carry[-1]
    )
    assert_close(result["liquidation_equity_jpy"], liquidated)
    return {
        "bars": len(equity),
        "fills": len(fills),
        "max_equity_error_jpy": float(np.max(np.abs(equity.equity - expected_equity))),
    }


def review_gaps(bars, quality, evidence):
    official = set()
    for window in evidence["windows"]:
        hours = pd.date_range(
            window["first_missing_bar_jst"], window["resume_jst"], freq="h", inclusive="left"
        ).tz_convert("UTC")
        assert len(hours) == window["hours"]
        assert not set(hours) & set(bars.timestamp)
        official.update(hours)
        assert pd.Timestamp(window["resume_jst"]).tz_convert("UTC") in set(bars.timestamp)
    unexplained = set()
    for gap in quality["gaps"]:
        if not gap["unexplained_bar_intervals"]:
            continue
        for time in pd.date_range(gap["after"], gap["before"], freq="h", inclusive="neither"):
            local = time.tz_convert("Asia/Tokyo")
            weekend = (
                local.weekday() == 6
                or (local.weekday() == 5 and local.hour >= 6)
                or (local.weekday() == 0 and local.hour < 7)
            )
            if not weekend:
                unexplained.add(time)
    assert len(unexplained) == quality["unexplained_bar_intervals"]
    assert unexplained == official
    return {
        "status": "reviewed_special_closures",
        "covered_hours": len(official),
        "covered_gaps": quality["unexplained_gap_count"],
        "remaining_unexplained_hours": 0,
        "note": "Companion audit only; original evaluation reports are preserved unchanged.",
        "sources": sorted({window["source"] for window in evidence["windows"]}),
    }


def verify(batch, prior, evidence_path):
    manifest = read_json(batch / "manifest.json")
    old = read_json(prior / "report.json")
    control = batch / "sma-24-120-fixed-control"
    new = read_json(control / "report.json")
    for key in ("data_sha256", "swap_sha256", "config_sha256", "active_start", "warmup_bars"):
        assert new[key] == old[key], key
    for filename in ("equity.csv", "orders.csv", "trades.csv"):
        previous = pd.read_csv(prior / filename, low_memory=False).drop(
            columns=["order_id"], errors="ignore"
        )
        current = pd.read_csv(control / filename, low_memory=False).drop(
            columns=["order_id"], errors="ignore"
        )
        pd.testing.assert_frame_equal(previous, current, check_exact=False, rtol=0, atol=5e-7)
    audited = {}
    for job in manifest["jobs"]:
        directory = Path(job["output"])
        report = read_json(directory / "report.json")
        assert report["code_sha256"] == manifest["code_sha256"]
        bars = pd.read_parquet(directory / "bars.parquet")
        swaps = pd.read_csv(directory / "swap.csv", parse_dates=["timestamp"])
        equity = pd.read_csv(directory / "equity.csv", low_memory=False)
        orders = pd.read_csv(directory / "orders.csv", low_memory=False)
        paths = {}
        for scenario, spec in report["scenarios"].items():
            for fold in [None, *range(1, report["fold_count"] + 1)]:
                scope = "continuous" if fold is None else "independent_fold"
                eq = equity[(equity.scenario == scenario) & (equity.evaluation_scope == scope)]
                od = orders[
                    (orders.scenario == scenario)
                    & (orders.evaluation_scope == scope)
                    & (orders.status == "Completed")
                ]
                result = spec["continuous"] if fold is None else spec["folds"][fold - 1]["result"]
                if fold is not None:
                    eq, od = eq[eq.fold == fold], od[od.fold == fold]
                paths[f"{scenario}-{fold}"] = reconcile_path(
                    bars,
                    swaps,
                    eq.reset_index(drop=True),
                    od.reset_index(drop=True),
                    result,
                    job["config"],
                    job["execution"],
                    spec["cost_multiplier"],
                )
        audited[job["name"]] = paths
    gaps = review_gaps(bars, report["data_quality"], read_json(evidence_path))
    return {
        "fixed_control_matches_prior": True,
        "paths": audited,
        "gap_review": gaps,
        "evidence_sha256": hashlib.sha256(evidence_path.read_bytes()).hexdigest(),
        "verifier_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--batch", type=Path, default=Path("runs/20260929-execution-recheck"))
    parser.add_argument(
        "--prior", type=Path, default=Path("runs/20260929-hypothesis-screen/sma-24-120-long")
    )
    parser.add_argument(
        "--evidence", type=Path, default=Path("research/20260929-holiday-evidence.json")
    )
    args = parser.parse_args()
    destination = args.batch / "verification.json"
    if destination.exists():
        raise FileExistsError(destination)
    result = verify(args.batch, args.prior, args.evidence)
    with destination.open("x", encoding="utf-8") as stream:
        json.dump(result, stream, ensure_ascii=False, indent=2)
    print(json.dumps(result, ensure_ascii=True, indent=2))
