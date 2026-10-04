"""Paper fills versus the research model's fill on the same decision bar. Read-only.

Paper fills at the public quote seen after the signal bar closes; research fills at the
next bar's BID/ASK open with the configured spread floor and slippage. The difference
is the execution assumption research cannot see. Positive cost means the paper fill was
worse for the account than the research fill.
"""

import argparse
import json
import sqlite3
from contextlib import closing
from datetime import datetime
from pathlib import Path

import pandas as pd

from trading.config import Settings
from trading.data import merge_bars, read_bars
from trading.execution import ExecutionModel, execution_prices

PIP = 0.01  # USD/JPY


def paper_fills(database: Path) -> list[dict]:
    """Every paper event with a fill, opened read-only so the account is never touched."""
    with closing(sqlite3.connect(f"{database.resolve().as_uri()}?mode=ro", uri=True)) as conn:
        rows = conn.execute("SELECT payload_json FROM events ORDER BY id").fetchall()
    fills = []
    for (payload,) in rows:
        event = json.loads(payload)
        if event.get("filled_units"):
            fills.append(
                {
                    "signal_time": pd.Timestamp(event["signal_time"]),
                    "action": event["action"],
                    "units": int(event["filled_units"]),
                    "price": float(event["fill_price"]),
                    "quote_bid": float(event["quote"]["bid"]),
                    "quote_ask": float(event["quote"]["ask"]),
                    "quote_time": pd.Timestamp(event["quote"]["timestamp"]),
                }
            )
    return fills


def execution_gap(fills: list[dict], bars: pd.DataFrame, cfg: Settings) -> dict:
    prices = execution_prices(bars, cfg, ExecutionModel(mode="bid_ask"))
    by_time = {t: i for i, t in enumerate(bars.timestamp)}
    rows, missing = [], []
    for fill in fills:
        # signal_time is the close of the last completed bar: the research fill bar's open.
        index = by_time.get(fill["signal_time"])
        if index is None:
            missing.append(fill["signal_time"].isoformat())
            continue
        buy = fill["units"] > 0
        research = float(prices[f"{'buy' if buy else 'sell'}_open"].iloc[index])
        per_unit = fill["price"] - research if buy else research - fill["price"]
        bar = bars.iloc[index]
        rows.append(
            {
                "signal_time": fill["signal_time"].isoformat(),
                "side": "BUY" if buy else "SELL",
                "units": abs(fill["units"]),
                "paper_price": fill["price"],
                "research_price": round(research, 6),
                "cost_pips": round(per_unit / PIP, 3),
                "cost_jpy": round(per_unit * abs(fill["units"]), 2),
                "quote_delay_seconds": round(
                    (fill["quote_time"] - fill["signal_time"]).total_seconds(), 3
                ),
                "paper_spread_pips": round((fill["quote_ask"] - fill["quote_bid"]) / PIP, 3),
                "research_open_spread_pips": round(
                    (float(bar["ask_open"]) - float(bar["bid_open"])) / PIP, 3
                ),
            }
        )
    costs = [r["cost_pips"] for r in rows]
    return {
        "fills": len(fills),
        "compared": len(rows),
        "missing_research_bars": missing,
        "mean_cost_pips": round(sum(costs) / len(costs), 3) if costs else None,
        "worst_cost_pips": max(costs) if costs else None,
        "total_cost_jpy": round(sum(r["cost_jpy"] for r in rows), 2),
        "mean_quote_delay_seconds": (
            round(sum(r["quote_delay_seconds"] for r in rows) / len(rows), 3) if rows else None
        ),
        "research_model": "next-bar BID/ASK open, spread floor and slippage from config",
        "paper_model": "public quote after the signal bar closes, plus configured slippage",
        "sufficient_sample": len(rows) >= 30,
        "rows": rows,
    }


def observation_report(directory: Path, extra_bars: list[Path] = ()) -> dict:
    manifest = json.loads((directory / "manifest.json").read_text(encoding="utf-8"))
    cfg = Settings.model_validate(manifest["config"])
    frames = [read_bars(path, cfg) for path in sorted((directory / "candles").glob("*.parquet"))]
    frames += [read_bars(path, cfg) for path in extra_bars]
    if not frames:
        raise ValueError("no research bars available for the observation period")
    bars = merge_bars(frames, cfg)
    report = execution_gap(paper_fills(directory / "paper.sqlite"), bars, cfg)
    return {"directory": str(directory), "config_sha256": manifest["config_sha256"], **report}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--directory", type=Path, required=True)
    parser.add_argument("--bars", type=Path, action="append", default=[])
    parser.add_argument("--output", type=Path)
    args = parser.parse_args(argv)
    try:
        report = observation_report(args.directory, args.bars)
    except (OSError, ValueError, KeyError, sqlite3.Error) as error:
        parser.exit(2, f"execution_gap_failed: {type(error).__name__}: {error}\n")
    report["generated_at"] = datetime.now().astimezone().isoformat()
    text = json.dumps(report, ensure_ascii=False, indent=2)
    if args.output is not None:
        args.output.write_text(text + "\n", encoding="utf-8")
    print(text)


if __name__ == "__main__":
    main()
