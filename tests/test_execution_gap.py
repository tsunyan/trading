"""Paper fills compared with the research next-bar BID/ASK fill, read-only."""

import json
import sqlite3

import pandas as pd
import pytest

from trading.execution_gap import execution_gap, main, paper_fills


def side_bars(bars, spread=0.04):
    frame = bars.copy()
    for field in ("open", "high", "low", "close"):
        frame[f"bid_{field}"] = frame[field] - spread / 2
        frame[f"ask_{field}"] = frame[field] + spread / 2
    return frame


def paper_db(path, events):
    with sqlite3.connect(path) as conn:
        conn.execute(
            "CREATE TABLE events (id INTEGER PRIMARY KEY AUTOINCREMENT, observed_at TEXT, "
            "payload_json TEXT)"
        )
        for event in events:
            conn.execute(
                "INSERT INTO events(observed_at,payload_json) VALUES (?,?)",
                (event.get("observed_at", ""), json.dumps(event)),
            )
    return path


def event(signal_time, units, price, *, delay=30, bid=151.0, ask=151.05, action="buy"):
    quote_time = pd.Timestamp(signal_time) + pd.Timedelta(seconds=delay)
    return {
        "action": action,
        "signal_time": signal_time,
        "observed_at": quote_time.isoformat(),
        "filled_units": units,
        "fill_price": price,
        "quote": {"bid": bid, "ask": ask, "timestamp": quote_time.isoformat()},
    }


def test_buy_and_sell_costs_are_signed_against_the_research_fill(cfg, bars, tmp_path):
    frame = side_bars(bars)
    # Bar 01:00 opens at mid 151.0; research buys at 151 + 0.02 + 0.01 = 151.03.
    # Bar 03:00 opens at mid 154.0; research sells at 154 - 0.02 - 0.01 = 153.97.
    database = paper_db(
        tmp_path / "paper.sqlite",
        [
            {"action": "hold", "signal_time": "2025-01-06T00:00:00+00:00", "filled_units": 0},
            event("2025-01-06T01:00:00+00:00", 500, 151.06),
            event("2025-01-06T03:00:00+00:00", -500, 153.99, action="sell"),
        ],
    )
    report = execution_gap(paper_fills(database), frame, cfg)
    buy, sell = report["rows"]
    assert buy["research_price"] == pytest.approx(151.03)
    assert buy["cost_pips"] == pytest.approx(3.0) and buy["cost_jpy"] == pytest.approx(15.0)
    assert sell["research_price"] == pytest.approx(153.97)
    # Selling 2 pips above the research price is better for the account: negative cost.
    assert sell["cost_pips"] == pytest.approx(-2.0) and sell["side"] == "SELL"
    assert report["compared"] == 2 and report["total_cost_jpy"] == pytest.approx(5.0)
    assert buy["quote_delay_seconds"] == 30 and buy["research_open_spread_pips"] == 4.0
    assert not report["sufficient_sample"]


def test_fill_without_a_research_bar_is_reported_not_guessed(cfg, bars, tmp_path):
    database = paper_db(
        tmp_path / "paper.sqlite", [event("2025-01-07T01:00:00+00:00", 500, 151.06)]
    )
    report = execution_gap(paper_fills(database), side_bars(bars), cfg)
    assert report["compared"] == 0 and report["mean_cost_pips"] is None
    assert report["missing_research_bars"] == ["2025-01-07T01:00:00+00:00"]


def test_research_side_columns_are_required(cfg, bars, tmp_path):
    database = paper_db(tmp_path / "paper.sqlite", [event("2025-01-06T01:00:00+00:00", 1, 1.0)])
    with pytest.raises(ValueError, match="BID/ASK"):
        execution_gap(paper_fills(database), bars, cfg)


def test_cli_reads_an_observation_directory_without_writing_it(cfg, bars, tmp_path, capsys):
    directory = tmp_path / "observation"
    (directory / "candles").mkdir(parents=True)
    side_bars(bars).to_parquet(directory / "candles" / "2025-01-06.parquet", index=False)
    (directory / "manifest.json").write_text(
        json.dumps({"config": cfg.model_dump(mode="json"), "config_sha256": "a" * 64})
    )
    database = paper_db(
        directory / "paper.sqlite", [event("2025-01-06T01:00:00+00:00", 500, 151.06)]
    )
    before = database.read_bytes()
    output = tmp_path / "gap.json"
    main(["--directory", str(directory), "--output", str(output)])
    printed = json.loads(capsys.readouterr().out)
    assert printed["compared"] == 1 and json.loads(output.read_text()) == printed
    assert database.read_bytes() == before
    with pytest.raises(SystemExit):
        main(["--directory", str(tmp_path / "missing")])
