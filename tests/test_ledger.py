import json
from datetime import UTC, datetime

import pandas as pd
import pytest

from trading.backtest import save_run
from trading.cli import execute, parser
from trading.ledger import add_hypothesis, decide, freeze_hypothesis, record_run, summary

T0 = datetime(2026, 9, 25, tzinfo=UTC)


@pytest.fixture
def ledger(tmp_path):
    database = tmp_path / "ledger.sqlite"
    add_hypothesis(database, "H001", "trend persists after a 24h move", now=T0)
    return database


def write_comparison(directory, bars, cfg, names=("01-sma_cross", "02-momentum")):
    """The on-disk shape of a saved comparison, as older runs left it (no comparison_id)."""
    directory.mkdir()
    for name in names:
        candidate = directory / name
        candidate.mkdir()
        bars.to_parquet(candidate / "bars.parquet", index=False)
        (candidate / "config.json").write_text(cfg.model_dump_json(), encoding="utf-8")
        (candidate / "report.json").write_text(
            json.dumps(
                {
                    "mode": "chronological_evaluation",
                    "warmup_bars": 3,
                    "verdict": {"status": "rejected"},
                }
            ),
            encoding="utf-8",
        )
    (directory / "report.json").write_text(
        json.dumps({"candidates": [{"candidate": name} for name in names]}), encoding="utf-8"
    )


def test_every_comparison_candidate_counts_as_a_trial(ledger, bars, cfg, tmp_path):
    write_comparison(tmp_path / "comparison", bars, cfg)

    entries = record_run(ledger, tmp_path / "comparison", "H001", "first look", imported=True)

    hypothesis = summary(ledger)["hypotheses"][0]
    assert len(entries) == 2
    assert hypothesis["trials"] == 2
    assert hypothesis["distinct_configs"] == 1
    first = hypothesis["entries"][0]
    assert first["candidate"] == "01-sma_cross"
    assert first["imported"] == 1
    assert first["result_viewed"] == 1
    assert first["verdict"] == "rejected"
    assert first["strategy_parameters"] == {"fast": 2, "slow": 3}
    assert first["period"] == "research"
    # Evaluations start deciding at the reported warm-up index.
    assert pd.Timestamp(first["evaluated_start"]) == bars.timestamp.iloc[3]


def test_backtest_run_records_provenance(ledger, bars, cfg, tmp_path):
    report = save_run(bars, cfg, tmp_path / "run")

    (entry_id,) = record_run(ledger, tmp_path / "run", "H001", "plumbing")

    entry = summary(ledger)["hypotheses"][0]["entries"][0]
    assert entry["entry_id"] == entry_id
    assert entry["mode"] == "backtest"
    assert entry["experiment_id"] == report["experiment_id"]
    assert entry["data_sha256"] == report["data_sha256"]
    assert entry["code_sha256"] == report["code_sha256"]
    # A backtest decides once it holds a full warm-up window (slow = 3 bars).
    assert pd.Timestamp(entry["evaluated_start"]) == bars.timestamp.iloc[2]


def test_the_same_run_cannot_be_recorded_twice(ledger, bars, cfg, tmp_path):
    save_run(bars, cfg, tmp_path / "run")
    record_run(ledger, tmp_path / "run", "H001", "first")

    with pytest.raises(ValueError, match="already in the ledger"):
        record_run(ledger, tmp_path / "run", "H001", "again")
    assert summary(ledger)["hypotheses"][0]["trials"] == 1


def test_runs_need_a_registered_hypothesis_and_a_purpose(ledger, bars, cfg, tmp_path):
    save_run(bars, cfg, tmp_path / "run")

    with pytest.raises(ValueError, match="not registered"):
        record_run(ledger, tmp_path / "run", "H999", "look")
    with pytest.raises(ValueError, match="purpose"):
        record_run(ledger, tmp_path / "run", "H001", " ")
    with pytest.raises(ValueError, match="already exists"):
        add_hypothesis(ledger, "H001", "again")
    with pytest.raises(ValueError, match="hypothesis id"):
        add_hypothesis(ledger, "H 2", "spaces are not ids")


def test_period_is_judged_from_the_first_decision_bar(ledger, bars, cfg, tmp_path):
    # Freeze between the warm-up bars and the first decision bar of a comparison run.
    freeze_hypothesis(ledger, "H001", now=bars.timestamp.iloc[3].to_pydatetime())
    write_comparison(tmp_path / "after", bars, cfg, names=("01-sma_cross",))
    write_comparison(tmp_path / "before", bars.iloc[:3], cfg, names=("01-sma_cross",))
    write_comparison(tmp_path / "across", bars, cfg, names=("01-sma_cross",))
    across = json.loads((tmp_path / "across/01-sma_cross/report.json").read_text())
    across["warmup_bars"] = 1
    (tmp_path / "across/01-sma_cross/report.json").write_text(json.dumps(across))

    for name in ("after", "before", "across"):
        record_run(ledger, tmp_path / name, "H001", name)

    periods = [entry["period"] for entry in summary(ledger)["hypotheses"][0]["entries"]]
    assert periods == ["forward_oos", "research", "mixed"]
    with pytest.raises(ValueError, match="already frozen"):
        freeze_hypothesis(ledger, "H001")


def test_decisions_are_appended_and_the_latest_is_listed(ledger, bars, cfg, tmp_path):
    save_run(bars, cfg, tmp_path / "run")
    (entry_id,) = record_run(ledger, tmp_path / "run", "H001", "look")

    decide(ledger, entry_id, "revise", "needs swap history", now=T0)
    decide(ledger, entry_id, "reject", "negative after swap", now=T0)

    entry = summary(ledger)["hypotheses"][0]["entries"][0]
    assert entry["decision"] == "reject"
    assert entry["decision_reason"] == "negative after swap"
    with pytest.raises(ValueError, match="decision must be"):
        decide(ledger, entry_id, "maybe", "unsure")
    with pytest.raises(ValueError, match="does not exist"):
        decide(ledger, entry_id + 1, "reject", "no such entry")


def test_trial_commands_refuse_to_run_without_a_registered_hypothesis(bars, tmp_path):
    config = tmp_path / "fx.toml"
    config.write_text(
        'market = "fx"\nsymbol = "USD_JPY"\nbar_seconds = 3600\nfast = 2\nslow = 3\n',
        encoding="utf-8",
    )
    data, database = tmp_path / "bars.parquet", tmp_path / "ledger.sqlite"
    bars.to_parquet(data, index=False)
    common = ["--config", str(config), "--data", str(data), "--ledger", str(database)]
    trial = ["--hypothesis", "H001", "--purpose", "smoke"]

    with pytest.raises(ValueError, match="not registered"):
        execute(parser().parse_args(["backtest", *common, *trial, "--output", str(tmp_path / "a")]))
    assert not (tmp_path / "a").exists()

    execute(
        parser().parse_args(
            [
                "ledger",
                "add-hypothesis",
                "--id",
                "H001",
                "--description",
                "smoke",
                "--database",
                str(database),
            ]
        )
    )
    result = execute(
        parser().parse_args(["backtest", *common, *trial, "--output", str(tmp_path / "b")])
    )

    assert result["ledger_entries"] == [1]
    listed = execute(parser().parse_args(["ledger", "list", "--database", str(database)]))
    assert listed["hypotheses"][0]["trials"] == 1
    with pytest.raises(SystemExit):
        parser().parse_args(["backtest", *common, "--output", str(tmp_path / "c")])
