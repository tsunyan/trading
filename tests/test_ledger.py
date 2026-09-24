import json
import shutil
import sqlite3
from datetime import UTC, datetime

import pandas as pd
import pytest

from trading.backtest import save_run
from trading.cli import execute, parser
from trading.ledger import add_hypothesis, decide, freeze_hypothesis, record_run, summary

T0 = datetime(2026, 9, 25, tzinfo=UTC)
CODE = "c" * 64


@pytest.fixture
def ledger(tmp_path):
    database = tmp_path / "ledger.sqlite"
    add_hypothesis(database, "H001", "trend persists after a 24h move", now=T0)
    return database


def write_comparison(directory, bars, cfg, names=("01-sma_cross", "02-momentum"), **report):
    """The on-disk shape of a saved comparison; each call is a distinct run."""
    directory.mkdir()
    for name in names:
        candidate = directory / name
        candidate.mkdir()
        bars.to_parquet(candidate / "bars.parquet", index=False)
        (candidate / "config.json").write_text(cfg.model_dump_json(), encoding="utf-8")
        content = {
            "mode": "chronological_evaluation",
            "created_at": f"2026-09-25T00:00:00+00:00/{directory.name}/{name}",
            "warmup_bars": 3,
            "config_sha256": cfg.fingerprint,
            "code_sha256": CODE,
            "run_parameters": {"mode": "chronological_evaluation", "warmup_bars": 3},
            "verdict": {"status": "rejected"},
        }
        content.update(report)
        (candidate / "report.json").write_text(json.dumps(content), encoding="utf-8")
    (directory / "report.json").write_text(
        json.dumps({"candidates": [{"candidate": name} for name in names]}), encoding="utf-8"
    )


def entries(ledger):
    return summary(ledger)["hypotheses"][0]["entries"]


def test_every_comparison_candidate_counts_as_a_trial(ledger, bars, cfg, tmp_path):
    write_comparison(tmp_path / "comparison", bars, cfg)

    recorded = record_run(ledger, tmp_path / "comparison", "H001", "first look", imported=True)

    hypothesis = summary(ledger)["hypotheses"][0]
    assert len(recorded) == 2
    assert hypothesis["trials"] == 2
    assert hypothesis["distinct_configs"] == 1
    first = hypothesis["entries"][0]
    assert first["candidate"] == "01-sma_cross"
    assert first["imported"] == 1
    assert first["result_viewed"] == 1
    assert first["verdict"] == "rejected"
    assert first["strategy_parameters"] == {"fast": 2, "slow": 3}
    assert first["period"] == "research"
    # The run's own time is kept apart from when it was imported.
    assert first["run_created_at"].endswith("/comparison/01-sma_cross")
    assert first["recorded_at"] != first["run_created_at"]
    # Evaluations start deciding at the reported warm-up index.
    assert pd.Timestamp(first["evaluated_start"]) == bars.timestamp.iloc[3]


def test_backtest_run_records_provenance(ledger, bars, cfg, tmp_path):
    report = save_run(bars, cfg, tmp_path / "run")

    (entry_id,) = record_run(ledger, tmp_path / "run", "H001", "plumbing")

    (entry,) = entries(ledger)
    assert entry["entry_id"] == entry_id
    assert entry["mode"] == "backtest"
    assert entry["experiment_id"] == report["experiment_id"]
    assert entry["run_created_at"] == report["created_at"]
    assert entry["run_parameters"] == {"mode": "backtest"}
    assert entry["data_sha256"] == report["data_sha256"]
    assert entry["code_sha256"] == report["code_sha256"]
    # A backtest decides once it holds a full warm-up window (slow = 3 bars).
    assert pd.Timestamp(entry["evaluated_start"]) == bars.timestamp.iloc[2]


def test_runs_without_a_creation_time_are_recorded_as_unknown(ledger, bars, cfg, tmp_path):
    write_comparison(tmp_path / "old", bars, cfg, names=("01-sma_cross",), created_at=None)

    record_run(ledger, tmp_path / "old", "H001", "legacy", imported=True)

    assert entries(ledger)[0]["run_created_at"] is None


def test_copies_of_a_run_are_not_new_trials_but_reruns_are(ledger, bars, cfg, tmp_path):
    save_run(bars, cfg, tmp_path / "run")
    record_run(ledger, tmp_path / "run", "H001", "first")

    with pytest.raises(ValueError, match="already in the ledger"):
        record_run(ledger, tmp_path / "run", "H001", "again")
    shutil.copytree(tmp_path / "run", tmp_path / "run-copy")
    with pytest.raises(ValueError, match="identical saved report"):
        record_run(ledger, tmp_path / "run-copy", "H001", "copied")

    save_run(bars, cfg, tmp_path / "rerun")
    record_run(ledger, tmp_path / "rerun", "H001", "same config, run again")
    hypothesis = summary(ledger)["hypotheses"][0]
    assert hypothesis["trials"] == 2
    assert hypothesis["distinct_configs"] == 1


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


def test_forward_oos_requires_the_frozen_candidate(ledger, bars, cfg, tmp_path):
    write_comparison(tmp_path / "research", bars.iloc[:3], cfg, names=("01-sma_cross",))
    (frozen_entry,) = record_run(ledger, tmp_path / "research", "H001", "pick")
    # Freeze between the warm-up bars and the first decision bar of the later runs.
    frozen = freeze_hypothesis(
        ledger, "H001", frozen_entry, now=bars.timestamp.iloc[3].to_pydatetime()
    )
    assert frozen["frozen_entry_id"] == frozen_entry

    write_comparison(tmp_path / "after", bars, cfg, names=("01-sma_cross",))
    write_comparison(tmp_path / "before", bars.iloc[:3], cfg, names=("01-sma_cross",))
    write_comparison(tmp_path / "across", bars, cfg, names=("01-sma_cross",), warmup_bars=1)
    write_comparison(tmp_path / "new-code", bars, cfg, names=("01-sma_cross",), code_sha256="d")
    write_comparison(
        tmp_path / "new-config", bars, cfg.model_copy(update={"slow": 4}), names=("01-sma_cross",)
    )
    write_comparison(
        tmp_path / "new-conditions",
        bars,
        cfg,
        names=("01-sma_cross",),
        run_parameters={"mode": "chronological_evaluation", "warmup_bars": 3, "fold_count": 5},
    )
    names = ["after", "before", "across", "new-code", "new-config", "new-conditions"]
    for name in names:
        record_run(ledger, tmp_path / name, "H001", name)

    periods = {entry["purpose"]: entry["period"] for entry in entries(ledger)}
    assert [periods[name] for name in names] == [
        "forward_oos",
        "research",
        "mixed",
        "modified_after_freeze",
        "modified_after_freeze",
        "modified_after_freeze",
    ]
    with pytest.raises(ValueError, match="already frozen"):
        freeze_hypothesis(ledger, "H001", frozen_entry)


def test_freeze_needs_a_complete_candidate_of_the_same_hypothesis(ledger, bars, cfg, tmp_path):
    add_hypothesis(ledger, "H002", "another idea")
    write_comparison(tmp_path / "legacy", bars, cfg, names=("01-sma_cross",), code_sha256=None)
    write_comparison(tmp_path / "other", bars, cfg, names=("01-sma_cross",))
    (legacy,) = record_run(ledger, tmp_path / "legacy", "H001", "legacy")
    (other,) = record_run(ledger, tmp_path / "other", "H002", "other")

    with pytest.raises(ValueError, match="lacks code_sha256"):
        freeze_hypothesis(ledger, "H001", legacy)
    with pytest.raises(ValueError, match="does not belong"):
        freeze_hypothesis(ledger, "H001", other)
    assert summary(ledger, "H001")["hypotheses"][0]["frozen_at"] is None


def test_decisions_are_appended_and_the_latest_is_listed(ledger, bars, cfg, tmp_path):
    save_run(bars, cfg, tmp_path / "run")
    (entry_id,) = record_run(ledger, tmp_path / "run", "H001", "look")

    decide(ledger, entry_id, "revise", "needs swap history", now=T0)
    decide(ledger, entry_id, "reject", "negative after swap", now=T0)

    (entry,) = entries(ledger)
    assert entry["decision"] == "reject"
    assert entry["decision_reason"] == "negative after swap"
    with pytest.raises(ValueError, match="decision must be"):
        decide(ledger, entry_id, "maybe", "unsure")
    with pytest.raises(ValueError, match="does not exist"):
        decide(ledger, entry_id + 1, "reject", "no such entry")


def test_a_ledger_with_another_schema_is_refused(tmp_path):
    database = tmp_path / "old.sqlite"
    with sqlite3.connect(database) as connection:
        connection.execute("CREATE TABLE entries (entry_id INTEGER PRIMARY KEY)")

    with pytest.raises(ValueError, match="ledger schema 0, expected 1"):
        summary(database)


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

    add = ["ledger", "add-hypothesis", "--id", "H001", "--description", "smoke"]
    execute(parser().parse_args([*add, "--database", str(database)]))
    result = execute(
        parser().parse_args(["backtest", *common, *trial, "--output", str(tmp_path / "b")])
    )

    assert result["ledger_entries"] == [1]
    listed = execute(parser().parse_args(["ledger", "list", "--database", str(database)]))
    assert listed["hypotheses"][0]["trials"] == 1
    frozen = execute(
        parser().parse_args(
            ["ledger", "freeze", "--id", "H001", "--entry", "1", "--database", str(database)]
        )
    )
    assert frozen["frozen_entry_id"] == 1
    with pytest.raises(SystemExit):
        parser().parse_args(["backtest", *common, "--output", str(tmp_path / "c")])
