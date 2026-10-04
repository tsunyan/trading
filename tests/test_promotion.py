"""Promotion of a frozen candidate to paper and live, on a temporary experiment ledger."""

import sqlite3

import pytest
from test_ledger import CODE, T0, write_comparison

from trading import promotion
from trading.ledger import add_hypothesis, decide, freeze_hypothesis, record_run
from trading.promotion import (
    PromotionError,
    judge,
    promote,
    require_live,
    revoke,
    set_criteria,
    status,
)


@pytest.fixture
def frozen(tmp_path, bars, cfg):
    database = tmp_path / "ledger.sqlite"
    add_hypothesis(database, "H001", "trend persists", now=T0)
    write_comparison(tmp_path / "research", bars.iloc[:3], cfg, names=("01-sma_cross",))
    (entry,) = record_run(database, tmp_path / "research", "H001", "pick")
    freeze_hypothesis(database, "H001", entry, now=bars.timestamp.iloc[3].to_pydatetime())
    return database


def promote_live(database, tmp_path, bars, cfg):
    """Paper, fixed criteria, a judged passing forward entry, then live."""
    promote(database, "H001", "paper", "start paper")
    set_criteria(database, "H001", CRITERIA)
    good = forward_run(database, tmp_path, bars, cfg, "good", profit_factor=1.5, max_drawdown_pct=6)
    assert judge(database, "H001", good)["decision"] == "advance"
    return promote(database, "H001", "live", "fixed criteria passed")


def test_paper_then_live_requires_fixed_criteria_and_a_judged_advance(frozen, tmp_path, bars, cfg):
    assert status(frozen, "H001")["stage"] is None
    with pytest.raises(PromotionError, match="paper_stage_required"):
        promote(frozen, "H001", "live", "too early")
    assert promote(frozen, "H001", "paper", "frozen; start paper")["stage"] == "paper"
    with pytest.raises(PromotionError, match="already_promoted"):
        promote(frozen, "H001", "paper", "again")
    with pytest.raises(PromotionError, match="forward_criteria_required"):
        promote(frozen, "H001", "live", "no criteria fixed")
    # Without fixed criteria even an advanced forward entry is not enough.
    early = forward_run(frozen, tmp_path, bars, cfg, "early", profit_factor=2, max_drawdown_pct=1)
    decide(frozen, early, "advance", "looked good")
    with pytest.raises(PromotionError, match="forward_criteria_required"):
        promote(frozen, "H001", "live", "manual advance")


def test_live_check_refuses_other_settings_code_and_revoked_candidates(frozen, tmp_path, bars, cfg):
    live = promote_live(frozen, tmp_path, bars, cfg)
    assert [h["action"] for h in live["history"]] == ["paper", "live"]
    assert require_live(frozen, "H001", cfg, code_sha256=CODE)["config_sha256"] == cfg.fingerprint
    with pytest.raises(PromotionError, match="strategy_code_differs_from_candidate"):
        require_live(frozen, "H001", cfg)  # This checkout's package hash is not the frozen one.
    with pytest.raises(PromotionError, match="strategy_config_differs_from_candidate"):
        require_live(frozen, "H001", cfg.model_copy(update={"slow": 4}), code_sha256=CODE)
    with pytest.raises(PromotionError, match="strategy_config_differs_from_candidate"):
        require_live(frozen, "H001", cfg.model_copy(update={"max_units": 900}), code_sha256=CODE)
    assert revoke(frozen, "H001", "live loss review")["stage"] == "revoked"
    with pytest.raises(PromotionError, match="strategy_not_promoted_for_live"):
        require_live(frozen, "H001", cfg, code_sha256=CODE)
    with pytest.raises(PromotionError, match="nothing_to_revoke"):
        revoke(frozen, "H001", "again")


def test_unfrozen_hypothesis_cannot_be_promoted(tmp_path):
    database = tmp_path / "ledger.sqlite"
    add_hypothesis(database, "H002", "not frozen", now=T0)
    with pytest.raises(PromotionError, match="frozen_candidate_required"):
        promote(database, "H002", "paper", "no")
    with pytest.raises(PromotionError, match="promotion_reason_required"):
        promote(database, "H002", "paper", " ")


def test_ledger_schema_version_is_unchanged(frozen):
    promote(frozen, "H001", "paper", "start")
    with sqlite3.connect(frozen) as conn:
        assert conn.execute("PRAGMA user_version").fetchone()[0] == 1


def test_cli_check_reports_fixed_reasons(frozen, tmp_path, cfg, capsys):
    config = tmp_path / "fx.toml"
    config.write_text(
        "\n".join(
            f"{k} = {v!r}".replace("'", '"')
            for k, v in cfg.model_dump().items()
            if not isinstance(v, bool)
        )
        + "\n"
    )
    with pytest.raises(SystemExit):
        promotion.main(
            ["check", "--ledger", str(frozen), "--hypothesis", "H001", "--config", str(config)]
        )
    assert capsys.readouterr().err == "promotion_failed: strategy_not_promoted_for_live\n"


CRITERIA = [
    {"path": "metrics.profit_factor", "op": ">=", "value": 1.2},
    {"path": "metrics.max_drawdown_pct", "op": "<=", "value": 10},
]


def forward_run(database, tmp_path, bars, cfg, name, **metrics):
    write_comparison(tmp_path / name, bars, cfg, names=("01-sma_cross",), metrics=metrics)
    (entry,) = record_run(database, tmp_path / name, "H001", name)
    return entry


def test_fixed_criteria_judge_forward_entries_and_gate_live_promotion(frozen, tmp_path, bars, cfg):
    promote(frozen, "H001", "paper", "start paper")
    assert set_criteria(frozen, "H001", CRITERIA)["criteria"] == CRITERIA
    with pytest.raises(PromotionError, match="forward_criteria_already_fixed"):
        set_criteria(frozen, "H001", CRITERIA)
    weak = forward_run(frozen, tmp_path, bars, cfg, "weak", profit_factor=1.1, max_drawdown_pct=4)
    verdict = judge(frozen, "H001", weak)
    assert verdict["decision"] == "reject"
    assert [c["passed"] for c in verdict["checks"]] == [False, True]
    with pytest.raises(PromotionError, match="advanced_forward_oos_entry_required"):
        promote(frozen, "H001", "live", "rejected")
    good = forward_run(frozen, tmp_path, bars, cfg, "good", profit_factor=1.5, max_drawdown_pct=6)
    assert judge(frozen, "H001", good)["decision"] == "advance"
    assert promote(frozen, "H001", "live", "fixed criteria passed")["stage"] == "live"


def test_criteria_must_come_before_forward_results(frozen, tmp_path, bars, cfg):
    forward_run(frozen, tmp_path, bars, cfg, "early", profit_factor=2.0, max_drawdown_pct=1)
    with pytest.raises(PromotionError, match="forward_results_already_recorded"):
        set_criteria(frozen, "H001", CRITERIA)


@pytest.mark.parametrize(
    "criteria",
    [
        [],
        [{"path": "x", "op": "==", "value": 1}],
        [{"path": "x", "op": ">=", "value": True}],
        [{"path": "x", "op": ">=", "value": float("nan")}],
        [{"path": "x", "op": "<=", "value": float("inf")}],
    ],
)
def test_invalid_criteria_are_refused(frozen, criteria):
    with pytest.raises(PromotionError, match="invalid_forward_criteria"):
        set_criteria(frozen, "H001", criteria)


def test_judge_refuses_research_entries_and_changed_reports(frozen, tmp_path, bars, cfg):
    set_criteria(frozen, "H001", CRITERIA)
    with pytest.raises(PromotionError, match="forward_oos_entry_required"):
        judge(frozen, "H001", 1)  # The frozen research entry.
    entry = forward_run(frozen, tmp_path, bars, cfg, "fwd", profit_factor=1.5, max_drawdown_pct=6)
    report = tmp_path / "fwd" / "01-sma_cross" / "report.json"
    report.write_text(report.read_text().replace("1.5", "9.5"))
    with pytest.raises(PromotionError, match="report_changed_since_recording"):
        judge(frozen, "H001", entry)


def test_manual_advance_does_not_bypass_fixed_criteria(frozen, tmp_path, bars, cfg):
    promote(frozen, "H001", "paper", "start paper")
    set_criteria(frozen, "H001", CRITERIA)
    weak = forward_run(frozen, tmp_path, bars, cfg, "weak", profit_factor=1.0, max_drawdown_pct=4)
    decide(frozen, weak, "advance", "looks fine to me")
    with pytest.raises(PromotionError, match="advanced_forward_oos_entry_required"):
        promote(frozen, "H001", "live", "manual override")
    # Imitating judge()'s reason text does not make a decision judge-made.
    with sqlite3.connect(frozen) as conn:
        (sha,) = conn.execute("SELECT criteria_sha256 FROM forward_criteria").fetchone()
    decide(frozen, weak, "advance", f"fixed criteria {sha[:12]}: forged pass")
    with pytest.raises(PromotionError, match="advanced_forward_oos_entry_required"):
        promote(frozen, "H001", "live", "forged reason")
    # A later manual advance on a judged-rejected entry does not count either.
    assert judge(frozen, "H001", weak)["decision"] == "reject"
    decide(frozen, weak, "advance", "override after judge")
    with pytest.raises(PromotionError, match="advanced_forward_oos_entry_required"):
        promote(frozen, "H001", "live", "override")


def test_judge_refuses_oversized_reports(frozen, tmp_path, bars, cfg, monkeypatch):
    set_criteria(frozen, "H001", CRITERIA)
    entry = forward_run(frozen, tmp_path, bars, cfg, "fwd", profit_factor=1.5, max_drawdown_pct=6)
    monkeypatch.setattr(promotion, "MAX_REPORT", 10)
    with pytest.raises(PromotionError, match="forward_report_too_large"):
        judge(frozen, "H001", entry)


def test_reads_never_add_tables_to_the_research_ledger(frozen, cfg):
    def tables():
        with sqlite3.connect(frozen) as conn:
            return {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}

    before = tables()
    assert status(frozen, "H001")["stage"] is None
    with pytest.raises(PromotionError, match="strategy_not_promoted_for_live"):
        require_live(frozen, "H001", cfg)
    assert tables() == before and "promotions" not in before


def test_missing_ledger_is_not_created_by_a_read(tmp_path, cfg):
    missing = tmp_path / "nowhere" / "ledger.sqlite"
    with pytest.raises(PromotionError, match="ledger_not_found"):
        require_live(missing, "H001", cfg)
    assert not missing.exists() and not missing.parent.exists()
