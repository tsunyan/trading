"""Promotion of a frozen candidate to paper and live, on a temporary experiment ledger."""

import sqlite3

import pytest
from test_ledger import T0, write_comparison

from trading import promotion
from trading.ledger import add_hypothesis, decide, freeze_hypothesis, record_run
from trading.promotion import PromotionError, promote, require_live, revoke, status


@pytest.fixture
def frozen(tmp_path, bars, cfg):
    database = tmp_path / "ledger.sqlite"
    add_hypothesis(database, "H001", "trend persists", now=T0)
    write_comparison(tmp_path / "research", bars.iloc[:3], cfg, names=("01-sma_cross",))
    (entry,) = record_run(database, tmp_path / "research", "H001", "pick")
    freeze_hypothesis(database, "H001", entry, now=bars.timestamp.iloc[3].to_pydatetime())
    return database


def forward(database, tmp_path, bars, cfg, decision):
    write_comparison(tmp_path / "after", bars, cfg, names=("01-sma_cross",))
    (entry,) = record_run(database, tmp_path / "after", "H001", "forward")
    decide(database, entry, decision, "fixed criteria checked before viewing")
    return entry


def test_paper_then_live_requires_an_advanced_forward_entry(frozen, tmp_path, bars, cfg):
    assert status(frozen, "H001")["stage"] is None
    with pytest.raises(PromotionError, match="paper_stage_required"):
        promote(frozen, "H001", "live", "too early")
    assert promote(frozen, "H001", "paper", "frozen; start paper")["stage"] == "paper"
    with pytest.raises(PromotionError, match="already_promoted"):
        promote(frozen, "H001", "paper", "again")
    with pytest.raises(PromotionError, match="advanced_forward_oos_entry_required"):
        promote(frozen, "H001", "live", "no forward evidence")
    forward(frozen, tmp_path, bars, cfg, "reject")
    with pytest.raises(PromotionError, match="advanced_forward_oos_entry_required"):
        promote(frozen, "H001", "live", "rejected forward")
    with sqlite3.connect(frozen) as conn:
        entry = conn.execute("SELECT max(entry_id) FROM entries").fetchone()[0]
    decide(frozen, entry, "advance", "forward criteria met")
    forward_advanced = promote(frozen, "H001", "live", "forward OOS passed")
    assert forward_advanced["stage"] == "live"
    assert [h["action"] for h in forward_advanced["history"]] == ["paper", "live"]
    assert require_live(frozen, "H001", cfg)["config_sha256"] == cfg.fingerprint


def test_live_check_refuses_other_settings_and_revoked_candidates(frozen, tmp_path, bars, cfg):
    promote(frozen, "H001", "paper", "start paper")
    forward(frozen, tmp_path, bars, cfg, "advance")
    promote(frozen, "H001", "live", "passed")
    with pytest.raises(PromotionError, match="strategy_config_differs_from_candidate"):
        require_live(frozen, "H001", cfg.model_copy(update={"slow": 4}))
    with pytest.raises(PromotionError, match="strategy_config_differs_from_candidate"):
        require_live(frozen, "H001", cfg.model_copy(update={"max_units": 900}))
    assert revoke(frozen, "H001", "live loss review")["stage"] == "revoked"
    with pytest.raises(PromotionError, match="strategy_not_promoted_for_live"):
        require_live(frozen, "H001", cfg)
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
