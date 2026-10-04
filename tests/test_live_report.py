"""Read-only profit and risk report from journal evidence, with temporary journals only."""

import socket
from datetime import timedelta

import pytest
from test_account_guard import (
    NOW,
    fill,
    intent,
    make_journal,
    open_position,
    positioned_account,
    quote,
)

from trading import live_report
from trading.broker_contracts import Settlement
from trading.execution_lab import fixture_evidence
from trading.live_report import report


@pytest.fixture(autouse=True)
def no_network(monkeypatch):
    monkeypatch.setattr(socket, "socket", lambda *a, **k: pytest.fail("real network attempted"))


def test_open_then_closed_position_reports_realized_net_and_risk_room(tmp_path):
    journal = make_journal(tmp_path, max_loss_jpy="20000")
    open_position(journal)
    later = NOW + timedelta(seconds=2)
    journal.update_account(positioned_account(later), quote(later), now=later)
    opened = report(journal)
    assert opened["account"]["equity"] == "999987" and len(opened["account"]["positions"]) == 1
    assert opened["account"]["loss_from_start"] == "13"
    assert opened["account"]["loss_limit_remaining"] == "19987"
    assert opened["orders"][0]["filled_units"] == 1000
    assert opened["orders"][0]["average_price"] == "150.01"
    assert opened["totals"] == {"realized": "0", "fees": "3", "settled_swap": "0", "net": "-3"}

    close = intent(
        client_id="Close",
        side="SELL",
        effect="CLOSE",
        price="150.11",
        positions=(Settlement(position_id=401, units=1000),),
    )
    journal.prepare(close)
    journal.begin_submission("Close", quote=quote(later, bid="150.11", ask="150.12"), now=later)
    closed_at = later + timedelta(seconds=1)
    journal.reconcile(
        fixture_evidence(
            close,
            102,
            202,
            "EXECUTED",
            [fill(302, price="150.11", fee="-3", lossGain="100", timestamp=closed_at.isoformat())],
            closed_at,
        )
    )
    result = report(journal)
    assert result["totals"] == {"realized": "100", "fees": "6", "settled_swap": "0", "net": "94"}
    assert [o["state"] for o in result["orders"]] == ["FILLED", "FILLED"]
    assert len(result["equity_history"]) == 2 and not result["broker_verified"]


def test_empty_journal_and_history_limits(tmp_path):
    journal = make_journal(tmp_path)
    empty = report(journal)
    assert empty["account"] is None and empty["orders"] == [] and empty["equity_history"] == []
    assert empty["totals"]["net"] == "0"
    for history in (-1, 1001, True):
        with pytest.raises(ValueError):
            report(journal, history=history)


def test_cli_hides_failures(tmp_path, capsys):
    with pytest.raises(SystemExit) as raised:
        live_report.main(
            [
                "--directory",
                str(tmp_path / "missing"),
                "--read-control-directory",
                str(tmp_path / "reads"),
                "--scope",
                "synthetic",
            ]
        )
    assert raised.value.code == 2
    assert capsys.readouterr().err.startswith("live_report_failed:")
