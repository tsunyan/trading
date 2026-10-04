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
    # One closing order: +100 realized minus its own 3 fee; the opening fee stays in totals.
    assert result["closed_trades"] == {
        "count": 1,
        "wins": 1,
        "losses": 0,
        "win_rate": "1",
        "profit_factor": None,
    }
    assert [o["state"] for o in result["orders"]] == ["FILLED", "FILLED"]
    assert len(result["equity_history"]) == 2 and not result["broker_verified"]


def test_empty_journal_and_history_limits(tmp_path):
    journal = make_journal(tmp_path)
    empty = report(journal)
    assert empty["account"] is None and empty["orders"] == [] and empty["equity_history"] == []
    assert empty["totals"]["net"] == "0"
    assert empty["closed_trades"]["count"] == 0 and empty["closed_trades"]["win_rate"] is None
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


def test_slippage_against_the_reviewed_quote_from_the_dispatch_log(tmp_path):
    import json as jsonlib

    from trading.live_report import read_dispatches
    from trading.order_runtime import append_dispatch

    journal = make_journal(tmp_path, max_loss_jpy="20000")
    open_position(journal)  # Buy001 filled at 150.01.
    log = tmp_path / "dispatch.jsonl"
    append_dispatch(log, "Buy001", "submit", "a" * 64, quote(NOW, bid="149.99", ask="150"))
    with log.open("a", encoding="utf-8") as output:
        output.write("not json\n")
        output.write(jsonlib.dumps({"client_id": "X", "operation": "cancel"}) + "\n")
    dispatches = read_dispatches(log)
    assert set(dispatches) == {"Buy001"}
    result = report(journal, dispatches=dispatches)
    assert result["orders"][0]["slippage"] == "0.01"  # Paid 0.01 above the reviewed ask.
    assert result["execution"] == {"orders_measured": 1, "slippage_cost": "10"}
    assert read_dispatches(tmp_path / "missing.jsonl") == {}
    assert report(journal)["execution"] == {"orders_measured": 0, "slippage_cost": "0"}


def test_executions_export_lists_every_fill_once_and_never_overwrites(tmp_path):
    import csv

    from trading.live_report import executions, write_csv

    journal = make_journal(tmp_path)
    open_position(journal)
    items = executions(journal)
    assert [(i["client_id"], i["execution_id"], i["units"], i["price"]) for i in items] == [
        ("Buy001", 301, 1000, "150.01")
    ]
    path = tmp_path / "executions.csv"
    write_csv(items, path)
    with path.open(encoding="utf-8", newline="") as source:
        rows = list(csv.DictReader(source))
    assert rows[0]["fee"] == "3" and rows[0]["effect"] == "OPEN"
    with pytest.raises(FileExistsError):
        write_csv(items, path)


def test_trade_stats_count_wins_losses_and_profit_factor():
    from decimal import Decimal

    from trading.live_report import _trade_stats

    stats = _trade_stats([Decimal("120"), Decimal("-40"), Decimal("-20"), Decimal("0")])
    assert stats == {
        "count": 4,
        "wins": 1,
        "losses": 2,
        "win_rate": "0.25",
        "profit_factor": "2",
    }
