"""Static live dashboard rendering from temporary stores; no server or network."""

import ctypes
import socket

import pytest
from test_live_operations import setup as operations_setup
from test_live_operations import unbound as operations_unbound

from trading import live_dashboard
from trading.live_doctor import diagnose
from trading.live_report import report


@pytest.fixture(autouse=True)
def no_native_or_network(monkeypatch):
    def forbidden(*args, **kwargs):
        pytest.fail("real credentials/network attempted")

    monkeypatch.setattr(ctypes, "WinDLL", forbidden, raising=False)
    monkeypatch.setattr(socket, "socket", forbidden)


@pytest.fixture
def setup(tmp_path):
    yield from operations_setup.__wrapped__(operations_unbound.__wrapped__(tmp_path))


def test_page_shows_gates_account_orders_and_escapes_cycle_text(setup):
    values, live = setup[0], setup[1]
    journal = live[3]
    before = journal.snapshot()
    cycle = {
        "ok": False,
        "reason": "<script>alert(1)</script>",
        "finished_at": "2026-10-04T05:00:00+00:00",
    }
    page = live_dashboard.render(
        diagnose(journal, values[0].wall), report(journal), cycle, generated_at="now"
    )
    assert "送信可能" in page and "Buy001" in page and "PREPARED" in page
    assert "True" not in page and "はい" in page and "決済注文ごとの成績" in page
    assert "<script>" not in page and "&lt;script&gt;" in page
    assert 'http-equiv="refresh"' in page and "prefers-color-scheme: dark" in page
    assert journal.snapshot() == before


def test_page_without_proof_or_history_still_renders(tmp_path):
    values, live, _ = operations_unbound.__wrapped__(tmp_path)
    journal = live[3]
    page = live_dashboard.render(
        diagnose(journal, values[0].wall), report(journal), None, generated_at="now"
    )
    assert "送信不可" in page and "2件以上" in page and "指定されていません" in page


def test_sparkline_scales_points():
    svg = live_dashboard._sparkline(
        [{"equity": "1000000"}, {"equity": "999000"}, {"equity": "1001000"}]
    )
    assert svg.count(",") >= 3 and "polyline" in svg


def test_cli_writes_the_page_atomically(setup, tmp_path, capsys):
    live = setup[1]
    output = tmp_path / "live.html"
    output.write_text("old")
    live_dashboard.main(
        [
            "--directory",
            str(live[3].path.parent),
            "--read-control-directory",
            str(live[1].path.parent),
            "--scope",
            "synthetic",
            "--output",
            str(output),
            "--cycle-result",
            str(tmp_path / "missing.json"),
        ]
    )
    assert output.read_text(encoding="utf-8").startswith("<!doctype html>")
    assert [p.name for p in tmp_path.iterdir() if p.name.startswith(".dashboard-")] == []
    assert '"network_used": false' in capsys.readouterr().out


def test_history_lists_the_newest_cycles_first_and_shows_broken_lines(tmp_path, monkeypatch):
    import json

    path = tmp_path / "cycles.jsonl"
    lines = [
        json.dumps({"ok": True, "finished_at": f"t{i}", "decision": {"action": "hold"}})
        for i in range(30)
    ]
    path.write_text("\n".join([*lines[:-1], "{broken", lines[-1]]) + "\n", encoding="utf-8")
    history = live_dashboard.read_history(path)
    assert history[0]["finished_at"] == "t29" and len(history) == 24
    assert history[1]["reason"] == "damaged_history_line"  # Visible, not dropped.
    assert history[-1]["finished_at"] == "t7"
    assert live_dashboard.read_history(tmp_path / "missing.jsonl") == []
    # Only the end of the file is read, in small chunks.
    monkeypatch.setattr(live_dashboard, "TAIL_CHUNK", 7)
    assert live_dashboard.read_history(path) == history
    page = live_dashboard.render(
        {
            "send_ready": True,
            "gates": {},
            "entry_halted": False,
            "approval_expires_at": None,
            "approval_seconds_left": None,
        },
        {"account": None, "totals": {}, "orders": [], "equity_history": [], "closing_orders": {}},
        None,
        generated_at="now",
        history=history,
    )
    assert "サイクルの履歴" in page and "t29" in page
