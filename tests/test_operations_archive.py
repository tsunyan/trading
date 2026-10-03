"""Retain exact notification history across capacity while keeping unsent work live."""

import hashlib
import json
import sqlite3
import subprocess
import sys
from datetime import timedelta

import pytest
from test_private_operations import no_credentials_or_network as no_credentials_or_network
from test_private_operations import setup as setup
from test_private_operations import stop

from trading import private_operations
from trading.private_operations import Alert, OperationsError, PrivateOperations, _body, _hash


def compact_history(setup, monkeypatch):
    clock, _, _, _, monitor = setup
    monkeypatch.setattr(private_operations, "MAX_ALERTS", 3)
    monitor.test_notification()  # 1: submitted and eligible.
    monitor.notify(send=lambda *a: None)
    monitor.test_notification()  # 2: pending, cannot archive.
    monitor.test_notification()  # 3: submitted and eligible.
    with sqlite3.connect(monitor.path) as conn:
        conn.execute(
            "UPDATE alerts SET last_attempt_at=?,attempts=1,submitted_at=? WHERE id=3",
            (clock.wall.isoformat(), clock.wall.isoformat()),
        )
    monitor.test_notification()  # 4: archives 1 and 3, preserving 2 in the outbox.
    return monitor


def test_archive_preserves_exact_bodies_ids_delivery_state_and_late_ack(setup, monkeypatch):
    clock, _, backend, workspace, _ = setup
    monitor = compact_history(setup, monkeypatch)
    with sqlite3.connect(monitor.path) as conn:
        archived = json.loads(conn.execute("SELECT payload FROM alert_archives").fetchone()[0])
        assert [r[0] for r in archived] == [1, 3]
        assert all(_hash(r[1]) == r[2] for r in archived)
        assert conn.execute("SELECT id FROM alerts WHERE body<>'' ORDER BY id").fetchall() == [
            (2,),
            (4,),
        ]
    saved = monitor.status()
    assert saved["alert_count"] == 4 and saved["archived_alerts"] == 2
    assert saved["active_alerts"] == 2 and saved["notifications_waiting_submission"] == 2
    assert saved["unacknowledged_alerts"] == 4
    assert [r["id"] for r in monitor.alerts()] == [1, 2, 3, 4]
    assert monitor.alerts()[0]["submitted_at"] is not None
    sent = []
    monitor.notify(send=lambda a, _: sent.append(a["id"]))
    assert sent == [2, 4]
    clock.advance(1)
    monitor.acknowledge(1)
    assert monitor.status()["unacknowledged_alerts"] == 3
    assert [r["id"] for r in monitor.alerts()] == [2, 3, 4]
    with pytest.raises(OperationsError, match="not_pending"):
        monitor.acknowledge(1)
    fresh = PrivateOperations(workspace.directory, clock=monitor.clock)
    assert fresh.audit_history()["alerts"] == 4 and backend.reads == []
    assert fresh.notify(send=lambda *a: pytest.fail("archived alert replayed"))["submitted"] == 0


def test_late_ack_after_multiple_archives_does_not_reverse_seal_order(setup, monkeypatch):
    clock, _, _, _, _ = setup
    monitor = compact_history(setup, monkeypatch)
    monitor.notify(send=lambda *a: None)
    for _ in range(8):
        clock.advance(1)
        monitor.test_notification()
        monitor.notify(send=lambda *a: None)
    assert monitor.status()["archive_count"] > 1
    clock.advance(1)
    monitor.acknowledge(1)
    assert monitor.audit_history()["alerts"] == 12
    clock.wall -= timedelta(seconds=1)
    with pytest.raises(OperationsError, match="clock_invalid"):
        monitor.test_notification()


def test_unresolved_condition_is_retained_even_after_submission_and_ack(setup, monkeypatch):
    _, _, _, workspace, monitor = setup
    monkeypatch.setattr(private_operations, "MAX_ALERTS", 2)
    stop(workspace)
    monitor.check()
    monitor.notify(send=lambda *a: None)
    monitor.acknowledge(1)
    monitor.test_notification()
    monitor.notify(send=lambda *a: None)
    monitor.test_notification()
    with sqlite3.connect(monitor.path) as conn:
        assert conn.execute("SELECT body,resolved_at FROM alerts WHERE id=1").fetchone()[0] != ""
    assert monitor.check()["conditions"] == ["private_sync_stopped"]
    saved = workspace.control.snapshot()
    workspace.recover(
        expected_plan_sha256=workspace.plan_sha256,
        expected_revision=saved["revision"],
        expected_head=workspace.journal.head(),
        expected_reason=saved["reason"],
    )
    monitor.notify(send=lambda *a: None)
    assert monitor.check()["conditions"] == []
    with sqlite3.connect(monitor.path) as conn:
        assert conn.execute("SELECT resolved_at FROM alerts WHERE id=1").fetchone()[0] is not None


@pytest.mark.parametrize(
    "sql",
    [
        "UPDATE alert_archives SET payload=zeroblob(length(payload))",
        "UPDATE alert_archives SET payload=substr(payload,1,length(payload)-1)",
        "UPDATE alert_archives SET digest='bad'",
        "DELETE FROM alert_archives",
        "DROP TABLE alert_archives",
        "DELETE FROM alert_archive_index WHERE id=1",
        "UPDATE alert_archive_index SET kind='private_sync_stopped' WHERE id=1",
        "UPDATE alert_archive_index SET archive=999 WHERE id=1",
        "DELETE FROM alerts WHERE id=1",
        "UPDATE alerts SET digest='bad' WHERE id=1",
        "UPDATE alerts SET body='{}' WHERE id=1",
        "UPDATE alerts SET last_attempt_at=NULL WHERE id=1",
    ],
)
def test_archive_or_delivery_corruption_fails_closed_without_touching_runtime(
    setup, monkeypatch, sql
):
    _, _, _, workspace, _ = setup
    monitor = compact_history(setup, monkeypatch)
    saved = workspace.control.snapshot()
    with sqlite3.connect(monitor.path) as conn:
        conn.execute(sql)
    before = monitor.path.read_bytes()
    for action in (monitor.status, monitor.check, monitor.audit_history):
        with pytest.raises(OperationsError):
            action()
    with pytest.raises(OperationsError):
        PrivateOperations(workspace.directory)
    assert monitor.path.read_bytes() == before and workspace.control.snapshot() == saved


@pytest.mark.parametrize("legacy", [False, True])
def test_archive_storage_failure_rolls_back_body_move_index_and_new_alert(
    setup, monkeypatch, legacy
):
    _, _, _, workspace, monitor = setup
    monkeypatch.setattr(private_operations, "MAX_ALERTS", 1)
    monitor.test_notification()
    monitor.notify(send=lambda *a: None)
    with sqlite3.connect(monitor.path) as conn:
        if legacy:
            conn.execute("DROP TABLE alert_archives")
            conn.execute("DROP TABLE alert_archive_index")
        conn.execute(
            "CREATE TRIGGER fail_compaction BEFORE UPDATE OF body ON alerts "
            "WHEN NEW.body='' BEGIN SELECT RAISE(ABORT, 'synthetic'); END"
        )
    before = monitor.path.read_bytes()
    with pytest.raises(OperationsError):
        monitor.test_notification()
    assert monitor.path.read_bytes() == before
    fresh = PrivateOperations(workspace.directory, clock=monitor.clock)
    assert fresh.status()["alert_count"] == 1 and fresh.status()["archive_count"] == 0
    assert fresh.alerts()[0]["id"] == 1


def test_reopen_legacy_store_does_not_create_archives_until_capacity_mutation(setup, monkeypatch):
    _, _, _, workspace, monitor = setup
    monkeypatch.setattr(private_operations, "MAX_ALERTS", 1)
    monitor.test_notification()
    monitor.notify(send=lambda *a: None)
    with sqlite3.connect(monitor.path) as conn:
        conn.execute("DROP TABLE alert_archives")
        conn.execute("DROP TABLE alert_archive_index")
    before = monitor.path.read_bytes()
    fresh = PrivateOperations(workspace.directory, clock=monitor.clock)
    fresh.status()
    fresh.audit_history()
    assert monitor.path.read_bytes() == before
    fresh.test_notification()
    assert fresh.status()["archived_alerts"] == 1


def test_more_than_ten_thousand_alerts_continue_checks_and_new_notifications(setup):
    clock, _, backend, workspace, monitor = setup
    # A trusted bulk fixture avoids quadratic public calls; normal verification checks all rows.
    body = _body(Alert(kind="private_notification_test", created_at=clock.wall))
    with monitor._store(write=True) as conn:
        state = monitor._verify(conn)[0]
        conn.executemany(
            "INSERT INTO alerts(id,body,digest,last_attempt_at,attempts,submitted_at) "
            "VALUES(?,?,?,?,1,?)",
            [
                (i, body, _hash(body), clock.wall.isoformat(), clock.wall.isoformat())
                for i in range(1, 10_001)
            ],
        )
        monitor._write(conn, state.model_copy(update={"alert_count": 10_000}))
        conn.commit()
    monitor.test_notification()
    assert monitor.status()["alert_count"] == 10_001
    assert monitor.status()["archived_alerts"] == 1000
    assert monitor.check()["conditions"] == []
    sent = []
    assert monitor.notify(send=lambda a, _: sent.append(a["id"]))["submitted"] == 1
    assert sent == [10_001]
    stop(workspace)
    assert monitor.check()["conditions"] == ["private_sync_stopped"]
    assert monitor.audit_history()["alerts"] == 10_002 and backend.reads == []


def test_history_pagination_includes_archived_and_acknowledged_rows_without_writes(
    setup, monkeypatch, capsys
):
    monitor = compact_history(setup, monkeypatch)
    monitor.acknowledge(1)
    before = monitor.path.read_bytes()
    page = monitor.history(limit=2)
    assert [r["id"] for r in page["alerts"]] == [1, 2]
    assert page["alerts"][0]["acknowledged_at"] is not None
    assert page["next_alert_id"] == 2
    assert [r["id"] for r in monitor.history(after_id=2)["alerts"]] == [3, 4]
    assert monitor.history(after_id=4)["alerts"] == []
    assert (
        private_operations.main(
            [
                "history",
                "--directory",
                str(monitor.directory),
                "--after-alert-id",
                "1",
                "--limit",
                "2",
            ]
        )
        == 0
    )
    assert [r["id"] for r in json.loads(capsys.readouterr().out)["alerts"]] == [2, 3]
    assert monitor.path.read_bytes() == before


@pytest.mark.parametrize(
    "args", [{"limit": 0}, {"limit": 101}, {"after_id": -1}, {"after_id": True}]
)
def test_history_rejects_invalid_page_requests(setup, args):
    monitor = setup[-1]
    with pytest.raises(OperationsError, match="history_page_invalid"):
        monitor.history(**args)


def test_normal_checks_skip_archived_json_semantics_but_full_audit_parses_every_body(
    setup, monkeypatch
):
    monitor = compact_history(setup, monkeypatch)
    checked = []
    original = monitor._parse_alert

    def parsed(raw, state):
        checked.append(raw)
        return original(raw, state)

    monkeypatch.setattr(monitor, "_parse_alert", parsed)
    monitor.status()
    assert len(checked) == 2
    checked.clear()
    monitor.audit_history()
    assert len(checked) == 4


@pytest.mark.parametrize("after_commit", [False, True])
def test_process_exit_at_archive_commit_keeps_complete_old_or_new_state(
    setup, monkeypatch, after_commit
):
    clock, _, _, workspace, monitor = setup
    monkeypatch.setattr(private_operations, "MAX_ALERTS", 1)
    monitor.test_notification()
    monitor.notify(send=lambda *a: None)
    code = """
import os, sys
from contextlib import contextmanager
from datetime import datetime
from trading import private_operations as module
module.MAX_ALERTS = 1
j = module.PrivateOperations(sys.argv[1], clock=lambda: datetime.fromisoformat(sys.argv[2]))
original = j._owned
@contextmanager
def interrupted():
    with original() as context:
        yield context
        if sys.argv[3] == 'before':
            os._exit(23)
    if sys.argv[3] == 'after':
        os._exit(23)
j._owned = interrupted
j.test_notification()
"""
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            code,
            str(workspace.directory),
            clock.wall.isoformat(),
            "after" if after_commit else "before",
        ],
        capture_output=True,
        timeout=20,
    )
    assert result.returncode == 23, result.stderr.decode()
    fresh = PrivateOperations(workspace.directory, clock=monitor.clock)
    saved = fresh.status()
    assert saved["alert_count"] == (2 if after_commit else 1)
    assert saved["archived_alerts"] == (1 if after_commit else 0)
    assert [r["id"] for r in fresh.history()["alerts"]] == list(range(1, saved["alert_count"] + 1))
    assert fresh.audit_history()["alerts"] == saved["alert_count"]


def test_full_audit_rejects_rehashed_archived_semantic_damage(setup, monkeypatch):
    monitor = compact_history(setup, monkeypatch)
    with sqlite3.connect(monitor.path) as conn:
        body, payload = conn.execute("SELECT body,payload FROM alert_archives").fetchone()
        payload = payload.replace(b"private_notification_test", b"private_notification_fail")
        descriptor = json.loads(body)
        descriptor["payload_sha256"] = hashlib.sha256(payload).hexdigest()
        body = private_operations._json(descriptor)
        conn.execute(
            "UPDATE alert_archives SET body=?,digest=?,payload=?", (body, _hash(body), payload)
        )
        state = json.loads(conn.execute("SELECT body FROM monitor").fetchone()[0])
        state["archive_head"] = _hash(body)
        body = private_operations._json(state)
        conn.execute("UPDATE monitor SET body=?,digest=?", (body, _hash(body)))
    # Hash-only inspection is not protection from hostile SQL edits and rehashed chains.
    assert monitor.status()["archived_alerts"] == 2
    with pytest.raises(OperationsError, match="integrity_failed"):
        monitor.audit_history()


def test_full_pending_outbox_does_not_prevent_watchdog_delivery_and_next_sample(setup, monkeypatch):
    _, _, _, workspace, monitor = setup
    monkeypatch.setattr(private_operations, "MAX_ALERTS", 3)
    for _ in range(3):
        monitor.test_notification()
    stop(workspace)
    sent = []
    result = monitor.watchdog(send=lambda a, _: sent.append(a["id"]))
    assert result["check_failed"] and result["reason"] == "operations_alert_capacity"
    assert result["last_check_at"] is None and result["conditions"] == []
    assert sent == [1, 2, 3]
    result = monitor.watchdog(send=lambda a, _: sent.append(a["id"]))
    assert "check_failed" not in result
    assert result["conditions"] == ["private_sync_stopped"] and sent == [1, 2, 3, 4]
    assert monitor.audit_history()["alerts"] == 4
