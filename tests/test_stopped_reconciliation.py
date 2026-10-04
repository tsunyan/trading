"""Explicit stopped GET composition, durable posting and separate recovery."""

import ctypes
import json
import socket
import subprocess
import sys
import threading
from contextlib import contextmanager
from decimal import Decimal

import httpx
import pytest
from test_account_events import execution, raw
from test_private_sync import make_setup

from trading import private_sync
from trading.broker_contracts import OrderIntent
from trading.event_journal import JournalError
from trading.known_orders import CatalogError, KnownOrder
from trading.private_sync import PrivateSyncError, PrivateSyncWorkspace, main
from trading.stream_control import StreamControl, StreamControlError


@pytest.fixture(autouse=True)
def no_native_or_network(monkeypatch):
    def forbidden(*args, **kwargs):
        pytest.fail("real credentials/network attempted")

    monkeypatch.setattr(ctypes, "WinDLL", forbidden, raising=False)
    monkeypatch.setattr(socket, "socket", forbidden)


def known_order():
    return KnownOrder(
        order_id=201,
        intent=OrderIntent(
            client_id="DemoOpen", side="BUY", effect="OPEN", units=1000, kind="LIMIT", price="150"
        ),
    )


def stopped(tmp_path, *, phase="STOPPED", known=True, duplicate=None, pending=False, end=False):
    setup = make_setup(tmp_path, known_orders=(known_order(),) if known else ())
    clock, _, _, _, _, workspace = setup
    row = execution(
        executionTimestamp=clock.wall.isoformat(), orderTimestamp=clock.wall.isoformat()
    )
    with workspace.control.ownership():
        state = workspace.control.begin(
            workspace.journal, expected_revision=0, expected_head=workspace.journal.head()
        )
        session = workspace.journal.start_session(
            expected_head=workspace.journal.head(), at=clock.wall, monotonic_ns=0
        )
        record = workspace.journal.record(
            session, "EVENT", at=clock.wall, monotonic_ns=0, sequence=1, payload=raw(row)
        )
        if not pending:
            workspace.journal.acknowledge(session, record)
        if duplicate is not None:
            clock.advance(1)
            record = workspace.journal.record(
                session,
                "EVENT",
                at=clock.wall,
                monotonic_ns=int(clock.mono * 1e9),
                sequence=2,
                payload=raw({**row, **duplicate}),
            )
            workspace.journal.acknowledge(session, record)
        if end:
            workspace.journal.record(session, "END", at=clock.wall, monotonic_ns=0)
        if phase == "STOPPED":
            workspace.control.finish(state["owner"], workspace.journal, reason="stream_failed")
    return (*setup, row, session)


def checks(workspace, **updates):
    state = workspace.control.snapshot()
    return {
        "expected_plan_sha256": workspace.plan_sha256,
        "expected_revision": state["revision"],
        "expected_head": workspace.journal.head(),
        "expected_reason": state["reason"],
        "read_only_confirmed": True,
        **updates,
    }


def transport(clock, row, requests, *, before_response=None, rest_changes=None):
    order = {
        "rootOrderId": 201,
        "orderId": 201,
        "clientOrderId": "DemoOpen",
        "symbol": "USD_JPY",
        "side": "BUY",
        "settleType": "OPEN",
        "orderType": "NORMAL",
        "executionType": "LIMIT",
        "size": "1000",
        "price": "150",
        "status": "ORDERED",
        "timestamp": row["orderTimestamp"],
    }
    fill = {
        k: row[k]
        for k in (
            "executionId",
            "positionId",
            "orderId",
            "clientOrderId",
            "symbol",
            "side",
            "settleType",
            "amount",
            "fee",
            "lossGain",
            "settledSwap",
        )
    }
    fill.update(
        size=row["executionSize"], price=row["executionPrice"], timestamp=row["executionTimestamp"]
    )
    fill.update(rest_changes or {})

    def get(request):
        requests.append(request.url.path)
        assert request.method == "GET" and request.url.params["orderId"] == "201"
        assert request.url.path in {"/private/v1/orders", "/private/v1/executions"}
        if before_response is not None:
            before_response(len(requests))
        data = {"list": [order if request.url.path.endswith("/orders") else fill]}
        return httpx.Response(
            200, json={"status": 0, "data": data, "responsetime": clock.wall.isoformat()}
        )

    return httpx.MockTransport(get)


@pytest.mark.parametrize("phase,end", [("STOPPED", False), ("STOPPED", True), ("RUNNING", False)])
def test_real_gets_book_once_leave_stop_and_require_separate_recovery(tmp_path, phase, end):
    clock, read_clocks, _, _, vault, workspace, row, _ = stopped(tmp_path, phase=phase, end=end)
    before, head = workspace.control.snapshot(), workspace.journal.head()
    requests = []
    args = dict(
        vault=vault, read_clocks=read_clocks, read_transport=transport(clock, row, requests)
    )
    first = workspace.reconcile_stopped(**checks(workspace), **args)
    assert first["booking"]["applied_execution_ids"] == (501,)
    assert first["control"] == before and workspace.journal.head() == head
    assert first["recovery_required"] and not first["complete"] and not first["live_enabled"]
    assert Decimal(first["cash"]["balance"]) == Decimal("999998")
    second = workspace.reconcile_stopped(**checks(workspace), **args)
    assert second["booking"]["already_applied_execution_ids"] == (501,)
    assert Decimal(second["booking"]["cash_delta"]) == 0 and second["cash"]["executions"] == 1
    assert requests == ["/private/v1/orders", "/private/v1/executions"] * 4
    result = workspace.recover(
        **{k: v for k, v in checks(workspace).items() if k != "read_only_confirmed"},
        acknowledge_token_uncertainty=True,
    )
    assert result["control"]["phase"] == "READY" and result["journal"]["records"] == 0
    assert result["journal"]["resync_required"]


def test_unknown_order_refuses_before_credentials_then_operator_registration_allows_proof(tmp_path):
    clock, read_clocks, _, backend, vault, workspace, row, _ = stopped(tmp_path, known=False)
    with pytest.raises(CatalogError, match="order_unknown"):
        workspace.reconcile_stopped(**checks(workspace), vault=vault)
    assert backend.reads == [] and workspace.book.snapshot()["executions"] == 0
    workspace.register_order(
        known_order(),
        expected_plan_sha256=workspace.plan_sha256,
        expected_catalog_head=workspace.catalog.snapshot()["head"],
        source_ref="reviewed",
        intent_confirmed=True,
    )
    result = workspace.reconcile_stopped(
        **checks(workspace),
        vault=vault,
        read_clocks=read_clocks,
        read_transport=transport(clock, row, []),
    )
    assert result["cash"]["executions"] == 1 and result["control"]["phase"] == "STOPPED"


@pytest.mark.parametrize(
    "field,value",
    [
        ("expected_plan_sha256", "f" * 64),
        ("expected_revision", 99),
        ("expected_revision", True),
        ("expected_head", "f" * 64),
        ("expected_reason", "other"),
        ("read_only_confirmed", False),
    ],
)
def test_changed_checkpoint_or_permission_refuses_before_credentials(tmp_path, field, value):
    _, _, _, backend, vault, workspace, _, _ = stopped(tmp_path)
    before = workspace.control.snapshot()
    with pytest.raises(ValueError):
        workspace.reconcile_stopped(**checks(workspace, **{field: value}), vault=vault)
    assert backend.reads == [] and workspace.control.snapshot() == before
    assert workspace.book.snapshot()["executions"] == 0


def test_ready_and_live_owner_refuse_before_credentials(tmp_path):
    _, _, _, backend, vault, workspace = make_setup(tmp_path)
    with pytest.raises(PrivateSyncError, match="stop_required"):
        workspace.reconcile_stopped(**checks(workspace), vault=vault)
    peer = PrivateSyncWorkspace(workspace.directory)
    with workspace.control.ownership(), pytest.raises(StreamControlError, match="owner_busy"):
        peer.reconcile_stopped(**checks(peer), vault=vault)
    assert backend.reads == []


def test_pending_delivery_and_rejected_segment_are_not_cleared(tmp_path):
    _, _, _, backend, vault, workspace, _, session = stopped(tmp_path, pending=True)
    with pytest.raises(JournalError, match="delivery_unresolved"):
        workspace.reconcile_stopped(**checks(workspace), vault=vault)
    workspace.journal.acknowledge(session, 2)
    with pytest.raises(JournalError, match="sequence_gap"):
        workspace.journal.record(
            session, "EVENT", at=workspace.clock(), monotonic_ns=0, sequence=9, payload=b"{}"
        )
    with pytest.raises(JournalError, match="fault_requires_review"):
        workspace.reconcile_stopped(**checks(workspace), vault=vault)
    assert backend.reads == [] and workspace.control.snapshot()["phase"] == "STOPPED"


@pytest.mark.parametrize("duplicate", [{}, {"fee": "-3", "amount": "-3"}])
def test_every_duplicate_variant_is_validated_even_with_a_new_receipt_time(tmp_path, duplicate):
    clock, read_clocks, _, _, vault, workspace, row, _ = stopped(tmp_path, duplicate=duplicate)
    args = dict(vault=vault, read_clocks=read_clocks, read_transport=transport(clock, row, []))
    if duplicate:
        with pytest.raises(PrivateSyncError, match="reconciliation_mismatch"):
            workspace.reconcile_stopped(**checks(workspace), **args)
        assert workspace.book.snapshot()["executions"] == 0
    else:
        result = workspace.reconcile_stopped(**checks(workspace), **args)
        assert result["reconciled_notice_count"] == 2 and result["cash"]["executions"] == 1
    assert workspace.control.snapshot()["phase"] == "STOPPED"


def test_foreign_journal_write_during_get_is_refused_before_cash_posting(tmp_path):
    clock, read_clocks, _, _, vault, workspace, row, session = stopped(tmp_path)

    def change(count):
        if count == 4:
            record = workspace.journal.record(session, "HEARTBEAT", at=clock.wall, monotonic_ns=0)
            workspace.journal.acknowledge(session, record)

    with pytest.raises(JournalError, match="head_changed"):
        workspace.reconcile_stopped(
            **checks(workspace),
            vault=vault,
            read_clocks=read_clocks,
            read_transport=transport(clock, row, [], before_response=change),
        )
    assert workspace.book.snapshot()["executions"] == 0


def test_deadline_authentication_and_rest_mismatch_preserve_stop(tmp_path):
    clock, read_clocks, reads, _, vault, workspace, row, _ = stopped(tmp_path)
    before = workspace.control.snapshot()

    def late(count):
        if count == 4:
            clock.advance(workspace.plan.collection_limit_seconds)

    with pytest.raises(ValueError):
        workspace.reconcile_stopped(
            **checks(workspace),
            vault=vault,
            read_clocks=read_clocks,
            read_transport=transport(clock, row, [], before_response=late),
        )
    with pytest.raises(PrivateSyncError, match="reconciliation_mismatch"):
        workspace.reconcile_stopped(
            **checks(workspace),
            vault=vault,
            read_clocks=read_clocks,
            read_transport=transport(clock, row, [], rest_changes={"price": "149"}),
        )
    with pytest.raises(ValueError):
        workspace.reconcile_stopped(
            **checks(workspace),
            vault=vault,
            read_clocks=read_clocks,
            read_transport=httpx.MockTransport(lambda _: httpx.Response(401)),
        )
    assert reads.status()["blocked"] and workspace.control.snapshot() == before
    assert workspace.book.snapshot()["executions"] == 0


def test_unresponsive_get_keeps_os_ownership_until_it_finishes(tmp_path):
    clock, read_clocks, _, _, vault, workspace, row, _ = stopped(tmp_path)
    entered, release = threading.Event(), threading.Event()
    results = []

    def hang(count):
        if count == 1:
            entered.set()
            assert release.wait(5)

    def run():
        results.append(
            workspace.reconcile_stopped(
                **checks(workspace),
                vault=vault,
                read_clocks=read_clocks,
                read_transport=transport(clock, row, [], before_response=hang),
            )
        )

    thread = threading.Thread(target=run)
    thread.start()
    try:
        assert entered.wait(3)
        peer = StreamControl(workspace.control.path.parent)
        with pytest.raises(StreamControlError, match="owner_busy"), peer.ownership():
            pass
    finally:
        release.set()
        thread.join(5)
    assert not thread.is_alive() and results[0]["cash"]["executions"] == 1


def test_interrupt_after_cash_commit_keeps_receipt_and_retry_does_not_duplicate(
    tmp_path, monkeypatch
):
    clock, read_clocks, _, _, vault, workspace, row, _ = stopped(tmp_path)
    original = workspace.journal.guard_recovery_events

    @contextmanager
    def interrupted(**kwargs):
        with original(**kwargs) as events:
            yield events
            raise RuntimeError("synthetic interruption after cash commit")

    args = dict(vault=vault, read_clocks=read_clocks, read_transport=transport(clock, row, []))
    before = workspace.control.snapshot()
    with monkeypatch.context() as patch:
        patch.setattr(workspace.journal, "guard_recovery_events", interrupted)
        with pytest.raises(RuntimeError):
            workspace.reconcile_stopped(**checks(workspace), **args)
    reopened = PrivateSyncWorkspace(workspace.directory)
    assert reopened.book.snapshot()["executions"] == 1 and reopened.control.snapshot() == before
    result = workspace.reconcile_stopped(**checks(workspace), **args)
    assert result["booking"]["already_applied_execution_ids"] == (501,)


def test_cli_passes_checkpoint_and_read_permission(tmp_path, monkeypatch, capsys):
    _, _, _, _, _, workspace, _, _ = stopped(tmp_path)
    called = []

    def reconcile(**kwargs):
        called.append(kwargs)
        return {"complete": False, "live_enabled": False, "recovery_required": True}

    monkeypatch.setattr(private_sync, "PrivateSyncWorkspace", lambda _: workspace)
    monkeypatch.setattr(workspace, "reconcile_stopped", reconcile)
    args = checks(workspace)
    main(
        [
            "reconcile-stopped",
            "--directory",
            str(workspace.directory),
            "--expected-plan-sha256",
            args["expected_plan_sha256"],
            "--expected-revision",
            str(args["expected_revision"]),
            "--expected-head",
            args["expected_head"],
            "--expected-reason",
            args["expected_reason"],
            "--read-only-confirmed",
        ]
    )
    assert called == [args] and json.loads(capsys.readouterr().out)["recovery_required"]


def test_empty_stopped_segment_does_not_load_credentials(tmp_path):
    _, _, _, backend, vault, workspace = make_setup(tmp_path)
    with workspace.control.ownership():
        state = workspace.control.begin(
            workspace.journal, expected_revision=0, expected_head=workspace.journal.head()
        )
        workspace.control.finish(state["owner"], workspace.journal, reason="stream_failed")
    result = workspace.reconcile_stopped(**checks(workspace), vault=vault)
    assert result["booking"] is None and result["reconciled_notice_count"] == 0
    assert result["control"]["phase"] == "STOPPED" and backend.reads == []


def test_stopped_get_dependency_refuses_before_credentials(tmp_path):
    _, _, reads, backend, vault, workspace, _, _ = stopped(tmp_path)
    reads.stop()
    with pytest.raises(PrivateSyncError, match="dependencies_blocked"):
        workspace.reconcile_stopped(**checks(workspace), vault=vault)
    assert backend.reads == [] and workspace.book.snapshot()["executions"] == 0


@pytest.mark.parametrize("stage,code", [("before", 71), ("after", 72)])
def test_real_process_exit_at_cash_commit_keeps_stop_and_retry_posts_once(tmp_path, stage, code):
    clock, read_clocks, _, _, vault, workspace, row, _ = stopped(tmp_path)
    before, head = workspace.control.snapshot(), workspace.journal.head()
    script = r"""
import ctypes,json,os,socket,sys
from contextlib import contextmanager
from types import SimpleNamespace
from pydantic import SecretStr
sys.path.insert(0,"tests")
from test_stopped_reconciliation import checks,transport
from test_private_stream import Clock
from trading.private_sync import PrivateSyncWorkspace
from trading.execution_cash_book import ExecutionCashBook
def forbidden(*a,**k): raise AssertionError("real network/native credentials forbidden")
socket.socket=forbidden
ctypes.WinDLL=forbidden
clock=Clock()
w=PrivateSyncWorkspace(sys.argv[1],clock=lambda:clock.wall,monotonic=lambda:clock.mono)
class Vault:
    def load(self,*a):
        return SimpleNamespace(
            api_key=SecretStr("synthetic-sync-key"),secret=SecretStr("synthetic-sync-secret"))
original=ExecutionCashBook._transaction
@contextmanager
def crash(self,**kwargs):
    with original(self,**kwargs) as conn:
        yield conn
        if kwargs.get("write") and sys.argv[2]=="before": os._exit(71)
    if kwargs.get("write") and sys.argv[2]=="after": os._exit(72)
ExecutionCashBook._transaction=crash
w.reconcile_stopped(**checks(w),vault=Vault(),read_clocks={"wall_ns":lambda:int(clock.wall.timestamp()*1e9),"monotonic_ns":lambda:int(clock.mono*1e9),"sleep":clock.advance},read_transport=transport(clock,json.loads(sys.argv[3]),[]))
"""
    process = subprocess.run(
        [sys.executable, "-c", script, str(workspace.directory), stage, json.dumps(row)],
        capture_output=True,
        timeout=15,
    )
    assert process.returncode == code, process.stderr.decode()
    reopened = PrivateSyncWorkspace(workspace.directory)
    assert reopened.control.snapshot() == before and reopened.journal.head() == head
    assert reopened.book.snapshot()["executions"] == (0 if stage == "before" else 1)
    clock.advance(2)  # The child advanced the shared GET limiter's durable clock.
    result = workspace.reconcile_stopped(
        **checks(workspace),
        vault=vault,
        read_clocks=read_clocks,
        read_transport=transport(clock, row, []),
    )
    assert result["cash"]["executions"] == 1 and Decimal(result["cash"]["balance"]) == Decimal(
        "999998"
    )
