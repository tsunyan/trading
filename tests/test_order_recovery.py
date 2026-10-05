"""GET-only unknown-order investigation using real bound clients and mocked HTTP."""

import copy
import json
import socket
import sqlite3
import subprocess
import sys
import threading
from datetime import timedelta
from types import SimpleNamespace

import httpx
import pytest
from pydantic import SecretStr
from test_account_guard import NOW, quote
from test_private_order import client, ready
from test_private_order import setup as live_setup

from trading.account_reader import CollectionError
from trading.live_journal import LiveOrderError, LiveOrderJournal
from trading.post_control import PersistentPostLimiter, PostControlError
from trading.private_order import OrderTransportError
from trading.private_order_recovery import OrderRecoveryError, PrivateOrderRecovery, main


@pytest.fixture(autouse=True)
def no_network(monkeypatch):
    monkeypatch.setattr(socket, "socket", lambda *a, **k: pytest.fail("real network attempted"))


@pytest.fixture
def setup(tmp_path):
    return live_setup.__wrapped__(tmp_path)


class Vault:
    def __init__(self):
        self.calls = []

    def load(self, reads, reference):
        self.calls.append((reads.scope, reference))
        return SimpleNamespace(api_key=SecretStr("fixture-key"), secret=SecretStr("fixture-secret"))


def unknown(setup):
    clock, reads, _, _ = setup
    order = ready(setup)
    with client(setup, lambda _: httpx.Response(500)) as sender:
        with pytest.raises(OrderTransportError):
            sender.submit(order.client_id, quote=quote(clock.now))
    recovery = PrivateOrderRecovery(
        setup[3].path.parent,
        reads.path.parent,
        reads.scope,
        clock=lambda: clock.now,
        monotonic=lambda: clock.mono,
        read_clocks={
            "wall_ns": lambda: int(clock.now.timestamp() * 1e9),
            "monotonic_ns": lambda: int(clock.mono * 1e9),
            "sleep": clock.advance,
        },
    )
    return recovery, order, Vault()


def checks(recovery, order):
    return {
        "expected_sha256": recovery.context(order.client_id)["checkpoint_sha256"],
        "credential_reference": "a" * 32,
        "read_only_confirmed": True,
    }


def transport(clock, order, calls, *, status="ORDERED", units=0, mutate=None):
    def handler(request):
        calls.append(request)
        assert request.method == "GET" and not request.content
        assert str(request.url).startswith("https://forex-api.coin.z.com/private/v1/")
        assert request.url.params == httpx.QueryParams({"orderId": "201"})
        row = {
            "rootOrderId": 101,
            "orderId": 201,
            "clientOrderId": order.client_id,
            "symbol": "USD_JPY",
            "side": order.side,
            "settleType": order.effect,
            "orderType": "NORMAL",
            "executionType": order.kind,
            "size": str(order.units),
            "price": str(order.price),
            "status": status,
            "timestamp": clock.now.isoformat(),
        }
        fills = (
            []
            if not units
            else [
                {
                    "executionId": 301,
                    "positionId": 401,
                    "orderId": 201,
                    "clientOrderId": order.client_id,
                    "symbol": "USD_JPY",
                    "side": order.side,
                    "settleType": order.effect,
                    "size": str(units),
                    "price": str(order.price),
                    "amount": "-2",
                    "fee": "-2",
                    "lossGain": "0",
                    "settledSwap": "0",
                    "timestamp": clock.now.isoformat(),
                }
            ]
        )
        # The two sweeps compare raw order/fill times, not receipt times.
        row["timestamp"] = (NOW + timedelta(seconds=1)).isoformat()
        for fill in fills:
            fill["timestamp"] = row["timestamp"]
        data = {"list": [row] if request.url.path.endswith("/orders") else fills}
        result = {"status": 0, "data": copy.deepcopy(data), "responsetime": clock.now.isoformat()}
        if mutate:
            mutate(request, result, len(calls))
        return httpx.Response(200, json=result)

    return httpx.MockTransport(handler)


def test_local_context_never_reads_credentials_or_changes_stores(setup):
    recovery, order, vault = unknown(setup)
    paths = (recovery.journal.path, recovery.posts.path, recovery.reads.path)
    before = [p.read_bytes() for p in paths]
    context = recovery.context(order.client_id)
    assert len(context["checkpoint_sha256"]) == 64 and context["post_claim"]
    assert not context["complete"] and not context["live_enabled"]
    assert vault.calls == [] and [p.read_bytes() for p in paths] == before


# Working orders take the same path as these terminal ones; EXPIRED-0 stays because an
# unfilled terminal order is the observation most easily mistaken for absence.
@pytest.mark.parametrize("status,units", [("EXECUTED", 1000), ("CANCELED", 400), ("EXPIRED", 0)])
def test_positive_order_observation_preserves_claim_stop_and_incompleteness(setup, status, units):
    recovery, order, vault = unknown(setup)
    clock, _, posts, journal = setup
    before = posts.snapshot()
    calls = []
    result = recovery.reconcile(
        order.client_id,
        201,
        **checks(recovery, order),
        vault=vault,
        transport=transport(clock, order, calls, status=status, units=units),
    )
    assert result["state"] == "RECONCILING" and result["broker_status"] == status
    assert not result["executions_complete"] and result["post_claim_retained"]
    assert not result["live_enabled"] and not result["complete"] and result["recovery_required"]
    assert len(calls) == 4 and len(vault.calls) == 1 and posts.snapshot() == before
    view = journal.snapshot()
    assert view["halted"] and view["live_control"]["phase"] == "STOPPED"
    assert view["orders"][0]["evidence"]["executions_complete"] is False
    record = view["events"][-1]
    assert record["kind"] == "ORDER_GET_OBSERVED" and len(record["payload"]["observations"]) == 4
    assert all(len(o["sha256"]) == 64 for o in record["payload"]["observations"])
    with pytest.raises(PostControlError), posts.operation("order", request_sha256="b" * 64):
        pass
    with client(setup, lambda _: pytest.fail("order resent")) as sender:
        with pytest.raises(OrderTransportError):
            sender.submit(order.client_id, quote=quote(clock.now))


@pytest.mark.parametrize(
    "failure",
    ["missing", "client", "side", "changing", "fills", "stale", "api"],
)
def test_absence_and_bad_gets_never_prove_rejection_or_release_claim(setup, failure):
    recovery, order, vault = unknown(setup)
    clock, _, posts, journal = setup
    before, saved = journal.snapshot(), posts.snapshot()

    def mutate(request, data, count):
        if failure == "api":
            data.update(status=1, messages=[{"message": "remote secret"}])
        elif failure == "stale":
            data["responsetime"] = "2020-01-01T00:00:00Z"
        elif request.url.path.endswith("/orders"):
            rows = data["data"]["list"]
            if failure == "missing":
                rows.clear()
            elif failure == "client":
                rows[0]["clientOrderId"] = "Other"
            elif failure == "side":
                rows[0]["side"] = "SELL"
            elif failure == "changing" and count == 3:
                rows[0]["status"] = "CANCELED"
            elif failure == "fills":
                rows[0]["status"] = "EXECUTED"

    selected = transport(clock, order, [], mutate=mutate)
    with pytest.raises(ValueError) as caught:
        recovery.reconcile(
            order.client_id, 201, **checks(recovery, order), vault=vault, transport=selected
        )
    assert "remote secret" not in str(caught.value)
    assert journal.snapshot() == before and posts.snapshot() == saved


def test_preflight_refusals_precede_credential_access(setup, subtests):
    recovery, order, vault = unknown(setup)
    stores = (recovery.journal.path, recovery.posts.path)
    before = [path.read_bytes() for path in stores]
    # Each refusal leaves the stores unchanged; the read stop runs last because it persists.
    for failure in ["confirmation", "checkpoint", "order_id", "reference", "read_stop"]:
        with subtests.test(failure=failure):
            args = checks(recovery, order)
            order_id = 201
            if failure == "confirmation":
                args["read_only_confirmed"] = False
            elif failure == "checkpoint":
                args["expected_sha256"] = "b" * 64
            elif failure == "order_id":
                order_id = True
            elif failure == "reference":
                args["credential_reference"] = "bad"
            else:
                recovery.reads.stop()
            with pytest.raises((OrderRecoveryError, LiveOrderError)):
                recovery.reconcile(order.client_id, order_id, **args, vault=vault)
            assert vault.calls == [] and [path.read_bytes() for path in stores] == before


def test_changed_post_checkpoint_and_foreign_claim_refuse_before_credentials(setup):
    recovery, order, vault = unknown(setup)
    args = checks(recovery, order)
    recovery.posts.stop("operator_stop")
    with pytest.raises(LiveOrderError, match="checkpoint_changed"):
        recovery.reconcile(order.client_id, 201, **args, vault=vault)
    with sqlite3.connect(recovery.posts.path) as conn:
        state = recovery.posts._state(conn)
        recovery.posts._write(conn, state, "TEST_CHANGED", request_sha256="b" * 64)
    with pytest.raises(LiveOrderError, match="claim_mismatch"):
        recovery.context(order.client_id)
    assert vault.calls == []


def test_emergency_stop_during_get_invalidates_checkpoint_without_clearing_it(setup):
    recovery, order, vault = unknown(setup)
    clock, _, posts, journal = setup

    def stop(request, data, count):
        if count == 4:
            posts.stop("operator_stop")

    with pytest.raises(LiveOrderError, match="checkpoint_changed"):
        recovery.reconcile(
            order.client_id,
            201,
            **checks(recovery, order),
            vault=vault,
            transport=transport(clock, order, [], mutate=stop),
        )
    assert journal.snapshot()["orders"][0]["state"] == "UNKNOWN"
    assert posts.snapshot()["reason"] == "operator_stop" and posts.snapshot()["claim"]


def test_live_owner_refuses_before_credentials_and_recovery_keeps_lock_during_get(setup):
    recovery, order, vault = unknown(setup)
    clock, reads, posts, _ = setup
    args = checks(recovery, order)
    with posts._ownership(), pytest.raises(PostControlError, match="owner_busy"):
        recovery.reconcile(order.client_id, 201, **args, vault=vault)
    assert vault.calls == []
    entered, release = threading.Event(), threading.Event()
    results = []

    def hold(request, data, count):
        if count == 1:
            entered.set()
            assert release.wait(5)

    def run():
        results.append(
            recovery.reconcile(
                order.client_id,
                201,
                **args,
                vault=vault,
                transport=transport(clock, order, [], mutate=hold),
            )
        )

    thread = threading.Thread(target=run)
    thread.start()
    try:
        assert entered.wait(3)
        peer = PersistentPostLimiter(posts.path.parent, reads, **clock.post_args())
        with pytest.raises(PostControlError, match="owner_busy"), peer._ownership():
            pass
    finally:
        release.set()
        thread.join(5)
    assert not thread.is_alive() and results[0]["post_claim_retained"]


def test_successful_recollection_after_restart_preserves_evidence_and_entry_halt(setup):
    recovery, order, vault = unknown(setup)
    clock, _, posts, journal = setup
    with journal._transaction() as conn:
        conn.execute("UPDATE account_gate SET entry_halted=1")
    for _ in range(2):
        recovery.reconcile(
            order.client_id,
            201,
            **checks(recovery, order),
            vault=vault,
            transport=transport(clock, order, [], units=400),
        )
        journal = LiveOrderJournal(journal.path.parent, posts, clock=lambda: clock.now)
    view = journal.snapshot()
    assert view["account_guard"]["entry_halted"] and not view["live_enabled"]
    assert len(view["orders"][0]["evidence"]["executions"]) == 1
    assert len([e for e in view["events"] if e["kind"] == "ORDER_GET_OBSERVED"]) == 2


def test_close_failure_and_collection_deadline_do_not_commit_observation(setup, monkeypatch):
    recovery, order, vault = unknown(setup)
    clock, _, posts, journal = setup
    before = journal.snapshot()

    def slow(request, data, count):
        clock.advance(9)
        data["responsetime"] = clock.now.isoformat()

    with pytest.raises(CollectionError):
        recovery.reconcile(
            order.client_id,
            201,
            **checks(recovery, order),
            vault=vault,
            transport=transport(clock, order, [], mutate=slow),
        )
    assert journal.snapshot() == before and posts.snapshot()["claim"]
    monkeypatch.setattr(
        httpx.Client, "close", lambda _: (_ for _ in ()).throw(RuntimeError("secret"))
    )
    with pytest.raises(ValueError):
        recovery.reconcile(
            order.client_id,
            201,
            **checks(recovery, order),
            vault=vault,
            transport=transport(clock, order, []),
        )
    assert journal.snapshot() == before and recovery.reads.status()["stopped"]


def test_inflight_dead_sender_is_stopped_atomically_with_observation(setup):
    recovery, order, vault = unknown(setup)
    clock, _, posts, journal = setup
    # Emulate the durable states left by exit immediately after SUBMITTING.
    with sqlite3.connect(posts.path) as conn:
        state = posts._state(conn)
        posts._write(conn, state, "TEST_INFLIGHT", phase="IN_FLIGHT", reason="completed")
    with journal._transaction() as conn:
        conn.execute("UPDATE metadata SET halted=0")
        journal._write_live(conn, journal._live_state(conn), phase="ENABLED")
        conn.execute("UPDATE orders SET state='SUBMITTING'")
    saved = posts.snapshot()
    recovery.reconcile(
        order.client_id,
        201,
        **checks(recovery, order),
        vault=vault,
        transport=transport(clock, order, []),
    )
    assert posts.snapshot() == saved
    assert journal.snapshot()["halted"] and journal.snapshot()["live_control"]["phase"] == "STOPPED"


@pytest.mark.parametrize("stage,code", [("before", 71), ("after", 72)])
def test_process_exit_at_observation_commit_keeps_claim_and_all_or_no_evidence(setup, stage, code):
    recovery, order, _ = unknown(setup)
    before, post = recovery.journal.snapshot(), recovery.posts.snapshot()
    script = r"""
import ctypes,os,socket,sys
from contextlib import contextmanager
sys.path.insert(0,"tests")
from test_order_recovery import Vault,checks,transport
from test_private_order import Clock
from trading.live_journal import LiveOrderJournal
from trading.order_journal import OrderJournal
from trading.private_order_recovery import PrivateOrderRecovery
def forbidden(*a,**k): raise AssertionError("real network/native credentials forbidden")
socket.socket=forbidden
ctypes.WinDLL=forbidden
c=Clock(); c.advance(1.1)
r=PrivateOrderRecovery(sys.argv[1],sys.argv[2],"synthetic",clock=lambda:c.now,monotonic=lambda:c.mono,read_clocks={"wall_ns":lambda:int(c.now.timestamp()*1e9),"monotonic_ns":lambda:int(c.mono*1e9),"sleep":c.advance})
original=OrderJournal._transaction
@contextmanager
def crash(self):
    with original(self) as conn:
        yield conn
        found=conn.execute("SELECT 1 FROM events WHERE kind='ORDER_GET_OBSERVED'").fetchone()
        if found and sys.argv[3]=="before": os._exit(71)
    if found and sys.argv[3]=="after": os._exit(72)
OrderJournal._transaction=crash
order=r.journal.snapshot()["orders"][0]["intent"]
from trading.broker_contracts import OrderIntent
order=OrderIntent.model_validate(order)
r.reconcile(order.client_id,201,**checks(r,order),vault=Vault(),transport=transport(c,order,[],units=400))
"""
    process = subprocess.run(
        [
            sys.executable,
            "-c",
            script,
            str(recovery.journal.path.parent),
            str(recovery.reads.path.parent),
            stage,
        ],
        capture_output=True,
        timeout=15,
    )
    assert process.returncode == code, process.stderr.decode()
    assert recovery.posts.snapshot() == post
    view = recovery.journal.snapshot()
    if stage == "before":
        assert view == before
    else:
        assert view["orders"][0]["state"] == "RECONCILING"
        assert view["events"][-1]["kind"] == "ORDER_GET_OBSERVED"
    setup[0].advance(3)  # Child GETs advanced the durable read clock.
    result = recovery.reconcile(
        order.client_id,
        201,
        **checks(recovery, order),
        vault=Vault(),
        transport=transport(setup[0], order, [], units=400),
    )
    assert result["post_claim_retained"] and recovery.posts.snapshot() == post


def test_missing_consumed_history_or_unsubmitted_order_cannot_be_investigated(setup):
    recovery, order, _ = unknown(setup)
    with recovery.journal._transaction() as conn:
        conn.execute("DELETE FROM events WHERE kind='SUBMITTING'")
    with pytest.raises(LiveOrderError, match="claim_mismatch"):
        recovery.context(order.client_id)


def test_code_update_allows_get_investigation_without_renewing_approval(setup, monkeypatch):
    recovery, order, vault = unknown(setup)
    clock, _, posts, journal = setup
    before = journal.snapshot()["live_control"]
    monkeypatch.setattr("trading.live_journal.implementation_sha256", lambda: "b" * 64)
    result = recovery.reconcile(
        order.client_id,
        201,
        **checks(recovery, order),
        vault=vault,
        transport=transport(clock, order, []),
    )
    assert result["post_claim_retained"] and posts.snapshot()["claim"]
    view = journal.snapshot()
    assert not view["implementation_matches"] and not view["live_enabled"]
    assert view["live_control"] == before


def test_saved_partial_fills_cannot_disappear_from_a_new_observation(setup):
    recovery, order, vault = unknown(setup)
    clock, _, posts, journal = setup
    recovery.reconcile(
        order.client_id,
        201,
        **checks(recovery, order),
        vault=vault,
        transport=transport(clock, order, [], units=400),
    )
    before, saved = journal.snapshot()["orders"], posts.snapshot()
    with pytest.raises(ValueError, match="executions disappeared"):
        recovery.reconcile(
            order.client_id,
            201,
            **checks(recovery, order),
            vault=vault,
            transport=transport(clock, order, []),
        )
    assert journal.snapshot()["orders"] == before and journal.snapshot()["halted"]
    assert posts.snapshot() == saved


def test_cli_context_and_reconcile_dispatch_never_print_credentials(setup, monkeypatch, capsys):
    recovery, order, _ = unknown(setup)
    monkeypatch.setattr("trading.private_order_recovery.PrivateOrderRecovery", lambda *a: recovery)
    shared = [
        "--directory",
        str(recovery.journal.path.parent),
        "--read-control-directory",
        str(recovery.reads.path.parent),
        "--scope",
        "synthetic",
        "--client-id",
        order.client_id,
    ]
    main(["context", *shared])
    context = json.loads(capsys.readouterr().out)
    assert context == recovery.context(order.client_id)
    received = []
    monkeypatch.setattr(
        recovery, "reconcile", lambda *a, **k: received.append((a, k)) or {"complete": False}
    )
    main(
        [
            "reconcile",
            *shared,
            "--order-id",
            "201",
            "--expected-sha256",
            context["checkpoint_sha256"],
            "--credential-reference",
            "a" * 32,
            "--read-only-confirmed",
        ]
    )
    assert received == [((order.client_id, 201), checks(recovery, order))]
    assert json.loads(capsys.readouterr().out) == {"complete": False}
