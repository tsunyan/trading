"""Real bound runtime/catalog composition with synthetic broker and credential boundaries."""

import ctypes
import hashlib
import json
import socket
import sqlite3
import subprocess
import sys
import threading
from datetime import timedelta
from decimal import Decimal

import httpx
import pytest
from test_account_events import execution, raw
from test_account_guard import intent, policy, quote
from test_private_order import client, ready, response
from test_private_stream import response as token_response
from test_private_supervisor import Socket
from test_private_sync import account_response, make_setup, options

from trading.account_reader import OrderReadReport
from trading.broker_contracts import Execution, OrderEvidence, OrderLimits
from trading.execution_positions import PositionBasis
from trading.known_orders import KnownOrder
from trading.live_journal import LiveOrderJournal
from trading.live_order_catalog import LiveCatalogError, LiveOrderCatalogSource
from trading.private_order import OrderTransportError
from trading.private_supervisor import PrivateStreamSupervisor, SupervisorError
from trading.private_sync import PrivateSyncWorkspace, _ReaderOwner, main


@pytest.fixture(autouse=True)
def no_native_or_network(monkeypatch):
    def forbidden(*args, **kwargs):
        pytest.fail("real credentials/network attempted")

    monkeypatch.setattr(ctypes, "WinDLL", forbidden, raising=False)
    monkeypatch.setattr(socket, "socket", forbidden)


class LiveClock:
    def __init__(self, clock):
        self.clock = clock

    @property
    def now(self):
        return self.clock.wall

    @property
    def mono(self):
        return self.clock.mono

    def advance(self, seconds):
        self.clock.advance(seconds)


@pytest.fixture
def setup(tmp_path):
    from trading.post_control import PersistentPostLimiter

    values = make_setup(tmp_path, position_basis=PositionBasis(positions=()))
    clock, _, reads, _, _, workspace = values
    adapter = LiveClock(clock)
    posts = PersistentPostLimiter.create(
        tmp_path / "posts",
        reads,
        wall_ns=lambda: int(clock.wall.timestamp() * 1e9),
        monotonic=lambda: clock.mono,
        sleep=clock.advance,
    )
    journal = LiveOrderJournal.create(
        tmp_path / "live",
        posts,
        OrderLimits(
            min_units=100,
            max_units=1000,
            unit_step=100,
            price_tick="0.001",
            max_reference_notional="200000",
        ),
        policy(),
        clock=lambda: clock.wall,
    )
    live = (adapter, reads, posts, journal)
    return values, live


def source(setup):
    values, live = setup
    return LiveOrderCatalogSource(live[2], values[5].catalog, clock=lambda: values[0].wall)


def accepted(setup):
    _, live = setup
    order = ready(live)
    with client(live, lambda request: response(live[0], request)) as sender:
        sender.submit(order.client_id, quote=quote(live[0].now))
    return order


def evidence(setup, order, *, complete=False):
    return OrderEvidence(
        intent=order,
        root_order_id=101,
        order_id=201,
        status="ORDERED",
        observed_at=setup[0][0].wall,
        executions=(),
        executions_complete=complete,
    )


def test_receipt_registers_exact_intent_idempotently_without_changing_stores_or_permissions(setup):
    values, live = setup
    _, _, _, backend, _, workspace = values
    order = accepted(setup)
    before, posts = live[3].snapshot(), live[2].snapshot()
    manifest = (workspace.directory / "sync-plan.json").read_bytes()
    result = source(setup).refresh()
    assert result["registered_count"] == result["identified_count"] == 1
    assert result["unidentified_client_ids"] == []
    assert not result["complete"] and not result["live_enabled"]
    assert workspace.catalog.lookup((201,)) == {201: order}
    assert source(setup).refresh()["registered_count"] == 0
    assert live[3].snapshot() == before and live[2].snapshot() == posts
    assert (workspace.directory / "sync-plan.json").read_bytes() == manifest
    assert workspace.plan.known_orders == () and backend.reads == []
    with sqlite3.connect(workspace.catalog.path) as conn:
        declaration = json.loads(conn.execute("SELECT body FROM orders").fetchone()[0])
    assert declaration["source_ref"].startswith("live/" + result["live_instance"] + "/")


@pytest.mark.parametrize("complete", [False, True])
def test_verified_get_identity_without_a_post_receipt_is_registered_without_promoting_history(
    setup, complete
):
    _, live = setup
    clock, _, posts, journal = live
    order = ready(live)
    plan = journal.request(order.client_id)
    with posts.operation("order", request_sha256=hashlib.sha256(plan.body).hexdigest()):
        journal.begin_submission(order.client_id, quote=quote(clock.now))
    journal.unknown(order.client_id)
    journal.reconcile(evidence(setup, order, complete=complete))
    journal.halt()
    posts.stop("operation_unknown")
    before, post_before = journal.snapshot(), posts.snapshot()
    result = source(setup).refresh()
    assert result["identified_count"] == 1 and result["registered_count"] == 1
    assert setup[0][5].catalog.lookup((201,)) == {201: order}
    assert journal.snapshot() == before and posts.snapshot() == post_before
    assert journal.snapshot()["orders"][0]["evidence"]["executions_complete"] == complete


@pytest.mark.parametrize("state", ["prepared", "abandoned", "unknown"])
def test_never_submitted_and_unidentified_intents_are_not_guessed_into_the_catalog(setup, state):
    _, live = setup
    clock, _, posts, journal = live
    order = ready(live)
    if state == "abandoned":
        journal.abandon(order.client_id)
    elif state == "unknown":
        plan = journal.request(order.client_id)
        with posts.operation("order", request_sha256=hashlib.sha256(plan.body).hexdigest()):
            journal.begin_submission(order.client_id, quote=quote(clock.now))
        journal.unknown(order.client_id)
    before = journal.snapshot()
    result = source(setup).refresh()
    assert result["records"] == result["identified_count"] == result["registered_count"] == 0
    assert result["unidentified_client_ids"] == ([order.client_id] if state == "unknown" else [])
    assert journal.snapshot() == before


@pytest.mark.parametrize(
    "damage",
    [
        "prepared_changed",
        "submitted_duplicate",
        "receipt_intent",
        "evidence_id",
        "reconciled_missing",
        "receipt_before_submit",
        "state",
    ],
)
def test_source_damage_refuses_registration_preserves_post_and_stops_live(setup, damage):
    values, live = setup
    order = accepted(setup)
    journal = live[3]
    journal.reconcile(evidence(setup, order))
    post_before = live[2].snapshot()
    with sqlite3.connect(journal.path) as conn:
        if damage == "reconciled_missing":
            conn.execute("DELETE FROM events WHERE kind='RECONCILED'")
        elif damage == "submitted_duplicate":
            conn.execute(
                "INSERT INTO events(recorded_at,client_id,kind,payload_json) "
                "SELECT recorded_at,client_id,kind,payload_json FROM events "
                "WHERE kind='SUBMITTING'"
            )
        elif damage == "prepared_changed":
            conn.execute("UPDATE events SET payload_json='{}' WHERE kind='PREPARED'")
        elif damage == "receipt_intent":
            payload = json.loads(
                conn.execute(
                    "SELECT payload_json FROM events WHERE kind='SUBMISSION_ACK'"
                ).fetchone()[0]
            )
            payload["intent"]["price"] = "151"
            conn.execute(
                "UPDATE events SET payload_json=? WHERE kind='SUBMISSION_ACK'",
                (json.dumps(payload),),
            )
        elif damage == "evidence_id":
            payload = json.loads(conn.execute("SELECT evidence_json FROM orders").fetchone()[0])
            payload["order_id"] = 202
            conn.execute("UPDATE orders SET evidence_json=?", (json.dumps(payload),))
        elif damage == "receipt_before_submit":
            conn.execute("UPDATE events SET id=0 WHERE kind='SUBMISSION_ACK'")
        else:
            conn.execute("UPDATE orders SET state='FILLED',evidence_json=NULL")
    with pytest.raises(LiveCatalogError, match="^live_catalog_source_failed$"):
        source(setup).refresh()
    assert values[5].catalog.snapshot()["records"] == 0 and live[2].snapshot() == post_before
    with sqlite3.connect(journal.path) as conn:
        assert conn.execute("SELECT halted FROM metadata").fetchone()[0] == 1


@pytest.mark.parametrize("conflict", ["order_id", "client_id"])
def test_existing_catalog_identity_conflicts_halt_live_without_overwriting_declarations(
    setup, conflict
):
    values, live = setup
    original = accepted(setup)
    workspace = values[5]
    declared = KnownOrder(
        order_id=201 if conflict == "order_id" else 202,
        intent=original.model_copy(update={"price": Decimal("151")})
        if conflict == "order_id"
        else original,
    )
    workspace.register_order(
        declared,
        expected_plan_sha256=workspace.plan_sha256,
        expected_catalog_head=workspace.catalog.snapshot()["head"],
        source_ref="synthetic-external-review",
        intent_confirmed=True,
    )
    before = workspace.catalog.snapshot()
    with pytest.raises(LiveCatalogError):
        source(setup).refresh()
    assert workspace.catalog.snapshot() == before and live[3].snapshot()["halted"]


def test_unknown_broker_identity_stops_live_without_creating_an_intent(setup):
    _, live = setup
    accepted(setup)
    with pytest.raises(LiveCatalogError, match="live_catalog_order_unknown"):
        source(setup).lookup((999,))
    assert live[3].snapshot()["halted"]
    assert setup[0][5].catalog.snapshot()["records"] == 1


@pytest.mark.parametrize(
    "change",
    [
        "order",
        "intent",
        "stale",
        "missing_fill",
        "changed_fill",
        "terminal_status",
        "terminal_extra",
    ],
)
def test_read_reports_must_match_saved_receipt_and_execution_history_before_booking(setup, change):
    _, live = setup
    order = accepted(setup)
    filled = Execution(
        execution_id=301,
        position_id=401,
        units=400,
        price="150",
        fee="2",
        loss_gain="0",
        settled_swap="0",
        timestamp=live[0].now,
    )
    previous = evidence(setup, order, complete=True).model_copy(
        update={
            "executions": (filled,),
            "status": "CANCELED" if change.startswith("terminal") else "ORDERED",
        }
    )
    live[3].reconcile(previous)
    live[0].advance(1)
    following = previous.model_copy(
        update={"observed_at": live[0].now, "executions_complete": False}
    )
    if change == "order":
        following = following.model_copy(update={"order_id": 999})
    elif change == "intent":
        following = following.model_copy(
            update={"intent": order.model_copy(update={"price": Decimal("151")})}
        )
    elif change == "stale":
        following = following.model_copy(
            update={"observed_at": previous.observed_at - timedelta(seconds=1)}
        )
    elif change == "missing_fill":
        following = following.model_copy(update={"executions": ()})
    elif change == "changed_fill":
        following = following.model_copy(
            update={"executions": (filled.model_copy(update={"price": Decimal("149")}),)}
        )
    elif change == "terminal_status":
        following = following.model_copy(update={"status": "ORDERED"})
    else:
        extra = filled.model_copy(
            update={
                "execution_id": 302,
                "units": 600,
                "timestamp": live[0].now,
            }
        )
        following = following.model_copy(update={"executions": (filled, extra)})
    posts = live[2].snapshot()
    with pytest.raises(LiveCatalogError, match="^live_catalog_read_failed$"):
        source(setup).verify_reports((OrderReadReport(evidence=following, observations=()),))
    assert live[3].snapshot()["halted"] and live[2].snapshot() == posts
    assert live[3].snapshot()["orders"][0]["evidence"] == previous.model_dump(mode="json")
    assert setup[0][5].book.snapshot()["executions"] == 0


def test_diagnostic_read_verification_keeps_terminal_full_proof_and_approval_unchanged(setup):
    _, live = setup
    order = accepted(setup)
    previous = evidence(setup, order, complete=True).model_copy(update={"status": "CANCELED"})
    live[3].reconcile(previous)
    before = live[3].snapshot()
    diagnostic = previous.model_copy(update={"executions_complete": False})
    source(setup).verify_reports((OrderReadReport(evidence=diagnostic, observations=()),))
    assert live[3].snapshot() == before


def test_code_update_still_exports_saved_identities_without_renewing_old_permission(
    setup, monkeypatch
):
    accepted(setup)
    before = setup[1][3].snapshot()["live_control"]
    monkeypatch.setattr("trading.live_journal.implementation_sha256", lambda: "b" * 64)
    assert source(setup).refresh()["registered_count"] == 1
    after = setup[1][3].snapshot()
    assert after["live_control"] == before and not after["live_enabled"]


def test_accepted_receipt_with_unresolved_post_claim_can_be_registered_but_not_released(
    setup, monkeypatch
):
    _, live = setup
    order = ready(live)
    original = live[2]._write

    def write(*args, **kwargs):
        if args[2] == "COMPLETED":
            raise ValueError("synthetic post completion failure")
        return original(*args, **kwargs)

    monkeypatch.setattr(live[2], "_write", write)
    with client(live, lambda request: response(live[0], request)) as sender:
        with pytest.raises(OrderTransportError):
            sender.submit(order.client_id, quote=quote(live[0].now))
    before, posts = live[3].snapshot(), live[2].snapshot()
    assert posts["claim"] is not None and posts["phase"] == "STOPPED"
    assert source(setup).refresh()["registered_count"] == 1
    assert live[3].snapshot() == before and live[2].snapshot() == posts


def test_stopped_sync_refreshes_live_receipt_then_books_matching_partial_fill_once(setup):
    values, live = setup
    clock, read_clocks, _, _, vault, workspace = values
    order = accepted(setup)
    row = execution(
        rootOrderId=101,
        clientOrderId=order.client_id,
        orderPrice="150.01",
        orderTimestamp=clock.wall.isoformat(),
        executionTimestamp=clock.wall.isoformat(),
    )
    with workspace.control.ownership():
        owner = workspace.control.begin(
            workspace.journal, expected_revision=0, expected_head=workspace.journal.head()
        )["owner"]
        session = workspace.journal.start_session(
            expected_head=workspace.journal.head(),
            at=clock.wall,
            monotonic_ns=int(clock.mono * 1e9),
        )
        record = workspace.journal.record(
            session,
            "EVENT",
            at=clock.wall,
            monotonic_ns=int(clock.mono * 1e9),
            sequence=1,
            payload=raw(row),
        )
        workspace.journal.acknowledge(session, record)
        workspace.control.finish(owner, workspace.journal, reason="stream_failed")
    live[3].halt()
    live[2].stop("operator_stop")
    before, post_before, control_before = (
        live[3].snapshot(),
        live[2].snapshot(),
        workspace.control.snapshot(),
    )
    broker_order = {
        "rootOrderId": 101,
        "orderId": 201,
        "clientOrderId": order.client_id,
        "symbol": "USD_JPY",
        "side": "BUY",
        "settleType": "OPEN",
        "orderType": "NORMAL",
        "executionType": "LIMIT",
        "size": "1000",
        "price": "150.01",
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
    fill.update(size="400", price="150", timestamp=row["executionTimestamp"])

    def get(request):
        assert request.method == "GET" and request.url.path.endswith(("/orders", "/executions"))
        return httpx.Response(
            200,
            json={
                "status": 0,
                "responsetime": clock.wall.isoformat(),
                "data": {"list": [broker_order if request.url.path.endswith("/orders") else fill]},
            },
        )

    args = dict(
        expected_plan_sha256=workspace.plan_sha256,
        expected_revision=control_before["revision"],
        expected_head=workspace.journal.head(),
        expected_reason=control_before["reason"],
        read_only_confirmed=True,
        vault=vault,
        read_clocks=read_clocks,
        read_transport=httpx.MockTransport(get),
    )
    first = workspace.reconcile_stopped(**args)
    second = workspace.reconcile_stopped(**args)
    assert first["booking"]["applied_execution_ids"] == (501,)
    assert second["booking"]["already_applied_execution_ids"] == (501,)
    assert second["cash"]["executions"] == second["catalog"]["records"] == 1
    assert live[3].snapshot() == before and live[2].snapshot() == post_before
    assert workspace.control.snapshot() == control_before


@pytest.mark.parametrize("changed", ["plan", "head", "missing_head"])
def test_manual_refresh_requires_original_plan_and_head_without_native_access_or_stop(
    setup, changed
):
    values, live = setup
    workspace, backend = values[5], values[3]
    accepted(setup)
    before = live[3].snapshot()
    args = {
        "expected_plan_sha256": workspace.plan_sha256,
        "expected_catalog_head": workspace.catalog.snapshot()["head"],
    }
    args["expected_plan_sha256" if changed == "plan" else "expected_catalog_head"] = (
        None if changed == "missing_head" else "b" * 64
    )
    with pytest.raises(ValueError):
        workspace.register_live_orders(**args)
    assert live[3].snapshot() == before and workspace.catalog.snapshot()["records"] == 0
    assert backend.reads == []


def test_concurrent_catalog_append_is_revalidated_without_replacing_either_mapping(
    setup, monkeypatch
):
    workspace = setup[0][5]
    order = accepted(setup)
    original = workspace.catalog.register
    raced = []

    def register(*args, **kwargs):
        if not raced:
            raced.append(
                original(
                    KnownOrder(order_id=301, intent=intent(client_id="External301")),
                    source_ref="synthetic-concurrent",
                    expected_head=workspace.catalog.snapshot()["head"],
                    intent_confirmed=True,
                )
            )
        return original(*args, **kwargs)

    monkeypatch.setattr(workspace.catalog, "register", register)
    result = source(setup).refresh()
    assert result["registered_count"] == 1 and result["records"] == 2
    assert workspace.catalog.lookup((201,))[201] == order
    assert not setup[1][3].snapshot()["halted"]


def test_already_registered_orders_are_verified_together_without_per_order_writes(
    setup, monkeypatch
):
    accepted(setup)
    source(setup).refresh()
    monkeypatch.setattr(
        setup[0][5].catalog, "register", lambda *a, **k: pytest.fail("known mapping rewritten")
    )
    result = source(setup).refresh()
    assert result["identified_count"] == 1 and result["registered_count"] == 0


def test_cli_refresh_is_local_and_keeps_binding_plan_and_activation(setup, capsys):
    workspace = setup[0][5]
    accepted(setup)
    before = setup[1][3].snapshot()
    main(
        [
            "register-live-orders",
            "--directory",
            str(workspace.directory),
            "--expected-plan-sha256",
            workspace.plan_sha256,
            "--expected-catalog-head",
            workspace.catalog.snapshot()["head"],
        ]
    )
    result = json.loads(capsys.readouterr().out)
    assert result["ok"] and result["registered_count"] == 1
    assert setup[1][3].snapshot() == before and setup[0][3].reads == []
    assert PrivateSyncWorkspace(workspace.directory).catalog.lookup((201,))


def test_invalid_bound_source_stops_runtime_before_keys_tokens_or_gets(setup):
    values, live = setup
    clock, read_clocks, _, backend, vault, workspace = values
    accepted(setup)
    with sqlite3.connect(live[3].path) as conn:
        conn.execute("DELETE FROM events WHERE kind='PREPARED'")
    with pytest.raises(SupervisorError, match="startup_failed"):
        workspace.run(
            threading.Event(),
            **options(workspace),
            vault=vault,
            read_clocks=read_clocks,
            read_transport=httpx.MockTransport(
                lambda _: pytest.fail("GET before source validation")
            ),
            token_transport=httpx.MockTransport(
                lambda _: pytest.fail("token before source validation")
            ),
            connector=lambda _: pytest.fail("socket before source validation"),
            stream_sleep=clock.advance,
        )
    assert backend.reads == [] and workspace.control.snapshot()["phase"] == "STOPPED"


def test_account_get_failure_in_bound_runtime_halts_previously_enabled_live_dispatch(setup):
    values, live = setup
    clock, read_clocks, _, _, vault, workspace = values
    ready(live)
    assert live[3].snapshot()["live_enabled"]

    def failed_get(_):
        raise OSError("synthetic unavailable broker")

    with pytest.raises(SupervisorError):
        workspace.run(
            threading.Event(),
            **options(workspace),
            vault=vault,
            read_clocks=read_clocks,
            read_transport=httpx.MockTransport(failed_get),
            token_transport=httpx.MockTransport(lambda r: token_response(clock, r.method)),
            connector=lambda _: Socket(clock),
            stream_sleep=clock.advance,
        )
    assert workspace.control.snapshot()["phase"] == "STOPPED"
    assert live[3].snapshot()["halted"] and not live[3].snapshot()["live_enabled"]


def test_reader_cleanup_failure_after_normal_sync_still_halts_live(setup, monkeypatch):
    from trading.private_read import PrivateReadClient

    values, live = setup
    clock, read_clocks, _, _, vault, workspace = values
    ready(live)
    stop = threading.Event()
    close, step = PrivateReadClient.close, PrivateStreamSupervisor.step

    def failed_close(reader):
        close(reader)
        raise OSError("synthetic cleanup failed")

    def finished(runner):
        result = step(runner)
        if runner.control.snapshot()["sync_successes"]:
            stop.set()
        return result

    monkeypatch.setattr(PrivateReadClient, "close", failed_close)
    monkeypatch.setattr(PrivateStreamSupervisor, "step", finished)
    with pytest.raises(OSError, match="synthetic cleanup failed"):
        workspace.run(
            stop,
            **options(workspace),
            vault=vault,
            read_clocks=read_clocks,
            read_transport=httpx.MockTransport(lambda r: account_response(clock, r)),
            token_transport=httpx.MockTransport(lambda r: token_response(clock, r.method)),
            connector=lambda _: Socket(clock),
            stream_sleep=clock.advance,
        )
    assert workspace.control.snapshot()["phase"] == "READY"
    assert live[3].snapshot()["halted"]


def test_live_receipt_added_during_capture_is_read_and_booked_once_without_manual_registration(
    setup, monkeypatch
):
    values, live = setup
    clock, read_clocks, _, _, vault, workspace = values
    manifest = (workspace.directory / "sync-plan.json").read_bytes()
    planned = ready(live)
    row = execution(
        rootOrderId=101,
        clientOrderId="Buy001",
        executionSize="1000",
        orderExecutedSize="1000",
        orderPrice="150.01",
        orderTimestamp=clock.wall.isoformat(),
        executionTimestamp=clock.wall.isoformat(),
    )
    broker_order = {
        "rootOrderId": 101,
        "orderId": 201,
        "clientOrderId": "Buy001",
        "symbol": "USD_JPY",
        "side": "BUY",
        "settleType": "OPEN",
        "orderType": "NORMAL",
        "executionType": "LIMIT",
        "size": "1000",
        "price": "150.01",
        "status": "EXECUTED",
        "timestamp": clock.wall.isoformat(),
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
    fill.update(size="1000", price="150", timestamp=row["executionTimestamp"])
    release, stop = threading.Event(), threading.Event()
    calls, sent, results, sockets = [], [], [], []

    def get(request):
        calls.append(request.url.path)
        assert request.method == "GET" and release.wait(3)
        if request.url.path.endswith("/orders") or request.url.path.endswith("/executions"):
            data = {"list": [broker_order if request.url.path.endswith("/orders") else fill]}
        elif request.url.path.endswith("/openPositions"):
            data = {
                "list": []
                if "prevId" in request.url.params
                else [
                    {
                        "positionId": 401,
                        "symbol": "USD_JPY",
                        "side": "BUY",
                        "size": "1000",
                        "orderedSize": "0",
                        "price": "150",
                        "lossGain": "0",
                        "totalSwap": "0",
                        "timestamp": row["executionTimestamp"],
                    }
                ]
            }
        else:
            payload = account_response(clock, request).json()
            if request.url.path.endswith("/account/assets"):
                payload["data"][0]["balance"] = "999998"
            return httpx.Response(200, json=payload)
        return httpx.Response(
            200, json={"status": 0, "data": data, "responsetime": clock.wall.isoformat()}
        )

    def connect(_):
        sock = Socket(clock)
        sockets.append(sock)
        return sock

    original = PrivateStreamSupervisor.step
    collect = _ReaderOwner.reservations

    def reservations(owner):
        assert release.wait(3)  # Submit in the capture window before the first GET claim.
        return collect(owner)

    def step(runner):
        if not sent:
            with client(live, lambda request: response(live[0], request)) as sender:
                sender.submit(planned.client_id, quote=quote(live[0].now))
            sent.append(planned)
            row["orderTimestamp"] = row["executionTimestamp"] = clock.wall.isoformat()
            broker_order["timestamp"] = fill["timestamp"] = clock.wall.isoformat()
            sockets[0].messages.append(raw(row))
        result = original(runner)
        release.set()
        if runner.control.snapshot()["sync_successes"]:
            results.append(runner._last_result)
            stop.set()
        if runner._worker is None:
            clock.advance(1)
        return result

    monkeypatch.setattr(PrivateStreamSupervisor, "step", step)
    monkeypatch.setattr(_ReaderOwner, "reservations", reservations)
    result = workspace.run(
        stop,
        **options(workspace),
        vault=vault,
        read_transport=httpx.MockTransport(get),
        token_transport=httpx.MockTransport(lambda r: token_response(clock, r.method)),
        connector=connect,
        read_clocks=read_clocks,
        stream_sleep=clock.advance,
    )
    assert result["control"]["phase"] == "READY" and result["cash"]["executions"] == 1
    assert Decimal(result["cash"]["balance"]) == Decimal("999998")
    assert result["catalog"]["records"] == 1 and workspace.catalog.lookup((201,))[201] == sent[0]
    assert results[0].account_inventory["positions"]["position_match"]
    assert calls.count("/private/v1/orders") >= 2 and calls.count("/private/v1/executions") >= 2
    assert (workspace.directory / "sync-plan.json").read_bytes() == manifest
    assert live[3].snapshot()["orders"][0]["state"] == "RECONCILING"
    assert live[3].snapshot()["orders"][0]["evidence"] is None
    assert not result["complete"] and not result["live_enabled"]


@pytest.mark.parametrize("phase", ["before_commit", "after_commit"])
def test_real_process_death_at_catalog_commit_retries_locally_without_duplicates(setup, phase):
    values, live = setup
    accepted(setup)
    script = r"""
import ctypes,os,socket,sys
from datetime import datetime
from trading.private_sync import PrivateSyncWorkspace
from trading.known_orders import KnownOrderCatalog
def forbidden(*a,**k): raise AssertionError("network/native credentials forbidden")
socket.socket=forbidden; ctypes.WinDLL=forbidden
w=PrivateSyncWorkspace(sys.argv[1],clock=lambda:datetime.fromisoformat(sys.argv[2]))
original=KnownOrderCatalog._register
def register(self,*a,**k):
    if sys.argv[3]=="before_commit": os._exit(77)
    result=original(self,*a,**k); os._exit(77)
KnownOrderCatalog._register=register
w.register_live_orders(expected_plan_sha256=w.plan_sha256,expected_catalog_head=w.catalog.snapshot()["head"])
"""
    workspace = values[5]
    before, post_before = live[3].snapshot(), live[2].snapshot()
    process = subprocess.run(
        [sys.executable, "-c", script, str(workspace.directory), values[0].wall.isoformat(), phase],
        capture_output=True,
        timeout=15,
    )
    assert process.returncode == 77, process.stderr.decode()
    assert workspace.catalog.snapshot()["records"] == (phase == "after_commit")
    source(setup).refresh()
    assert workspace.catalog.snapshot()["records"] == 1
    assert live[3].snapshot() == before and live[2].snapshot() == post_before
