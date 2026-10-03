"""Accepted live orders reconciled from saved broker IDs with synthetic GETs only."""

import ctypes
import socket
from datetime import timedelta

import httpx
import pytest
from test_account_guard import quote
from test_live_account import run
from test_live_operations import setup as operations_setup
from test_live_operations import unbound as operations_unbound
from test_private_order import client, response

from trading import live_order_sync
from trading.live_order_sync import HISTORY_CONFIRMATIONS, LiveOrderSync, LiveOrderSyncError
from trading.order_journal import OrderBlocked


@pytest.fixture(autouse=True)
def no_native_or_network(monkeypatch):
    def forbidden(*args, **kwargs):
        pytest.fail("real credentials/network attempted")

    monkeypatch.setattr(ctypes, "WinDLL", forbidden, raising=False)
    monkeypatch.setattr(socket, "socket", forbidden)


@pytest.fixture
def setup(tmp_path):
    yield from operations_setup.__wrapped__(operations_unbound.__wrapped__(tmp_path))


def accept(setup):
    _, live, _, order, _ = setup
    with client(live, lambda request: response(live[0], request)) as sender:
        sender.submit(order.client_id, quote=quote(live[0].now))
    assert live[3].snapshot()["orders"][0]["state"] == "RECONCILING"
    return order


def broker_order(order, clock, *, status="ORDERED", order_id=201):
    return {
        "rootOrderId": 101,
        "orderId": order_id,
        "clientOrderId": order.client_id,
        "symbol": "USD_JPY",
        "side": order.side,
        "settleType": order.effect,
        "orderType": "NORMAL",
        "executionType": order.kind,
        "size": str(order.units),
        "price": str(order.price),
        "status": status,
        "timestamp": clock.wall.isoformat(),
    }


def execution(order, clock, **changes):
    return {
        "executionId": 301,
        "positionId": 401,
        "orderId": 201,
        "clientOrderId": order.client_id,
        "symbol": "USD_JPY",
        "side": order.side,
        "settleType": order.effect,
        "size": str(order.units),
        "price": str(order.price),
        "amount": "-3",
        "fee": "-3",
        "lossGain": "0",
        "settledSwap": "0",
        "timestamp": clock.wall.isoformat(),
        **changes,
    }


def sync(setup, orders, fills=(), *, confirmations=HISTORY_CONFIRMATIONS):
    values, live = setup[0], setup[1]
    clock = values[0]
    clock.advance(1)
    elapsed = [0]

    def sleep(seconds):
        clock.advance(seconds)
        elapsed[0] += round(seconds * 1e9)

    calls = []

    def handler(request):
        calls.append(request)
        assert request.method == "GET"
        rows = orders if request.url.path.endswith("/orders") else list(fills)
        return httpx.Response(
            200,
            json={"status": 0, "responsetime": clock.wall.isoformat(), "data": {"list": rows}},
        )

    syncer = LiveOrderSync(
        live[3].path.parent,
        live[1].path.parent,
        "synthetic",
        clock=lambda: clock.wall,
        monotonic=lambda: clock.mono,
        read_clocks={
            "wall_ns": lambda: int(clock.wall.timestamp() * 1e9),
            "monotonic_ns": lambda: elapsed[0],
            "sleep": sleep,
        },
        post_sleep=clock.advance,
    )
    result = syncer.reconcile(
        setup[3].client_id,
        credential_reference=values[5].plan.credential_reference,
        confirmations=confirmations,
        vault=values[4],
        transport=httpx.MockTransport(handler),
    )
    return result, calls


def test_accepted_order_becomes_working_and_the_account_refresh_then_reconciles(setup):
    values, live, _, order, _ = setup
    accept(setup)
    clock = values[0]
    result, calls = sync(setup, [broker_order(order, clock)])
    assert result["state"] == "WORKING" and result["order_id"] == 201
    assert {r.method for r in calls} == {"GET"} and len(calls) == 4
    row = live[3].snapshot()["orders"][0]
    assert row["state"] == "WORKING" and row["evidence"]["executions_complete"] is True
    # The account proof can now include the working order and admit later work.
    wire = {
        **broker_order(order, clock),
        "timestamp": (clock.wall - timedelta(seconds=1)).isoformat(),
    }
    refreshed = run(setup, [], orders=[wire])
    assert refreshed["reconciled"] and refreshed["working_orders"] == 1


def test_full_execution_is_filled(setup):
    values, live, _, order, _ = setup
    accept(setup)
    clock = values[0]
    result, _ = sync(
        setup, [broker_order(order, clock, status="EXECUTED")], [execution(order, clock)]
    )
    assert result["state"] == "FILLED" and result["executions"] == 1
    assert not live[3].snapshot()["halted"]


@pytest.mark.parametrize(
    "confirmations",
    [set(), {"complete-history"}, {*HISTORY_CONFIRMATIONS, "x"}, None],
)
def test_confirmations_are_required_before_any_read(setup, confirmations):
    values = setup[0]
    accept(setup)
    with pytest.raises(LiveOrderSyncError, match="history_confirmations_required"):
        sync(setup, [], confirmations=confirmations)
    assert values[3].reads == []


def test_unsent_or_unknown_orders_use_other_paths(setup):
    values, live, _, order, _ = setup
    with pytest.raises(LiveOrderSyncError, match="accepted_order_required"):
        sync(setup, [])
    with client(live, lambda request: httpx.Response(500)) as sender:
        with pytest.raises(ValueError):
            sender.submit(order.client_id, quote=quote(live[0].now))
    with pytest.raises(LiveOrderSyncError, match="accepted_order_required"):
        sync(setup, [])
    assert values[3].reads == []


def test_missing_or_different_broker_order_fails_without_changing_the_journal(setup):
    values, live, _, order, _ = setup
    accept(setup)
    before = live[3].snapshot()["orders"]
    with pytest.raises(LiveOrderSyncError, match="order_collection_failed"):
        sync(setup, [broker_order(order, values[0], order_id=999)])
    with pytest.raises(LiveOrderSyncError, match="order_collection_failed"):
        sync(setup, [])
    assert live[3].snapshot()["orders"] == before and not live[3].snapshot()["halted"]


def test_evidence_contradicting_the_receipt_halts_the_journal(setup):
    values, live, _, order, _ = setup
    accept(setup)
    clock = values[0]
    early = execution(order, clock, timestamp=(clock.wall - timedelta(hours=1)).isoformat())
    with pytest.raises(OrderBlocked, match="submission_receipt_evidence_mismatch"):
        sync(setup, [broker_order(order, clock, status="EXECUTED")], [early])
    assert live[3].snapshot()["halted"]


def test_cli_requires_confirmations(setup, capsys):
    values, live = setup[0], setup[1]
    with pytest.raises(SystemExit) as raised:
        live_order_sync.main(
            [
                "--directory",
                str(live[3].path.parent),
                "--read-control-directory",
                str(live[1].path.parent),
                "--scope",
                "synthetic",
                "--client-id",
                setup[3].client_id,
                "--credential-reference",
                values[5].plan.credential_reference,
            ]
        )
    assert raised.value.code == 2
    assert "history_confirmations_required" in capsys.readouterr().err
    assert values[3].reads == []
