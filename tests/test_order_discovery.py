"""Finding broker IDs of an unknown order among active orders, with synthetic GETs only."""

import ctypes
import socket
from datetime import timedelta

import httpx
import pytest
from test_account_guard import quote
from test_live_account import broker
from test_live_operations import setup as operations_setup
from test_live_operations import unbound as operations_unbound
from test_private_order import client

from trading import order_discovery
from trading.order_discovery import OrderDiscovery, OrderDiscoveryError
from trading.private_order import OrderTransportError


@pytest.fixture(autouse=True)
def no_native_or_network(monkeypatch):
    def forbidden(*args, **kwargs):
        pytest.fail("real credentials/network attempted")

    monkeypatch.setattr(ctypes, "WinDLL", forbidden, raising=False)
    monkeypatch.setattr(socket, "socket", forbidden)


@pytest.fixture
def setup(tmp_path):
    yield from operations_setup.__wrapped__(operations_unbound.__wrapped__(tmp_path))


def unknown(setup):
    _, live, _, order, _ = setup
    with client(live, lambda request: httpx.Response(500)) as sender:
        with pytest.raises(OrderTransportError):
            sender.submit(order.client_id, quote=quote(live[0].now))
    assert live[3].snapshot()["orders"][0]["state"] == "UNKNOWN"
    return order


def wire(order, clock, **changes):
    return {
        "rootOrderId": 101,
        "orderId": 201,
        "clientOrderId": order.client_id,
        "symbol": "USD_JPY",
        "side": order.side,
        "orderType": "NORMAL",
        "executionType": "LIMIT",
        "settleType": order.effect,
        "size": str(order.units),
        "price": str(order.price),
        "status": "ORDERED",
        "timestamp": (clock.wall - timedelta(seconds=1)).isoformat(),
        **changes,
    }


def discover(setup, orders=(), *, confirmed=True):
    values, live = setup[0], setup[1]
    clock = values[0]
    clock.advance(1)
    elapsed = [0]

    def sleep(seconds):
        clock.advance(seconds)
        elapsed[0] += round(seconds * 1e9)

    discovery = OrderDiscovery(
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
    )
    calls = []
    result = discovery.discover(
        setup[3].client_id,
        credential_reference=values[5].plan.credential_reference,
        read_only_confirmed=confirmed,
        vault=values[4],
        transport=broker(clock, calls, orders=orders),
    )
    return result, calls


def test_still_active_unknown_order_is_found_without_changing_the_journal(setup):
    _, live, _, order, _ = setup
    unknown(setup)
    before = live[3].snapshot()
    result, calls = discover(setup, [wire(order, setup[0][0])])
    assert result["found"] and (result["root_order_id"], result["order_id"]) == (101, 201)
    assert result["status"] == "ORDERED" and not result["absence_proven"]
    assert {request.method for request in calls} == {"GET"}
    assert live[3].snapshot() == before


def test_absence_is_reported_but_never_proven(setup):
    _, live, _, order, _ = setup
    unknown(setup)
    other = wire(order, setup[0][0], clientOrderId="Other001", rootOrderId=301, orderId=301)
    result, _ = discover(setup, [other])
    assert result["found"] is False and result["absence_proven"] is False
    assert "order_id" not in result


@pytest.mark.parametrize("change", [{"size": "900"}, {"side": "SELL"}])
def test_same_client_id_with_different_terms_is_refused(setup, change):
    _, _, _, order, _ = setup
    unknown(setup)
    with pytest.raises(OrderDiscoveryError, match="active_order_terms_differ"):
        discover(setup, [wire(order, setup[0][0], **change)])


def test_confirmation_and_unclaimed_orders_refuse_before_any_read(setup):
    values = setup[0]
    with pytest.raises(ValueError):
        discover(setup)  # A PREPARED order has no unknown claim to investigate.
    unknown(setup)
    with pytest.raises(OrderDiscoveryError, match="read_confirmation_required"):
        discover(setup, confirmed=False)
    assert values[3].reads == []


def test_cli_hides_failures(setup, capsys):
    values, live = setup[0], setup[1]
    with pytest.raises(SystemExit) as raised:
        order_discovery.main(
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
    assert "read_confirmation_required" in capsys.readouterr().err
    assert values[3].reads == []


def test_journal_change_during_the_reads_returns_no_stale_ids(setup, monkeypatch):
    from trading.live_journal import LiveOrderJournal

    _, live, _, order, _ = setup
    unknown(setup)
    original = LiveOrderJournal.order_recovery_context
    calls = []

    def advanced(self, client_id):
        context = original(self, client_id)
        calls.append(client_id)
        # The second read (after the GETs) sees another process's resolution.
        return context if len(calls) == 1 else {**context, "checkpoint_sha256": "0" * 64}

    monkeypatch.setattr(LiveOrderJournal, "order_recovery_context", advanced)
    with pytest.raises(OrderDiscoveryError, match="journal_changed_during_discovery"):
        discover(setup, [wire(order, setup[0][0])])
    assert len(calls) == 2
