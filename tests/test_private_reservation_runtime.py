"""Actual read/runtime pipeline discovers and fences known active reservations."""

import ctypes
import socket
import threading

import httpx
import pytest
from test_account_events import raw
from test_execution_positions import close
from test_private_stream import response
from test_private_supervisor import Socket
from test_private_sync import account_response, make_setup, options

from trading.account_read_lab import demo_transcript
from trading.account_reader import CollectionError
from trading.broker_contracts import OrderIntent, RequestPlan, Settlement
from trading.execution_positions import OpeningPosition, PositionBasis
from trading.private_supervisor import PrivateStreamSupervisor, SupervisorError
from trading.private_sync import KnownOrder, PrivateSyncError, _ReaderOwner


@pytest.fixture(autouse=True)
def no_native_or_network(monkeypatch):
    def forbidden(*args, **kwargs):
        pytest.fail("real credentials/network attempted")

    monkeypatch.setattr(ctypes, "WinDLL", forbidden, raising=False)
    monkeypatch.setattr(socket, "socket", forbidden)


def known(identity=201):
    return KnownOrder(
        order_id=identity,
        intent=OrderIntent(
            client_id=f"Known{identity}",
            side="SELL",
            effect="CLOSE",
            units=300,
            kind="LIMIT",
            price="150.1",
            positions=(Settlement(position_id=401, units=300),),
        ),
    )


def basis():
    return PositionBasis(
        positions=(OpeningPosition(position_id=401, side="BUY", units=400, average_price="150"),)
    )


class Responses:
    def __init__(self, clock, *, kind="match", delay=0):
        self.clock, self.kind, self.delay = clock, kind, delay
        self.started_at = clock.wall
        self.calls = []
        self.assets_seen = 0

    def __call__(self, request):
        self.calls.append((request.url.path, tuple(request.url.params.multi_items())))
        self.clock.advance(self.delay)
        path = request.url.path
        identity = 999 if self.kind == "unknown" else 201
        order = {
            "rootOrderId": identity,
            "orderId": identity,
            "clientOrderId": f"Known{identity}",
            "symbol": "USD_JPY",
            "side": "SELL",
            "settleType": "CLOSE",
            "orderType": "NORMAL",
            "executionType": "LIMIT",
            "size": "300",
            "price": "150.1",
            "status": "ORDERED",
            "timestamp": self.started_at.isoformat(),
        }
        discovery = self.assets_seen <= 4
        active = not (
            self.kind in {"empty", "flat"}
            or self.kind == "late"
            and discovery
            or self.kind == "gone"
            and not discovery
        )
        if path.endswith("/account/assets"):
            self.assets_seen += 1
            return account_response(self.clock, request)
        if path.endswith("/openPositions"):
            data = demo_transcript(self.started_at).exchanges[1].response["data"]
            data["list"][0]["orderedSize"] = (
                "100" if self.kind == "difference" else "300" if active else "0"
            )
            if "prevId" in request.url.params or self.kind == "flat":
                data = {"list": []}
        elif path.endswith("/activeOrders"):
            data = {"list": [order] if active and "prevId" not in request.url.params else []}
        elif path.endswith("/orders"):
            if self.kind == "wrong_intent":
                order["price"] = "151"
            elif self.kind == "gone":
                order["status"] = "CANCELED"
            data = {"list": [order]}
        else:
            assert path.endswith("/executions")
            data = {"list": []}
        return httpx.Response(
            200, json={"status": 0, "data": data, "responsetime": self.clock.wall.isoformat()}
        )


def run(values, get, monkeypatch):
    clock, read_clocks, _, _, vault, workspace = values
    stop = threading.Event()
    observed = []
    original = PrivateStreamSupervisor.step

    def step(runner):
        original(runner)
        if runner.control.snapshot()["sync_successes"]:
            observed.append(runner._last_result)
            stop.set()

    monkeypatch.setattr(PrivateStreamSupervisor, "step", step)
    result = workspace.run(
        stop,
        **options(workspace),
        vault=vault,
        read_transport=httpx.MockTransport(get),
        token_transport=httpx.MockTransport(lambda request: response(clock, request.method)),
        connector=lambda _: Socket(clock),
        read_clocks=read_clocks,
        stream_sleep=clock.advance,
    )
    return result, observed


@pytest.mark.parametrize("kind", ["match", "empty", "gone", "flat"])
def test_declared_position_runtime_collects_and_compares_current_reservations(
    tmp_path, monkeypatch, kind
):
    opening = PositionBasis(positions=()) if kind == "flat" else basis()
    values = make_setup(tmp_path, position_basis=opening, known_orders=(known(),))
    clock, _, _, _, _, workspace = values
    manifest = (workspace.directory / "sync-plan.json").read_bytes()
    get = Responses(clock, kind=kind)
    result, observed = run(values, get, monkeypatch)
    assert result["control"]["phase"] == "READY" and result["control"]["sync_successes"] == 1
    diagnostic = observed[0]
    assert diagnostic.position_reservations["reservation_match"]
    assert diagnostic.account_inventory["positions"]["position_match"]
    assert diagnostic.position_reservations["head"] == diagnostic.account_inventory["head"]
    assert result["cash"]["executions"] == 0 and not result["live_enabled"]
    assert (workspace.directory / "sync-plan.json").read_bytes() == manifest
    paths = [p for p, _ in get.calls]
    if kind in {"match", "gone"}:
        first_order = paths.index("/private/v1/orders")
        last_order = max(i for i, p in enumerate(paths) if p == "/private/v1/executions")
        assert first_order > 0 and "/private/v1/account/assets" in paths[last_order + 1 :]
        assert paths.count("/private/v1/orders") == paths.count("/private/v1/executions") == 2
    else:
        assert "/private/v1/orders" not in paths
    assert get.assets_seen == 8  # Separate discovery and final repeated account observations.


@pytest.mark.parametrize("kind", ["difference", "unknown", "late", "wrong_intent"])
def test_current_reservation_failure_persists_stop_without_booking_or_plan_change(
    tmp_path, monkeypatch, kind
):
    values = make_setup(tmp_path, position_basis=basis(), known_orders=(known(),))
    clock, _, _, _, _, workspace = values
    before = workspace.book.snapshot()
    get = Responses(clock, kind=kind)
    with pytest.raises(SupervisorError, match="sync_failed"):
        run(values, get, monkeypatch)
    assert workspace.control.snapshot()["phase"] == "STOPPED"
    assert workspace.control.snapshot()["sync_successes"] == 0
    assert workspace.book.snapshot() == before
    paths = [p for p, _ in get.calls]
    if kind in {"unknown", "late"}:
        assert "/private/v1/orders" not in paths and "/private/v1/executions" not in paths
    if kind == "unknown":
        assert get.assets_seen == 4  # Unknown ID rejected after discovery, before final report.
    elif kind in {"difference", "late"}:
        assert get.assets_seen == 8


def test_discovery_orders_and_final_account_use_one_budget(tmp_path):
    values = make_setup(
        tmp_path, position_basis=basis(), known_orders=(known(),), collection_limit_seconds=10
    )
    clock, _, _, _, _, workspace = values
    handler = Responses(clock, delay=0.5)

    class Client:
        def get(self, request):
            query = dict(request.query)
            return handler(
                httpx.Request("GET", "https://synthetic/private" + request.path, params=query)
            ).json()

        def close(self):
            pass

    owner = _ReaderOwner(
        Client(), workspace.plan, clock=lambda: clock.wall, monotonic=lambda: clock.mono
    )
    reservations = owner.reservations()
    assert len(reservations) == 1 and clock.mono == 8  # 12 discovery GETs + 4 order GETs.
    with pytest.raises(CollectionError, match="read_transport_failed"):
        owner.account(continuation=True)
    assert clock.mono == 10 and len(handler.calls) == 20
    with pytest.raises(PrivateSyncError, match="sync_collection_deadline"):
        owner.get(RequestPlan("GET", "/v1/orders", query=(("orderId", "201"),)))
    assert len(handler.calls) == 20


def test_periodic_attempt_gets_a_fresh_shared_budget(tmp_path):
    values = make_setup(tmp_path, position_basis=PositionBasis(positions=()))
    clock, _, _, _, _, workspace = values
    handler = Responses(clock, kind="flat", delay=0.5)

    class Client:
        def get(self, request):
            return handler(
                httpx.Request(
                    "GET", "https://synthetic/private" + request.path, params=dict(request.query)
                )
            ).json()

        def close(self):
            pass

    owner = _ReaderOwner(
        Client(), workspace.plan, clock=lambda: clock.wall, monotonic=lambda: clock.mono
    )
    for _ in range(2):
        assert owner.reservations() == ()
        assert not owner.account(continuation=True).positions
        clock.advance(31)
    assert len(handler.calls) == 32


def test_all_discovered_ids_are_validated_before_first_order_get(tmp_path):
    values = make_setup(tmp_path, position_basis=basis(), known_orders=(known(),))
    clock, _, _, _, _, workspace = values
    handler = Responses(clock, kind="unknown")

    class Client:
        def get(self, request):
            result = handler(
                httpx.Request(
                    "GET", "https://synthetic/private" + request.path, params=dict(request.query)
                )
            ).json()
            if request.path == "/v1/activeOrders" and result["data"]["list"]:
                other = {
                    **result["data"]["list"][0],
                    "orderId": 201,
                    "rootOrderId": 201,
                    "clientOrderId": "Known201",
                }
                result["data"]["list"].append(other)
            return result

        def close(self):
            pass

    owner = _ReaderOwner(
        Client(), workspace.plan, clock=lambda: clock.wall, monotonic=lambda: clock.mono
    )
    with pytest.raises(PrivateSyncError, match="sync_order_intent_missing"):
        owner.reservations()
    assert all(
        path not in {"/private/v1/orders", "/private/v1/executions"} for path, _ in handler.calls
    )
    assert handler.assets_seen == 4


def test_expired_shared_budget_persists_runtime_stop(tmp_path, monkeypatch):
    values = make_setup(
        tmp_path, position_basis=basis(), known_orders=(known(),), collection_limit_seconds=10
    )
    clock, _, _, _, _, workspace = values
    get = Responses(clock, delay=0.5)
    with pytest.raises(SupervisorError, match="sync_failed"):
        run(values, get, monkeypatch)
    assert workspace.control.snapshot()["phase"] == "STOPPED"
    assert workspace.control.snapshot()["sync_successes"] == 0
    assert workspace.book.snapshot()["executions"] == 0
    # The persistent limiter's waiting time also consumes the shared budget.
    assert 12 <= len(get.calls) <= 20 and clock.mono >= 10


@pytest.mark.parametrize("deliver", [True, False])
def test_partial_close_is_reserved_after_cash_booking_and_unnotified_fill_stops(
    tmp_path, monkeypatch, deliver
):
    values = make_setup(tmp_path, position_basis=basis(), known_orders=(known(),))
    clock, read_clocks, _, _, vault, workspace = values
    row = close(units=100, pnl="10", order_id=201)
    row.update(
        clientOrderId="Known201",
        orderSize="300",
        orderExecutedSize="100",
        executionTimestamp=clock.wall.isoformat(),
        orderTimestamp=clock.wall.isoformat(),
    )
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
    fill.update(size="100", price="150.1", timestamp=clock.wall.isoformat())
    source = Responses(clock)
    release, stop = threading.Event(), threading.Event()
    original = PrivateStreamSupervisor.step
    observed = []

    def get(request):
        assert release.wait(3)
        result = source(request).json()
        if request.url.path.endswith("/account/assets"):
            result["data"][0]["balance"] = "1000008"
        elif request.url.path.endswith("/openPositions") and result["data"]["list"]:
            result["data"]["list"][0].update(size="300", orderedSize="200")
        elif request.url.path.endswith("/executions"):
            result["data"]["list"] = [fill]
        return httpx.Response(200, json=result)

    def connect(_):
        sock = Socket(clock)
        if deliver:
            sock.messages.append(raw(row))
        return sock

    def step(runner):
        original(runner)
        release.set()
        if runner.control.snapshot()["sync_successes"]:
            observed.append(runner._last_result)
            stop.set()
        if runner._worker is None:
            clock.advance(1)

    monkeypatch.setattr(PrivateStreamSupervisor, "step", step)

    def execute():
        return workspace.run(
            stop,
            **options(workspace),
            vault=vault,
            read_transport=httpx.MockTransport(get),
            token_transport=httpx.MockTransport(lambda request: response(clock, request.method)),
            connector=connect,
            read_clocks=read_clocks,
            stream_sleep=clock.advance,
        )

    if deliver:
        result = execute()
        diagnostic = observed[0]
        assert result["cash"]["execution_ids"] == (601,)
        assert diagnostic.position_reservations["reservation_match"]
        assert diagnostic.position_reservations["orders"][0]["remaining_units"] == 200
        assert (
            diagnostic.execution_cash["head"]
            == diagnostic.position_reservations["head"]
            == diagnostic.account_inventory["head"]
        )
        assert result["cash"]["positions"][0]["units"] == 300
    else:
        with pytest.raises(SupervisorError, match="sync_failed"):
            execute()
        assert workspace.book.snapshot()["execution_ids"] == ()
        assert workspace.control.snapshot()["phase"] == "STOPPED"
