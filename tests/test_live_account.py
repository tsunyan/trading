"""Read-only account collection into the live account gate, with synthetic GET/ticker only."""

import ctypes
import socket
from datetime import timedelta
from decimal import Decimal

import httpx
import pytest
from test_account_guard import intent, quote
from test_live_operations import setup as operations_setup
from test_live_operations import unbound as operations_unbound
from test_private_order import response

from trading import live_account
from trading.account_reader import (
    AccountReadReport,
    ActiveOrder,
    Assets,
    HeldPosition,
    Observation,
)
from trading.execution_lab import fixture_evidence
from trading.live_account import (
    ACCOUNT_CONFIRMATIONS,
    AccountDiscrepancy,
    LiveAccountError,
    LiveAccountRefresh,
    snapshot_from_report,
)
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


def refresher(setup):
    clock = setup[0][0]
    # Integer nanoseconds: float sums such as 2.1 + 0.25 can truncate below the read pacing.
    elapsed = [0]

    def sleep(seconds):
        clock.advance(seconds)
        elapsed[0] += round(seconds * 1e9)

    return LiveAccountRefresh(
        setup[1][3].path.parent,
        setup[1][1].path.parent,
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


def broker(clock, calls, *, assets=None, positions=(), orders=()):
    row = {
        "balance": "1000000",
        "equity": "1000000",
        "availableAmount": "1000000",
        "margin": "0",
        "estimatedTradeFee": "0",
        "positionLossGain": "0",
        "totalSwap": "0",
        "transferableAmount": "1000000",
        **(assets or {}),
    }

    def handler(request):
        calls.append(request)
        assert request.method == "GET"
        path, first = request.url.path, "prevId" not in request.url.params
        if path.endswith("/account/assets"):
            data = [row]
        elif path.endswith("/openPositions"):
            data = {"list": list(positions) if first else []}
        elif path.endswith("/activeOrders"):
            data = {"list": list(orders) if first else []}
        else:
            pytest.fail("unexpected private path")
        return httpx.Response(
            200, json={"status": 0, "data": data, "responsetime": clock.wall.isoformat()}
        )

    return httpx.MockTransport(handler)


def run(setup, calls, *, confirmations=ACCOUNT_CONFIRMATIONS, current=None, **broker_options):
    values = setup[0]
    clock = values[0]
    clock.advance(1)
    return refresher(setup).refresh(
        values[5].plan.credential_reference,
        confirmations=confirmations,
        quote=current or quote(clock.wall),
        vault=values[4],
        transport=broker(clock, calls, **broker_options),
    )


def report(when, *, positions=(), orders=(), swap="0", **assets):
    values = {
        "balance": "1000000",
        "equity": "1000000",
        "available_amount": "1000000",
        "margin": "0",
        "estimated_trade_fee": "0",
        "position_loss_gain": "0",
        "total_swap": swap,
        "transferable_amount": "1000000",
        **assets,
    }
    return AccountReadReport(
        assets=Assets(**values),
        positions=positions,
        active_orders=orders,
        observations=tuple(
            Observation(
                path="/v1/account/assets",
                query=(),
                response_at=when + timedelta(seconds=offset),
                received_at=when + timedelta(seconds=offset),
                sha256="a" * 64,
            )
            for offset in (2, 0, 1)
        ),
    )


def working_row(order, when, fills=()):
    evidence = fixture_evidence(order, 101, 201, "ORDERED", list(fills), when)
    return {
        "client_id": order.client_id,
        "state": "WORKING",
        "intent_json": order.model_dump_json(),
        "evidence_json": evidence.model_dump_json(),
    }


def active(order, when, **changes):
    return ActiveOrder(
        **{
            "root_order_id": 101,
            "order_id": 201,
            "client_id": order.client_id,
            "symbol": "USD_JPY",
            "side": order.side,
            "effect": order.effect,
            "kind": "LIMIT",
            "units": order.units,
            "price": order.price,
            "status": "ORDERED",
            "timestamp": when,
            **changes,
        }
    )


def test_report_maps_to_a_declared_complete_snapshot_at_the_earliest_response():
    from test_account_guard import NOW

    held = HeldPosition(
        position_id=401,
        symbol="USD_JPY",
        side="BUY",
        units=1000,
        ordered_units=0,
        price="150.01",
        loss_gain="-10",
        total_swap="3",
        timestamp=NOW,
    )
    order = intent()
    snapshot = snapshot_from_report(
        report(NOW, positions=(held,), orders=(active(order, NOW),), swap="3", margin="6000"),
        account_id="fixture-account",
        rows=[working_row(order, NOW)],
    )
    assert snapshot.complete and snapshot.observed_at == NOW
    assert snapshot.account_id == "fixture-account"
    assert snapshot.unrealized_swap == Decimal(3) and snapshot.required_margin == 6000
    assert snapshot.positions[0].average_price == Decimal("150.01")
    assert [(o.client_id, o.order_id, o.remaining_units) for o in snapshot.working_orders] == [
        (order.client_id, 201, 1000)
    ]


def test_inconsistent_swap_totals_are_refused_without_halting():
    from test_account_guard import NOW

    with pytest.raises(LiveAccountError, match="position_swap_total_mismatch"):
        snapshot_from_report(report(NOW, swap="1"), account_id="a", rows=[])
    with pytest.raises(LiveAccountError, match="account_read_report_required"):
        snapshot_from_report({}, account_id="a", rows=[])


@pytest.mark.parametrize(
    "change",
    [
        {"client_id": "Other001"},
        {"order_id": 202},
        {"root_order_id": 102},
        {"units": 900},
        {"price": Decimal("149")},
        {"side": "SELL"},
        {"effect": "CLOSE"},
    ],
)
def test_active_orders_must_be_explained_by_the_journal(change):
    from test_account_guard import NOW

    order = intent()
    with pytest.raises(AccountDiscrepancy):
        snapshot_from_report(
            report(NOW, orders=(active(order, NOW, **change),)),
            account_id="a",
            rows=[working_row(order, NOW)],
        )


def test_refresh_reconciles_flat_account_with_get_only_and_no_post(setup):
    values, live, _, order, _ = setup
    before_posts = live[2].snapshot()
    calls = []
    result = run(setup, calls)
    assert result["reconciled"] and result["positions"] == result["working_orders"] == 0
    assert calls and {request.method for request in calls} == {"GET"}
    assert len(calls) == 8  # Two sweeps: assets, empty positions, empty orders, assets.
    assert live[2].snapshot()["revision"] == before_posts["revision"]
    proof = live[3].snapshot()["account_guard"]["last_proof"]
    assert proof["snapshot"]["complete"] is True
    assert proof["snapshot"]["observed_at"] == result["observed_at"]
    assert values[3].reads == [values[5].plan.credential_reference]
    assert not live[3].snapshot()["halted"]
    # The refreshed proof still admits the reviewed send path for the prepared order.
    context = live[3].execution_context(order.client_id, quote=quote(values[0].wall))
    assert context["risk"]["allowed"]


@pytest.mark.parametrize(
    "confirmations",
    [set(), ACCOUNT_CONFIRMATIONS - {"complete-account"}, {*ACCOUNT_CONFIRMATIONS, "x"}, None],
)
def test_confirmations_are_required_before_any_credential_or_get(setup, confirmations):
    values = setup[0]
    calls = []
    with pytest.raises(LiveAccountError, match="account_confirmations_required"):
        run(setup, calls, confirmations=confirmations)
    assert calls == [] and values[3].reads == []


def test_stopped_reads_refuse_before_loading_the_read_key(setup):
    values, live = setup[0], setup[1]
    live[1].stop("operator_stop")
    calls = []
    with pytest.raises(LiveAccountError, match="read_control_blocked"):
        run(setup, calls)
    assert calls == [] and values[3].reads == []


def test_external_position_is_a_ledger_discrepancy_that_halts(setup):
    _, live, _, order, _ = setup
    clock = setup[0][0]
    position = {
        "positionId": 900,
        "symbol": "USD_JPY",
        "side": "BUY",
        "size": "1000",
        "orderedSize": "0",
        "price": "150",
        "lossGain": "0",
        "totalSwap": "0",
        "timestamp": (clock.wall - timedelta(seconds=5)).isoformat(),
    }
    with pytest.raises(OrderBlocked, match="positions_mismatch"):
        run(setup, [], positions=[position], assets={"margin": "6000", "availableAmount": "994000"})
    assert live[3].snapshot()["halted"]


def test_unexplained_active_order_halts_before_the_gate_update(setup):
    _, live, _, order, _ = setup
    clock = setup[0][0]
    before = live[3].snapshot()["account_guard"]["last_proof"]
    foreign = {
        "rootOrderId": 777,
        "orderId": 777,
        "clientOrderId": "Manual01",
        "symbol": "USD_JPY",
        "side": "BUY",
        "orderType": "NORMAL",
        "executionType": "LIMIT",
        "settleType": "OPEN",
        "size": "1000",
        "price": "140",
        "status": "ORDERED",
        "timestamp": (clock.wall - timedelta(seconds=5)).isoformat(),
    }
    with pytest.raises(AccountDiscrepancy):
        run(setup, [], orders=[foreign])
    snapshot = live[3].snapshot()
    assert snapshot["halted"] and snapshot["account_guard"]["last_proof"] == before


def test_valuation_instant_difference_alone_is_refused_without_halting(setup):
    _, live, _, _, _ = setup
    before = live[3].snapshot()["account_guard"]["last_proof"]
    with pytest.raises(LiveAccountError, match="valuation_time_mismatch"):
        run(setup, [], assets={"equity": "1000001"})
    snapshot = live[3].snapshot()
    assert not snapshot["halted"] and snapshot["account_guard"]["last_proof"] == before


def test_balance_difference_is_recorded_and_halts(setup):
    _, live, _, _, _ = setup
    with pytest.raises(OrderBlocked, match="balance_mismatch"):
        run(setup, [], assets={"balance": "999000", "equity": "999000"})
    assert live[3].snapshot()["halted"]


def test_working_order_accepted_through_the_reviewed_path_is_reconciled(setup):
    values, live, _, order, _ = setup
    clock = values[0]
    with __import__("test_private_order").client(
        live, lambda request: response(live[0], request)
    ) as sender:
        sender.submit(order.client_id, quote=quote(clock.wall))
    live[3].reconcile(fixture_evidence(order, 101, 201, "ORDERED", [], clock.wall))
    wire = {
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
        "timestamp": clock.wall.isoformat(),
    }
    result = run(setup, [], orders=[wire])
    assert result["reconciled"] and result["working_orders"] == 1


def test_cli_requires_confirmations_and_hides_values(setup, capsys):
    values, live = setup[0], setup[1]
    with pytest.raises(SystemExit) as raised:
        live_account.main(
            [
                "--directory",
                str(live[3].path.parent),
                "--read-control-directory",
                str(live[1].path.parent),
                "--scope",
                "synthetic",
                "--credential-reference",
                values[5].plan.credential_reference,
            ]
        )
    assert raised.value.code == 2
    err = capsys.readouterr().err
    assert "account_confirmations_required" in err and "Traceback" not in err
    assert values[3].reads == []
