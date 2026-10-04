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


@pytest.mark.parametrize("state", ["RECONCILING", "UNKNOWN", "SUBMITTING"])
def test_own_unreconciled_order_is_refused_without_halting(state):
    from test_account_guard import NOW

    order = intent()
    row = {**working_row(order, NOW), "state": state}
    with pytest.raises(LiveAccountError, match="local_order_reconciliation_required") as raised:
        snapshot_from_report(report(NOW, orders=(active(order, NOW),)), account_id="a", rows=[row])
    assert not isinstance(raised.value, AccountDiscrepancy)


def test_refresh_right_after_acceptance_asks_for_order_reconciliation_first(setup):
    _, live, _, order, _ = setup
    clock = setup[0][0]
    with __import__("test_private_order").client(
        live, lambda request: response(live[0], request)
    ) as sender:
        sender.submit(order.client_id, quote=quote(clock.wall))
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
        "timestamp": (clock.wall - timedelta(seconds=1)).isoformat(),
    }
    with pytest.raises(LiveAccountError, match="local_order_reconciliation_required"):
        run(setup, [], orders=[wire])
    assert not live[3].snapshot()["halted"]


@pytest.mark.parametrize("value", ["0", "-0.01", "1.01", "nan", "inf", "x", True])
def test_valuation_tolerance_must_be_a_small_positive_jpy_per_unit(value):
    with pytest.raises(LiveAccountError, match="invalid_valuation_tolerance"):
        live_account._valuation_tolerance(value)
    assert live_account._valuation_tolerance(None) is None


def test_revalue_marks_at_the_ticker_and_never_raises_available_margin():
    from test_account_guard import NOW, account, policy
    from test_account_guard import quote as guard_quote

    from trading.account_guard import Position

    held = (Position(position_id=401, side="BUY", units=1000, average_price="150.01"),)
    broker = account(
        NOW,
        balance="999997",
        equity="999985",
        required_margin="6000.4",
        available_margin="993984.6",
        positions=held,
    )
    marked = guard_quote(NOW)  # bid 150 -> ticker equity 999987.
    revalued = live_account.revalue(broker, marked, tolerance=Decimal("0.01"), policy=policy())
    assert revalued.equity == Decimal("999987")
    assert revalued.available_margin == Decimal("993984.6")
    lower = guard_quote(NOW, bid="149.99", ask="150")  # ticker equity 999977.
    revalued = live_account.revalue(broker, lower, tolerance=Decimal("0.01"), policy=policy())
    assert revalued.equity == Decimal("999977")
    assert revalued.available_margin == Decimal("993976.6")
    with pytest.raises(LiveAccountError, match="valuation_outside_tolerance"):
        live_account.revalue(broker, marked, tolerance=Decimal("0.001"), policy=policy())


def test_revalue_invariants_over_random_positions_and_quotes():
    import random

    from test_account_guard import NOW, policy
    from test_account_guard import account as guard_account
    from test_account_guard import quote as guard_quote

    from trading.account_guard import Position, marked_equity

    rng = random.Random(4)
    for _ in range(300):
        lots = tuple(
            Position(
                position_id=400 + i,
                side=rng.choice(["BUY", "SELL"]),
                units=rng.choice([1000, 5000]),
                average_price=str(Decimal(14900 + rng.randint(0, 200)) / 100),
            )
            for i in range(rng.randint(1, 3))
        )
        bid = Decimal(14900 + rng.randint(0, 200)) / 100
        current = guard_quote(NOW, bid=str(bid), ask=str(bid + Decimal("0.01")))
        base = guard_account(NOW, positions=lots)
        local = marked_equity(base, current)
        broker_equity = local + Decimal(rng.randint(-3000, 3000)) / 100
        margin = Decimal(rng.randint(0, 50000))
        available = max(broker_equity - margin, Decimal(0))
        broker = base.model_copy(
            update={
                "equity": broker_equity,
                "required_margin": margin,
                "available_margin": available,
            }
        )
        units = sum(p.units for p in lots)
        tolerance = Decimal(rng.choice(["0.001", "0.005", "0.01"]))
        allowed = abs(broker_equity - local) <= tolerance * units + policy().tolerance_jpy
        if not allowed:
            with pytest.raises(LiveAccountError, match="valuation_outside_tolerance"):
                live_account.revalue(broker, current, tolerance=tolerance, policy=policy())
            continue
        revalued = live_account.revalue(broker, current, tolerance=tolerance, policy=policy())
        assert revalued.equity == local
        assert revalued.available_margin <= broker.available_margin
        assert revalued.available_margin <= max(local - margin, Decimal(0))


def unknown_submission(setup):
    _, live, _, order, _ = setup
    clock = setup[0][0]
    with __import__("test_private_order").client(live, lambda _: httpx.Response(500)) as sender:
        with pytest.raises(ValueError):
            sender.submit(order.client_id, quote=quote(clock.wall))
    return order


def absent(setup, order, **broker_options):
    values = setup[0]
    clock = values[0]
    clock.advance(301)
    return refresher(setup).refresh(
        values[5].plan.credential_reference,
        confirmations=ACCOUNT_CONFIRMATIONS,
        quote=quote(clock.wall),
        vault=values[4],
        transport=broker(clock, [], **broker_options),
        absent_order=order.client_id,
    )


def test_absent_order_observation_is_recorded_without_updating_the_gate(setup):
    live = setup[1]
    order = unknown_submission(setup)
    gate_before = live[3].snapshot()
    result = absent(setup, order)
    assert result["account_shows_no_effect"] and not result["absence_proven"]
    assert live[3].order_absence_context(order.client_id)["absent_state"] == "UNKNOWN"
    after = live[3].snapshot()
    assert after["orders"] == gate_before["orders"] and after["halted"]


def test_absent_order_found_active_is_refused_before_any_record(setup):
    live = setup[1]
    order = unknown_submission(setup)
    clock = setup[0][0]
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
    with pytest.raises(LiveAccountError, match="unknown_order_is_active"):
        absent(setup, order, orders=[wire])
    with pytest.raises(ValueError, match="absence_account_required"):
        live[3].order_absence_context(order.client_id)


def test_absent_order_with_a_position_is_refused(setup):
    live = setup[1]
    order = unknown_submission(setup)
    clock = setup[0][0]
    held = {
        "positionId": 401,
        "symbol": "USD_JPY",
        "side": "BUY",
        "size": "1000",
        "orderedSize": "0",
        "price": "150.01",
        "lossGain": "0",
        "totalSwap": "0",
        "timestamp": clock.wall.isoformat(),
    }
    with pytest.raises(ValueError, match="complete_account_required"):
        absent(
            setup,
            order,
            positions=[held],
            assets={"margin": "6000", "availableAmount": "994000"},
        )
    with pytest.raises(ValueError, match="absence_account_required"):
        live[3].order_absence_context(order.client_id)


def test_swap_diagnostic_is_reported_without_affecting_the_reconciliation(setup):
    from test_swap_check import schedule

    values = setup[0]
    clock = values[0]
    clock.advance(1)
    result = refresher(setup).refresh(
        values[5].plan.credential_reference,
        confirmations=ACCOUNT_CONFIRMATIONS,
        quote=quote(clock.wall),
        vault=values[4],
        transport=broker(clock, []),
        swap_schedule=schedule(),
    )
    assert result["reconciled"]
    assert result["swap_check"]["positions"] == 0 and result["swap_check"]["diagnostic_only"]
