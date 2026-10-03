"""The documented live runbook composed end to end, with synthetic broker boundaries only.

Account refresh, strategy proposal, preparation, activation, reviewed send with a stored
order key, GET reconciliation of the accepted order and a second account refresh.
"""

import ctypes
import socket

import httpx
import pandas as pd
import pytest
from pydantic import SecretStr
from test_credential_store import MemoryBackend
from test_live_operations import bind, watchdog
from test_live_operations import unbound as operations_unbound
from test_private_operations import begin
from test_private_order import approval, response

from trading import live_setup
from trading.account_guard import AccountQuote
from trading.config import Settings
from trading.live_account import ACCOUNT_CONFIRMATIONS, LiveAccountRefresh
from trading.live_journal import CONFIRMATIONS
from trading.live_order_sync import HISTORY_CONFIRMATIONS, LiveOrderSync
from trading.live_signal import decide, journal_state
from trading.order_credentials import OrderCredentialVault
from trading.order_runtime import OrderRuntime


@pytest.fixture(autouse=True)
def no_native_or_network(monkeypatch):
    def forbidden(*args, **kwargs):
        pytest.fail("real credentials/network attempted")

    monkeypatch.setattr(ctypes, "WinDLL", forbidden, raising=False)
    monkeypatch.setattr(socket, "socket", forbidden)


@pytest.fixture
def running(tmp_path):
    """Registered journal with a healthy original sync and watchdog; nothing prepared."""
    unbound = operations_unbound.__wrapped__(tmp_path)
    values, live, monitor = unbound
    bind(unbound)
    lease = values[5].control.ownership()
    lease.__enter__()
    try:
        begin(values[5])
        watchdog(monitor)
        values[0].advance(1)
        values[5].control.update(values[5].control.snapshot()["owner"], success=True)
        watchdog(monitor)
        yield values, live, monitor
    finally:
        lease.__exit__(None, None, None)


def options(clock):
    elapsed = [0]

    def sleep(seconds):
        clock.advance(seconds)
        elapsed[0] += round(seconds * 1e9)

    return {
        "clock": lambda: clock.wall,
        "monotonic": lambda: clock.mono,
        "read_clocks": {
            "wall_ns": lambda: int(clock.wall.timestamp() * 1e9),
            "monotonic_ns": lambda: elapsed[0],
            "sleep": sleep,
        },
        "post_sleep": clock.advance,
    }


FLAT = {
    "balance": "1000000",
    "equity": "1000000",
    "availableAmount": "1000000",
    "margin": "0",
    "estimatedTradeFee": "0",
    "positionLossGain": "0",
    "totalSwap": "0",
    "transferableAmount": "1000000",
}


def private_get(clock, *, assets=FLAT, positions=(), orders=(), fills=()):
    def handler(request):
        assert request.method == "GET"
        path, first = request.url.path, "prevId" not in request.url.params
        if path.endswith("/account/assets"):
            data = [dict(assets)]
        elif path.endswith("/openPositions"):
            data = {"list": list(positions) if first else []}
        elif path.endswith("/activeOrders"):
            data = {"list": []}
        elif path.endswith("/orders"):
            data = {"list": list(orders)}
        else:
            data = {"list": list(fills)}
        return httpx.Response(
            200, json={"status": 0, "data": data, "responsetime": clock.wall.isoformat()}
        )

    return httpx.MockTransport(handler)


def rising_bars(now):
    start = pd.Timestamp(now).floor("h") - pd.Timedelta(hours=6)
    closes = [149.0, 149.1, 149.2, 149.3, 149.5, 149.8]
    return pd.DataFrame(
        {
            "timestamp": [start + pd.Timedelta(hours=i) for i in range(len(closes))],
            "symbol": "USD_JPY",
            "open": closes,
            "high": [c + 0.05 for c in closes],
            "low": [c - 0.05 for c in closes],
            "close": closes,
            "volume": 0,
        }
    )


def test_documented_runbook_runs_from_account_proof_to_a_filled_and_held_position(running):
    values, live, _ = running
    clock = values[0]
    directories = (live[3].path.parent, live[1].path.parent, "synthetic")
    read_key, read_vault = values[5].plan.credential_reference, values[4]

    def current():
        return AccountQuote(bid="150.000", ask="150.010", observed_at=clock.wall, market_open=True)

    # 1. Read-only account proof for a flat account.
    clock.advance(1)
    refreshed = LiveAccountRefresh(*directories, **options(clock)).refresh(
        read_key,
        confirmations=ACCOUNT_CONFIRMATIONS,
        quote=current(),
        vault=read_vault,
        transport=private_get(clock),
    )
    assert refreshed["reconciled"] and refreshed["positions"] == 0

    # 2. Strategy proposal from completed bars and the proof's holdings.
    journal = live[3]
    positions, pending, limits = journal_state(journal, clock.wall)
    cfg = Settings(market="fx", symbol="USD_JPY", bar_seconds=3600, fast=2, slow=4)

    def propose():
        return decide(
            rising_bars(clock.wall),
            current(),
            cfg,
            positions=positions,
            pending=journal_state(journal, clock.wall)[1],
            units=1000,
            max_slippage="0.02",
            limits=limits,
            now=clock.wall,
        )

    decision = propose()
    intent = decision["intent"]
    assert decision["action"] == "open" and intent.kind == "MARKET" and intent.side == "BUY"

    # 3. Prepare and activate through the operational helper (registered journal).
    journal.prepare(intent)
    enabled = live_setup.activate(
        journal,
        approval(journal, live[0]),
        expected_revision=journal.snapshot()["live_control"]["revision"],
        confirmations=CONFIRMATIONS,
    )
    assert enabled["live_enabled"] and enabled["orders"][0]["state"] == "PREPARED"

    # 4. Order key in its own namespace, reviewed context, explicit dispatch.
    order_vault = OrderCredentialVault(MemoryBackend())
    order_key = order_vault.save(
        journal, SecretStr("order-key"), SecretStr("order-secret"), order_permission_confirmed=True
    )
    runtime = OrderRuntime(*directories, **options(clock))
    sent_quote = current()
    context = runtime.context(intent.client_id, quote=sent_quote)
    assert context["path"] == "/v1/order" and context["body"]["executionType"] == "MARKET"
    posts = []

    def broker(request):
        posts.append(request)
        return response(live[0], request, status="WAITING")

    receipt = runtime.dispatch(
        intent.client_id,
        expected_sha256=context["checkpoint_sha256"],
        credential_reference=order_key,
        quote=sent_quote,
        order_permission_confirmed=True,
        vault=order_vault,
        transport=httpx.MockTransport(broker),
    )
    assert len(posts) == 1 and receipt.root_order_id == 101
    assert journal.snapshot()["orders"][0]["state"] == "RECONCILING"
    assert propose()["reason"] == "unsettled_local_order"

    # 5. GET reconciliation of the accepted market order from its saved broker ID.
    filled_at = clock.wall.isoformat()
    wire = {
        "rootOrderId": 101,
        "orderId": 201,
        "clientOrderId": intent.client_id,
        "symbol": "USD_JPY",
        "side": "BUY",
        "settleType": "OPEN",
        "orderType": "NORMAL",
        "executionType": "MARKET",
        "size": "1000",
        "status": "EXECUTED",
        "timestamp": filled_at,
    }
    fill = {
        "executionId": 301,
        "positionId": 401,
        "orderId": 201,
        "clientOrderId": intent.client_id,
        "symbol": "USD_JPY",
        "side": "BUY",
        "settleType": "OPEN",
        "size": "1000",
        "price": "150.01",
        "amount": "-3",
        "fee": "-3",
        "lossGain": "0",
        "settledSwap": "0",
        "timestamp": filled_at,
    }
    clock.advance(1)
    synced = LiveOrderSync(*directories, **options(clock)).reconcile(
        intent.client_id,
        credential_reference=read_key,
        confirmations=HISTORY_CONFIRMATIONS,
        vault=read_vault,
        transport=private_get(clock, orders=[wire], fills=[fill]),
    )
    assert synced["state"] == "FILLED"

    # 6. The next account proof holds the position; the strategy is then at target.
    held = {
        "positionId": 401,
        "symbol": "USD_JPY",
        "side": "BUY",
        "size": "1000",
        "orderedSize": "0",
        "price": "150.01",
        "lossGain": "-10",
        "totalSwap": "0",
        "timestamp": filled_at,
    }
    assets = {
        **FLAT,
        "balance": "999997",
        "equity": "999987",
        "availableAmount": "993986.6",
        "margin": "6000.4",
        "positionLossGain": "-10",
        "transferableAmount": "993986.6",
    }
    clock.advance(1)
    refreshed = LiveAccountRefresh(*directories, **options(clock)).refresh(
        read_key,
        confirmations=ACCOUNT_CONFIRMATIONS,
        quote=current(),
        vault=read_vault,
        transport=private_get(clock, assets=assets, positions=[held]),
    )
    assert refreshed["reconciled"] and refreshed["positions"] == 1
    positions = journal_state(journal, clock.wall)[0]
    assert propose()["reason"] == "at_target"
    assert not journal.snapshot()["halted"] and len(posts) == 1
