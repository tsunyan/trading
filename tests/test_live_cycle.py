"""One operator cycle up to the reviewed send, with synthetic broker boundaries only."""

import ctypes
import json
import socket

import httpx
import pytest
from pydantic import SecretStr
from test_credential_store import MemoryBackend
from test_live_flow import FLAT, options, private_get, rising_bars
from test_live_flow import running as flow_running
from test_private_order import approval, response

from trading import live_cycle, live_setup
from trading.account_guard import AccountQuote
from trading.broker_contracts import OrderIntent
from trading.config import Settings
from trading.live_cycle import CYCLE_CONFIRMATIONS, LiveCycle, LiveCycleError
from trading.live_journal import CONFIRMATIONS
from trading.order_credentials import OrderCredentialVault
from trading.order_runtime import OrderRuntime, _quote

CFG = Settings(market="fx", symbol="USD_JPY", bar_seconds=3600, fast=2, slow=4)


@pytest.fixture(autouse=True)
def no_native_or_network(monkeypatch):
    def forbidden(*args, **kwargs):
        pytest.fail("real credentials/network attempted")

    monkeypatch.setattr(ctypes, "WinDLL", forbidden, raising=False)
    monkeypatch.setattr(socket, "socket", forbidden)


@pytest.fixture
def running(tmp_path):
    yield from flow_running.__wrapped__(tmp_path)


def cycle(running, tmp_path, *, prepare, transport=None, confirmations=CYCLE_CONFIRMATIONS):
    values, live, _ = running
    clock = values[0]
    clock.advance(1)
    quote = AccountQuote(bid="150.000", ask="150.010", observed_at=clock.wall, market_open=True)
    return LiveCycle(live[3].path.parent, live[1].path.parent, "synthetic", **options(clock)).run(
        values[5].plan.credential_reference,
        confirmations=confirmations,
        cfg=CFG,
        units=1000,
        max_slippage="0.02",
        prepare=prepare,
        bars=rising_bars(clock.wall),
        quote=quote,
        vault=values[4],
        transport=transport or private_get(clock),
        quote_output=tmp_path / "quote.json",
        intent_output=tmp_path / "intent.json",
    )


def activate(running):
    live = running[1]
    journal = live[3]
    live_setup.activate(
        journal,
        approval(journal, live[0]),
        expected_revision=journal.snapshot()["live_control"]["revision"],
        confirmations=CONFIRMATIONS,
    )


def test_cycle_proposes_then_prepares_and_the_printed_checkpoint_sends_once(running, tmp_path):
    values, live, _ = running
    journal = live[3]
    first = cycle(running, tmp_path, prepare=False)
    assert first["decision"]["action"] == "open" and first["prepared"] is False
    assert journal.snapshot()["orders"] == []
    intent = OrderIntent.model_validate_json((tmp_path / "intent.json").read_bytes())
    assert intent.side == "BUY" and intent.kind == "MARKET"

    activate(running)
    second = cycle(running, tmp_path, prepare=True)
    assert second["prepared"] and second["risk"]["allowed"]
    assert second["request"]["path"] == "/v1/order"
    assert journal.snapshot()["orders"][0]["state"] == "PREPARED"

    order_vault = OrderCredentialVault(MemoryBackend())
    key = order_vault.save(
        journal, SecretStr("order-key"), SecretStr("order-secret"), order_permission_confirmed=True
    )
    posts = []

    def broker(request):
        posts.append(request)
        return response(live[0], request)

    OrderRuntime(
        live[3].path.parent, live[1].path.parent, "synthetic", **options(values[0])
    ).dispatch(
        second["client_id"],
        expected_sha256=second["checkpoint_sha256"],
        credential_reference=key,
        quote=_quote(tmp_path / "quote.json"),
        order_permission_confirmed=True,
        vault=order_vault,
        transport=httpx.MockTransport(broker),
    )
    assert len(posts) == 1 and journal.snapshot()["orders"][0]["state"] == "RECONCILING"


def test_next_cycle_reconciles_the_accepted_order_and_holds_at_target(running, tmp_path):
    values, live, _ = running
    clock, journal = values[0], live[3]
    cycle(running, tmp_path, prepare=False)
    activate(running)
    prepared = cycle(running, tmp_path, prepare=True)
    order_vault = OrderCredentialVault(MemoryBackend())
    key = order_vault.save(
        journal, SecretStr("order-key"), SecretStr("order-secret"), order_permission_confirmed=True
    )
    OrderRuntime(live[3].path.parent, live[1].path.parent, "synthetic", **options(clock)).dispatch(
        prepared["client_id"],
        expected_sha256=prepared["checkpoint_sha256"],
        credential_reference=key,
        quote=_quote(tmp_path / "quote.json"),
        order_permission_confirmed=True,
        vault=order_vault,
        transport=httpx.MockTransport(lambda request: response(live[0], request)),
    )
    filled_at = clock.wall.isoformat()
    identity = {
        "clientOrderId": prepared["client_id"],
        "symbol": "USD_JPY",
        "side": "BUY",
        "settleType": "OPEN",
    }
    order = {
        **identity,
        "rootOrderId": 101,
        "orderId": 201,
        "orderType": "NORMAL",
        "executionType": "MARKET",
        "size": "1000",
        "status": "EXECUTED",
        "timestamp": filled_at,
    }
    fill = {
        **identity,
        "executionId": 301,
        "positionId": 401,
        "orderId": 201,
        "size": "1000",
        "price": "150.01",
        "amount": "-3",
        "fee": "-3",
        "lossGain": "0",
        "settledSwap": "0",
        "timestamp": filled_at,
    }
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
    result = cycle(
        running,
        tmp_path,
        prepare=True,
        transport=private_get(clock, assets=assets, positions=[held], orders=[order], fills=[fill]),
    )
    assert result["reconciled_orders"] == [{"client_id": prepared["client_id"], "state": "FILLED"}]
    assert result["account"]["positions"] == 1
    assert result["decision"]["reason"] == "at_target" and result["prepared"] is False


@pytest.mark.parametrize(
    "confirmations",
    [set(), CYCLE_CONFIRMATIONS - {"complete-history"}, {*CYCLE_CONFIRMATIONS, "x"}],
)
def test_cycle_confirmations_are_required_before_any_read(running, tmp_path, confirmations):
    values = running[0]
    with pytest.raises(LiveCycleError, match="cycle_confirmations_required"):
        cycle(running, tmp_path, prepare=False, confirmations=confirmations)
    assert values[3].reads == []


def test_cli_failure_reports_only_a_fixed_reason(running, tmp_path, capsys):
    values, live, _ = running
    config = tmp_path / "fx.toml"
    config.write_text('market = "fx"\nsymbol = "USD_JPY"\nbar_seconds = 3600\nfast = 2\nslow = 4\n')
    with pytest.raises(SystemExit) as raised:
        live_cycle.main(
            [
                "--config",
                str(config),
                "--directory",
                str(live[3].path.parent),
                "--read-control-directory",
                str(live[1].path.parent),
                "--scope",
                "synthetic",
                "--credential-reference",
                values[5].plan.credential_reference,
                "--units",
                "1000",
                "--max-slippage",
                "0.02",
                "--quote-output",
                str(tmp_path / "quote.json"),
            ]
        )
    assert raised.value.code == 2
    assert capsys.readouterr().err == "live_cycle_failed: cycle_confirmations_required\n"
    assert values[3].reads == [] and json.dumps({}) == "{}"
