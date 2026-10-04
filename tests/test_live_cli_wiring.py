"""Success-path wiring of the live CLIs: arguments in, JSON out. Internals are replaced."""

import json
import socket
from datetime import UTC, datetime
from decimal import Decimal
from types import SimpleNamespace

import pytest

from trading import (
    live_account,
    live_order_sync,
    live_signal,
    order_credentials,
    order_discovery,
    order_runtime,
)
from trading.account_guard import AccountQuote
from trading.broker_contracts import OrderLimits

NOW = datetime(2026, 10, 5, 10, 0, 5, tzinfo=UTC)
STORES = ["--directory", "live", "--read-control-directory", "reads", "--scope", "synthetic"]


@pytest.fixture(autouse=True)
def no_network(monkeypatch):
    monkeypatch.setattr(socket, "socket", lambda *a, **k: pytest.fail("real network attempted"))


def quote_file(tmp_path):
    path = tmp_path / "quote.json"
    path.write_text(
        AccountQuote(bid="150", ask="150.01", observed_at=NOW, market_open=True).model_dump_json()
    )
    return path


class Recorder:
    def __init__(self, result):
        self.calls, self.result = [], result

    def factory(self, *args, **kwargs):
        self.calls.append(("init", args, kwargs))
        return self

    def __getattr__(self, name):
        def method(*args, **kwargs):
            self.calls.append((name, args, kwargs))
            return self.result

        return method


def test_order_runtime_submit_prints_receipt_ids(tmp_path, monkeypatch, capsys):
    recorder = Recorder(SimpleNamespace(root_order_id=101))
    monkeypatch.setattr(order_runtime, "OrderRuntime", recorder.factory)
    order_runtime.main(
        [
            "submit",
            *STORES,
            "--client-id",
            "Buy001",
            "--quote",
            str(quote_file(tmp_path)),
            "--expected-sha256",
            "a" * 64,
            "--credential-reference",
            "b" * 32,
            "--order-permission-confirmed",
        ]
    )
    printed = json.loads(capsys.readouterr().out)
    assert printed == {
        "operation": "submit",
        "client_id": "Buy001",
        "root_order_id": 101,
        "accepted": True,
        "network_used": True,
        "account_complete": False,
        "reconciliation_required": True,
    }
    name, args, kwargs = recorder.calls[-1]
    assert name == "dispatch" and args == ("Buy001",)
    assert kwargs["expected_sha256"] == "a" * 64 and kwargs["quote"].bid == 150
    assert kwargs["order_permission_confirmed"] is True and kwargs["operation"] == "submit"


def test_order_runtime_context_prints_the_checkpoint(tmp_path, monkeypatch, capsys):
    recorder = Recorder({"checkpoint_sha256": "c" * 64})
    monkeypatch.setattr(order_runtime, "OrderRuntime", recorder.factory)
    order_runtime.main(
        ["context", *STORES, "--client-id", "Buy001", "--quote", str(quote_file(tmp_path))]
    )
    assert json.loads(capsys.readouterr().out) == {
        "checkpoint_sha256": "c" * 64,
        "network_used": False,
    }


def test_live_account_passes_confirmations_tolerance_and_prints(tmp_path, monkeypatch, capsys):
    recorder = Recorder({"reconciled": True, "valuation_adjusted": False})
    monkeypatch.setattr(live_account, "LiveAccountRefresh", recorder.factory)
    live_account.main(
        [
            *STORES,
            "--credential-reference",
            "b" * 32,
            "--quote",
            str(quote_file(tmp_path)),
            "--valuation-tolerance",
            "0.05",
            "--confirm",
            "complete-account",
        ]
    )
    printed = json.loads(capsys.readouterr().out)
    assert printed["reconciled"] and printed["orders_sent"] is False
    name, args, kwargs = recorder.calls[-1]
    assert name == "refresh" and args == ("b" * 32,)
    assert kwargs["confirmations"] == ["complete-account"]
    assert kwargs["valuation_tolerance"] == "0.05" and kwargs["quote"].ask == Decimal("150.01")


def test_live_order_sync_prints_state(monkeypatch, capsys):
    recorder = Recorder({"client_id": "Buy001", "state": "FILLED"})
    monkeypatch.setattr(live_order_sync, "LiveOrderSync", recorder.factory)
    live_order_sync.main(
        [*STORES, "--client-id", "Buy001", "--credential-reference", "b" * 32, "--confirm", "x"]
    )
    printed = json.loads(capsys.readouterr().out)
    assert printed["state"] == "FILLED" and printed["network_used"] is True
    assert recorder.calls[-1][2]["confirmations"] == ["x"]


def test_order_discovery_prints_found_ids(monkeypatch, capsys):
    recorder = Recorder({"found": True, "order_id": 201})
    monkeypatch.setattr(order_discovery, "OrderDiscovery", recorder.factory)
    order_discovery.main(
        [
            *STORES,
            "--client-id",
            "Buy001",
            "--credential-reference",
            "b" * 32,
            "--read-only-confirmed",
        ]
    )
    assert json.loads(capsys.readouterr().out)["order_id"] == 201
    assert recorder.calls[-1][2]["read_only_confirmed"] is True


@pytest.mark.parametrize("flatten", [False, True])
def test_live_signal_writes_the_intent_and_skips_bars_when_flattening(
    tmp_path, monkeypatch, capsys, flatten
):
    from test_live_signal import RISING, bars

    from trading.account_guard import Position

    config = tmp_path / "fx.toml"
    config.write_text('market = "fx"\nsymbol = "USD_JPY"\nbar_seconds = 3600\nfast = 2\nslow = 4\n')
    limits = OrderLimits(
        min_units=1000,
        max_units=10000,
        unit_step=1000,
        price_tick="0.001",
        max_reference_notional="2000000",
    )
    held = (
        ()
        if not flatten
        else (Position(position_id=401, side="BUY", units=1000, average_price="150"),)
    )
    fetched = []
    monkeypatch.setattr(
        live_signal,
        "recent_bars",
        lambda cfg, now: fetched.append(now) or bars(RISING, end=now),
    )
    monkeypatch.setattr(
        live_signal, "PrivateOrderRecovery", lambda *a: SimpleNamespace(journal=None)
    )
    monkeypatch.setattr(live_signal, "journal_state", lambda journal, now: (held, False, limits))
    monkeypatch.setattr(live_signal, "entry_halted", lambda journal: False)
    now = datetime.now(UTC)
    current = tmp_path / "quote.json"
    current.write_text(
        AccountQuote(bid="150", ask="150.01", observed_at=now, market_open=True).model_dump_json()
    )
    args = [
        "--config",
        str(config),
        *STORES,
        "--quote",
        str(current),
        "--units",
        "1000",
        "--max-slippage",
        "0.02",
        "--output",
        str(tmp_path / "intent.json"),
    ]
    live_signal.main([*args, "--flatten"] if flatten else args)
    printed = json.loads(capsys.readouterr().out)
    assert printed["prepared"] is False and printed["orders_sent"] is False
    assert printed["action"] == ("close" if flatten else "open")
    assert json.loads((tmp_path / "intent.json").read_text())["client_id"].startswith(
        "F" if flatten else "S"
    )
    assert bool(fetched) is not flatten


def test_order_credentials_binding_is_local(monkeypatch, capsys):
    monkeypatch.setattr(
        order_credentials, "PrivateOrderRecovery", lambda *a: SimpleNamespace(journal="j")
    )
    monkeypatch.setattr(
        order_credentials.OrderCredentialVault,
        "binding",
        staticmethod(lambda journal: {"live_instance": "x" * 32}),
    )
    order_credentials.main(["binding", *STORES])
    assert json.loads(capsys.readouterr().out) == {
        "binding": {"live_instance": "x" * 32},
        "network_used": False,
        "broker_identity_verified": False,
    }


def test_order_runtime_submit_appends_the_reviewed_quote_to_the_dispatch_log(
    tmp_path, monkeypatch, capsys
):
    recorder = Recorder(SimpleNamespace(root_order_id=101))
    monkeypatch.setattr(order_runtime, "OrderRuntime", recorder.factory)
    log = tmp_path / "dispatch.jsonl"
    order_runtime.main(
        [
            "submit",
            *STORES,
            "--client-id",
            "Buy001",
            "--quote",
            str(quote_file(tmp_path)),
            "--expected-sha256",
            "a" * 64,
            "--credential-reference",
            "b" * 32,
            "--order-permission-confirmed",
            "--dispatch-log",
            str(log),
        ]
    )
    assert json.loads(capsys.readouterr().out)["accepted"]
    line = json.loads(log.read_text(encoding="utf-8"))
    assert line["client_id"] == "Buy001" and line["quote"]["ask"] == "150.01"
    assert line["checkpoint_sha256"] == "a" * 64


def test_live_signal_cli_refuses_an_unpromoted_strategy_before_reading(
    tmp_path, monkeypatch, capsys
):
    from test_live_cycle import live_ledger

    ledger = live_ledger(tmp_path, stage="paper")
    config = tmp_path / "fx.toml"
    config.write_text('market = "fx"\nsymbol = "USD_JPY"\nbar_seconds = 3600\nfast = 2\nslow = 4\n')
    monkeypatch.setattr(
        live_signal,
        "PrivateOrderRecovery",
        lambda *a: pytest.fail("journal opened before the promotion check"),
    )
    base = [
        "--config",
        str(config),
        *STORES,
        "--quote",
        str(quote_file(tmp_path)),
        "--units",
        "1000",
        "--max-slippage",
        "0.02",
        "--output",
        str(tmp_path / "intent.json"),
    ]
    with pytest.raises(SystemExit):
        live_signal.main([*base, "--ledger", str(ledger), "--hypothesis", "H001"])
    assert capsys.readouterr().err == "strategy_not_promoted_for_live\n"
    with pytest.raises(SystemExit):
        live_signal.main([*base, "--ledger", str(ledger)])
    assert capsys.readouterr().err == "ledger_and_hypothesis_required_together\n"


def test_context_can_fetch_and_save_the_quote_it_reviews(tmp_path, monkeypatch, capsys):
    from trading import live_quote

    fresh = AccountQuote(bid="151", ask="151.01", observed_at=NOW, market_open=True)
    monkeypatch.setattr(live_quote, "fetch_quote", lambda: fresh)
    recorder = Recorder({"checkpoint_sha256": "d" * 64})
    monkeypatch.setattr(order_runtime, "OrderRuntime", recorder.factory)
    saved = tmp_path / "quote.json"
    order_runtime.main(["context", *STORES, "--client-id", "Buy001", "--fetch-quote", str(saved)])
    assert json.loads(capsys.readouterr().out)["checkpoint_sha256"] == "d" * 64
    assert AccountQuote.model_validate_json(saved.read_text()) == fresh
    assert recorder.calls[-1][2]["quote"] == fresh
    with pytest.raises(SystemExit):
        order_runtime.main(
            [
                "submit",
                *STORES,
                "--client-id",
                "Buy001",
                "--fetch-quote",
                str(saved),
                "--expected-sha256",
                "a" * 64,
                "--credential-reference",
                "b" * 32,
                "--order-permission-confirmed",
            ]
        )
