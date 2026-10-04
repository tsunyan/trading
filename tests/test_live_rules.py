"""Live order limits against public USD_JPY trading rules, with synthetic HTTP only."""

import json
import socket

import httpx
import pytest
from test_account_guard import policy

from trading import live_rules
from trading.broker_contracts import OrderLimits
from trading.live_rules import LiveRulesError, conflicts, fetch_rules, parse_rules


@pytest.fixture(autouse=True)
def no_network(monkeypatch):
    monkeypatch.setattr(socket, "socket", lambda *a, **k: pytest.fail("real network attempted"))


def row(**changes):
    return {
        "symbol": "USD_JPY",
        "minOpenOrderSize": "1000",
        "maxOrderSize": "500000",
        "sizeStep": "1000",
        "tickSize": "0.001",
        **changes,
    }


def body(*rows):
    return json.dumps({"status": 0, "data": list(rows)}).encode()


def limits(**changes):
    values = dict(
        min_units=1000,
        max_units=10000,
        unit_step=1000,
        price_tick="0.001",
        max_reference_notional="2000000",
    )
    return OrderLimits(**{**values, **changes})


def test_fetch_parses_exact_rules_from_one_get():
    calls = []

    def handler(request):
        calls.append(request)
        return httpx.Response(
            200,
            content=body(row(symbol="EUR_JPY"), row()),
            headers={"content-type": "application/json"},
        )

    rules = fetch_rules(transport=httpx.MockTransport(handler))
    assert rules["min_open_order_size"] == 1000 and str(rules["tick_size"]) == "0.001"
    assert len(calls) == 1 and str(calls[0].url).endswith("/public/v1/symbols")


@pytest.mark.parametrize(
    "content",
    [body(), body(row(), row()), body(row(sizeStep=1000)), body(row(tickSize="0")), b"{"],
)
def test_malformed_rules_are_refused(content):
    with pytest.raises(LiveRulesError, match="invalid_public_symbols"):
        parse_rules(content)


@pytest.mark.parametrize(
    ("change", "problem"),
    [
        ({"min_units": 500, "unit_step": 500}, "min_units_below_broker_minimum"),
        ({"max_units": 600000}, "max_units_above_broker_maximum"),
        ({"min_units": 1500, "unit_step": 500}, "unit_step_not_multiple_of_broker_step"),
        ({"price_tick": "0.0005"}, "price_tick_not_multiple_of_broker_tick"),
    ],
)
def test_each_conflict_is_named(change, problem):
    rules = parse_rules(body(row()))
    assert conflicts(limits(), rules) == []
    assert problem in conflicts(limits(**change), rules)


def test_cli_checks_a_config_and_saves_immutable_evidence(tmp_path, monkeypatch, capsys):
    config = tmp_path / "live.json"
    config.write_text(
        json.dumps(
            {
                "limits": limits(max_units=600000).model_dump(mode="json"),
                "policy": policy().model_dump(mode="json"),
            }
        )
    )
    monkeypatch.setattr(live_rules, "fetch_rules", lambda: parse_rules(body(row())))
    output = tmp_path / "rules.json"
    code = live_rules.main(["check", "--config", str(config), "--output", str(output)])
    printed = json.loads(capsys.readouterr().out)
    assert code == 1 and printed["conflicts"] == ["max_units_above_broker_maximum"]
    assert json.loads(output.read_text()) == printed
    with pytest.raises(SystemExit):
        live_rules.main(["check", "--config", str(config), "--output", str(output)])
    assert "live_rules_failed" in capsys.readouterr().err
    with pytest.raises(SystemExit):
        live_rules.main(["check"])
    assert "config_or_journal_required" in capsys.readouterr().err


@pytest.mark.parametrize(
    ("response", "reason"),
    [
        (httpx.Response(503), "unexpected_public_symbols_status"),
        (
            httpx.Response(200, content=b"<html>", headers={"content-type": "text/html"}),
            "expected_public_symbols_json",
        ),
        (
            httpx.Response(
                200,
                content=b"x" * (live_rules.MAX_SYMBOLS_BYTES + 1),
                headers={"content-type": "application/json"},
            ),
            "public_symbols_too_large",
        ),
    ],
)
def test_fetch_refusals(response, reason):
    with pytest.raises(LiveRulesError, match=reason):
        fetch_rules(transport=httpx.MockTransport(lambda request: response))


def test_transport_error_and_slow_body():
    def down(request):
        raise httpx.ConnectError("down")

    with pytest.raises(LiveRulesError, match="public_symbols_unavailable"):
        fetch_rules(transport=httpx.MockTransport(down))
    ticks = iter([0, 6])
    ok = httpx.Response(200, content=body(row()), headers={"content-type": "application/json"})
    with pytest.raises(LiveRulesError, match="public_symbols_deadline_exceeded"):
        fetch_rules(transport=httpx.MockTransport(lambda r: ok), monotonic=lambda: next(ticks))
