"""Public ticker to order-review quote with synthetic HTTP only."""

import json
import socket
from datetime import UTC, datetime
from decimal import Decimal

import httpx
import pytest

from trading import live_quote
from trading.account_guard import AccountQuote
from trading.live_quote import (
    LiveQuoteError,
    fetch_quote,
    fetch_status,
    parse_status,
    parse_ticker,
    write_quote,
)

NOW = datetime(2026, 10, 5, 1, 0, tzinfo=UTC)


@pytest.fixture(autouse=True)
def no_network(monkeypatch):
    monkeypatch.setattr(socket, "socket", lambda *a, **k: pytest.fail("real network attempted"))


def row(**updates):
    return {
        "symbol": "USD_JPY",
        "bid": "150.123",
        "ask": "150.126",
        "timestamp": "2026-10-05T00:59:59.123Z",
        "status": "OPEN",
        **updates,
    }


def body(*rows, status=0):
    return json.dumps({"status": status, "data": list(rows), "responsetime": "x"}).encode()


def transport(calls, content=None, **response):
    def handler(request):
        calls.append(request)
        return httpx.Response(
            response.get("status_code", 200),
            content=content if content is not None else body(row(symbol="EUR_JPY"), row()),
            headers={"content-type": response.get("content_type", "application/json")},
        )

    return httpx.MockTransport(handler)


def test_exact_decimal_quote_from_one_unauthenticated_get():
    calls = []
    quote = fetch_quote(transport=transport(calls), clock=lambda: NOW)
    assert quote == AccountQuote(
        bid="150.123",
        ask="150.126",
        observed_at=datetime(2026, 10, 5, 0, 59, 59, 123000, tzinfo=UTC),
        market_open=True,
    )
    assert quote.bid == Decimal("150.123") and quote.ask - quote.bid == Decimal("0.003")
    assert len(calls) == 1 and calls[0].method == "GET"
    assert str(calls[0].url) == "https://forex-api.coin.z.com/public/v1/ticker"
    assert not {"api-key", "api-sign", "api-timestamp"} & set(calls[0].headers)


def test_closed_market_is_kept_for_the_risk_gate_not_hidden():
    quote = parse_ticker(body(row(status="CLOSE")), received_at=NOW)
    assert quote.market_open is False


@pytest.mark.parametrize(
    "content",
    [
        body(),
        body(row(), row()),
        body(row(), status=5),
        body(row(bid=150.123)),
        body(row(bid="1.5e2")),
        body(row(bid="150.2", ask="150.1")),
        body(row(status="MAINTENANCE")),
        body(row(timestamp="2026-10-05T00:59:59")),
        body({k: v for k, v in row().items() if k != "ask"}),
        b'{"status":0,"status":0,"data":[]}',
        b"not json",
    ],
)
def test_malformed_ticker_is_refused_without_echo(content):
    with pytest.raises(LiveQuoteError, match="invalid_public_ticker") as raised:
        parse_ticker(content, received_at=NOW)
    assert "150" not in str(raised.value)


def test_future_ticker_beyond_small_skew_is_refused():
    parse_ticker(body(row(timestamp="2026-10-05T01:00:02Z")), received_at=NOW)
    with pytest.raises(LiveQuoteError, match="public_ticker_from_future"):
        parse_ticker(body(row(timestamp="2026-10-05T01:00:03Z")), received_at=NOW)


@pytest.mark.parametrize(
    ("options", "reason"),
    [
        ({"status_code": 503}, "unexpected_public_ticker_status"),
        ({"status_code": 302}, "unexpected_public_ticker_status"),
        ({"content_type": "text/html"}, "expected_public_ticker_json"),
        ({"content": b"x" * (live_quote.MAX_TICKER_BYTES + 1)}, "public_ticker_too_large"),
    ],
)
def test_transport_refusals(options, reason):
    calls = []
    with pytest.raises(LiveQuoteError, match=reason):
        fetch_quote(transport=transport(calls, **options), clock=lambda: NOW)
    assert len(calls) == 1


def test_transport_error_and_slow_body_are_refused():
    def broken(request):
        raise httpx.ConnectError("down")

    with pytest.raises(LiveQuoteError, match="public_ticker_unavailable"):
        fetch_quote(transport=httpx.MockTransport(broken), clock=lambda: NOW)
    ticks = iter([0, 6, 6])
    with pytest.raises(LiveQuoteError, match="public_ticker_deadline_exceeded"):
        fetch_quote(transport=transport([]), clock=lambda: NOW, monotonic=lambda: next(ticks))


def test_written_file_is_exactly_what_the_order_runtime_accepts(tmp_path):
    from trading.order_runtime import _quote

    quote = parse_ticker(body(row()), received_at=NOW)
    path = tmp_path / "quote.json"
    path.write_text("old")
    write_quote(quote, path)
    assert _quote(path) == quote
    assert [p.name for p in tmp_path.iterdir()] == ["quote.json"]


def test_cli_writes_quote_and_hides_failures(tmp_path, monkeypatch, capsys):
    quote = parse_ticker(body(row()), received_at=NOW)
    monkeypatch.setattr(live_quote, "fetch_quote", lambda: quote)
    live_quote.main(["--output", str(tmp_path / "q.json")])
    printed = json.loads(capsys.readouterr().out)
    assert printed["network_used"] and not printed["credentials_used"]
    assert AccountQuote.model_validate_json((tmp_path / "q.json").read_text()) == quote

    def failed():
        raise LiveQuoteError("public_ticker_unavailable")

    monkeypatch.setattr(live_quote, "fetch_quote", failed)
    with pytest.raises(SystemExit) as raised:
        live_quote.main(["--output", str(tmp_path / "q.json")])
    assert raised.value.code == 2
    assert "public_ticker_unavailable" in capsys.readouterr().err
    assert AccountQuote.model_validate_json((tmp_path / "q.json").read_text()) == quote
    with pytest.raises(SystemExit):
        live_quote.main(["--output", str(tmp_path / "missing" / "q.json")])


@pytest.mark.parametrize("state", ["OPEN", "CLOSE", "MAINTENANCE"])
def test_service_status_is_read_from_the_fixed_public_endpoint(state):
    calls = []

    def handler(request):
        calls.append(str(request.url))
        payload = {"status": 0, "data": {"status": state}, "responsetime": "x"}
        return httpx.Response(200, json=payload)

    assert fetch_status(transport=httpx.MockTransport(handler)) == state
    assert calls == ["https://forex-api.coin.z.com/public/v1/status"]


@pytest.mark.parametrize(
    "content",
    [
        b"{}",
        b'{"status": 1, "data": {"status": "OPEN"}}',
        b'{"status": 0, "data": {"status": "UNKNOWN"}}',
        b'{"status": 0, "data": {"status": "OPEN", "status": "CLOSE"}}',
    ],
)
def test_malformed_service_status_is_refused(content):
    with pytest.raises(LiveQuoteError, match="invalid_public_service_status"):
        parse_status(content)


def test_service_status_transport_failure_is_fixed_reason():
    def broken(request):
        raise httpx.ConnectError("x")

    with pytest.raises(LiveQuoteError, match="public_service_unavailable"):
        fetch_status(transport=httpx.MockTransport(broken))
