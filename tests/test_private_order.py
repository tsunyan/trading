"""Enabled dedicated journal -> current risk claim -> one real mocked HTTP POST."""

import hashlib
import hmac
import json
import socket
import sqlite3
import subprocess
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta

import httpx
import pytest
from pydantic import SecretStr
from test_account_guard import NOW, account, fill, intent, policy, quote

from trading.account_guard import Position
from trading.broker_contracts import OrderLimits, Settlement
from trading.execution_lab import fixture_evidence
from trading.live_journal import (
    CONFIRMATIONS,
    EVIDENCE_KINDS,
    AcceptanceEvidence,
    LiveApproval,
    LiveOrderError,
    LiveOrderJournal,
)
from trading.order_journal import OrderBlocked, OrderJournal
from trading.paper_runner import python_process_args
from trading.post_control import PersistentPostLimiter, PostControlError
from trading.private_order import OrderTransportError, PrivateOrderClient
from trading.read_control import PersistentReadLimiter


@pytest.fixture(autouse=True)
def no_network(monkeypatch):
    monkeypatch.setattr(socket, "socket", lambda *a, **k: pytest.fail("real network attempted"))


class Clock:
    def __init__(self):
        self.now, self.mono = NOW, 0.0

    def advance(self, seconds):
        self.now += timedelta(seconds=seconds)
        self.mono += seconds

    def post_args(self):
        return {
            "wall_ns": lambda: int(self.now.timestamp() * 1e9),
            "monotonic": lambda: self.mono,
            "sleep": self.advance,
        }


@pytest.fixture
def setup(tmp_path):
    clock = Clock()
    reads = PersistentReadLimiter.create(tmp_path / "reads", "synthetic")
    posts = PersistentPostLimiter.create(tmp_path / "posts", reads, **clock.post_args())
    limits = OrderLimits(
        min_units=100,
        max_units=1000,
        unit_step=100,
        price_tick="0.001",
        max_reference_notional="200000",
    )
    journal = LiveOrderJournal.create(
        tmp_path / "live", posts, limits, policy(), clock=lambda: clock.now
    )
    return clock, reads, posts, journal


def approval(journal, clock):
    state = journal.activation_context()
    return LiveApproval(
        account_id="fixture-account",
        configuration_sha256=state["configuration_sha256"],
        implementation_sha256=state["implementation_sha256"],
        accepted_at=clock.now,
        expires_at=clock.now + timedelta(hours=1),
        evidence=tuple(
            AcceptanceEvidence(kind=kind, reference=f"synthetic-{kind}", sha256="a" * 64)
            for kind in sorted(EVIDENCE_KINDS)
        ),
    )


def enable(journal, clock):
    journal.activate(
        approval(journal, clock),
        expected_revision=journal.snapshot()["live_control"]["revision"],
        confirmations=CONFIRMATIONS,
        now=clock.now,
    )


def ready(setup, order=None):
    clock, _, _, journal = setup
    order = order or intent()
    journal.prepare(order)
    journal.update_account(account(clock.now), quote(clock.now), now=clock.now)
    enable(journal, clock)
    return order


def response(clock, request, status="WAITING"):
    body = json.loads(request.content)
    row = {
        "rootOrderId": 111 if request.url.path.endswith("/closeOrder") else 101,
        "orderId": 211 if request.url.path.endswith("/closeOrder") else 201,
        "clientOrderId": body["clientOrderId"],
        "symbol": body["symbol"],
        "side": body["side"],
        "orderType": "NORMAL",
        "executionType": body["executionType"],
        "settleType": "CLOSE" if request.url.path.endswith("/closeOrder") else "OPEN",
        "size": body.get("size", str(sum(int(p["size"]) for p in body.get("settlePosition", [])))),
        "status": status,
        "timestamp": clock.now.isoformat(),
    }
    if "limitPrice" in body:
        row["price"] = body["limitPrice"]
    return httpx.Response(
        200, json={"status": 0, "data": [row], "responsetime": clock.now.isoformat()}
    )


def client(setup, handler, **kwargs):
    clock, _, _, journal = setup
    return PrivateOrderClient(
        SecretStr("fixture-key"),
        SecretStr("fixture-secret"),
        journal=journal,
        transport=httpx.MockTransport(handler),
        clock=lambda: clock.now,
        monotonic=lambda: clock.mono,
        **kwargs,
    )


def test_create_is_disabled_separate_and_permanently_bound(setup, tmp_path):
    clock, reads, posts, journal = setup
    before = journal.snapshot()
    assert not before["live_enabled"] and before["mode"] == "live-execution-v1"
    assert not (journal.path.parent / "order-lab.sqlite").exists()
    assert posts.execution_binding() == {
        "instance": before["live_control"]["instance"],
        "path": str(journal.path.parent),
    }
    assert (
        LiveOrderJournal(journal.path.parent, posts, clock=lambda: clock.now).snapshot() == before
    )
    with pytest.raises(LiveOrderError):
        LiveOrderJournal.create(tmp_path / "alternate", posts, journal.limits, policy())
    assert not (tmp_path / "alternate").exists()
    offline = OrderJournal.create(tmp_path / "offline", journal.limits)
    with pytest.raises(OrderTransportError, match="dedicated_live_journal_required"):
        PrivateOrderClient(SecretStr("fixture-key"), SecretStr("fixture-secret"), journal=offline)
    with pytest.raises(sqlite3.Error):
        LiveOrderJournal(offline.path.parent, posts)
    assert offline.snapshot()["orders"] == [] and reads.scope == "synthetic"


@pytest.mark.parametrize("halted", [False, True])
def test_token_recovery_preserves_live_binding_and_does_not_enable_orders(setup, halted):
    from trading.post_control import TOKEN_CONFIRMATIONS, TOKEN_QUIET_SECONDS

    clock, reads, posts, journal = setup
    order = ready(setup)
    if halted:
        journal.halt()
    before = journal.snapshot()
    binding = posts.execution_binding()
    with pytest.raises(RuntimeError), posts.token_slot("POST"):
        raise RuntimeError("synthetic")
    saved = posts.snapshot()
    clock.advance(TOKEN_QUIET_SECONDS)
    posts.recover_token(
        expected_revision=saved["revision"],
        expected_reason=saved["reason"],
        expected_claim=saved["claim"],
        confirmations=TOKEN_CONFIRMATIONS,
    )
    fresh_posts = PersistentPostLimiter(posts.path.parent, reads, **clock.post_args())
    fresh = LiveOrderJournal(journal.path.parent, fresh_posts, clock=lambda: clock.now)
    assert fresh_posts.execution_binding() == binding
    state = fresh.snapshot()
    assert state["live_control"] == before["live_control"]
    assert state["orders"] == before["orders"]
    assert state["halted"] == halted
    # The old approval has expired during the token quiet period; recovery
    # neither renews it nor clears an explicit live stop.
    assert not state["live_enabled"]
    with pytest.raises(LiveOrderError, match="not_enabled"):
        fresh.request(order.client_id)


@pytest.mark.parametrize("damage", ["table", "row", "digest", "policy", "mode"])
def test_binding_policy_and_mode_damage_refuses_reopen(setup, damage):
    clock, _, posts, journal = setup
    if damage in {"table", "row"}:
        with sqlite3.connect(posts.path) as conn:
            conn.execute(
                "DROP TABLE execution_binding"
                if damage == "table"
                else "DELETE FROM execution_binding"
            )
    else:
        with sqlite3.connect(journal.path) as conn:
            conn.execute(
                {
                    "digest": "UPDATE live_control SET digest='wrong'",
                    "policy": "DELETE FROM account_gate",
                    "mode": "UPDATE metadata SET mode='offline-execution-lab-v1'",
                }[damage]
            )
    with pytest.raises(ValueError):
        LiveOrderJournal(journal.path.parent, posts, clock=lambda: clock.now)


@pytest.mark.parametrize(
    "damage", ["confirmations", "account", "configuration", "implementation", "expiry", "revision"]
)
def test_activation_requires_matching_current_explicit_acceptance(setup, damage):
    clock, _, _, journal = setup
    accepted = approval(journal, clock)
    kwargs = {"expected_revision": 0, "confirmations": CONFIRMATIONS, "now": clock.now}
    if damage == "confirmations":
        kwargs["confirmations"] = CONFIRMATIONS - {"live-orders"}
    elif damage == "expiry":
        kwargs["now"] = accepted.expires_at
    elif damage == "revision":
        kwargs["expected_revision"] = 1
    else:
        field = {
            "account": "account_id",
            "configuration": "configuration_sha256",
            "implementation": "implementation_sha256",
        }[damage]
        accepted = accepted.model_copy(update={field: "other" if damage == "account" else "b" * 64})
    with pytest.raises(LiveOrderError):
        journal.activate(accepted, **kwargs)
    assert not journal.snapshot()["live_enabled"]


def test_code_update_preserves_read_and_requires_new_explicit_approval(setup, monkeypatch):
    clock, reads, posts, journal = setup
    order = ready(setup)
    saved = journal.snapshot()
    old_approval = approval(journal, clock)
    before = journal.path.read_bytes()
    monkeypatch.setattr("trading.live_journal.implementation_sha256", lambda: "b" * 64)
    reopened = LiveOrderJournal(journal.path.parent, posts, clock=lambda: clock.now)
    current = reopened.snapshot()
    context = reopened.activation_context()
    assert current["live_control"] == saved["live_control"]
    assert current["orders"] == saved["orders"]
    assert not current["implementation_matches"] and not current["live_enabled"]
    assert context["implementation_sha256"] == "b" * 64
    assert context["configuration_sha256"] != old_approval.configuration_sha256
    assert journal.path.read_bytes() == before
    with pytest.raises(LiveOrderError, match="not_enabled"):
        journal.request(order.client_id)  # Existing objects are fenced too.
    with pytest.raises(LiveOrderError, match="activation_refused"):
        reopened.activate(
            old_approval,
            expected_revision=context["revision"],
            confirmations=CONFIRMATIONS,
            now=clock.now,
        )
    enable(reopened, clock)
    activated = reopened.snapshot()
    assert activated["live_enabled"] and activated["implementation_matches"]
    assert activated["live_control"]["instance"] == saved["live_control"]["instance"]
    assert activated["live_control"]["revision"] == context["revision"] + 1
    assert activated["live_control"]["configuration_sha256"] == context["configuration_sha256"]
    with client(
        (clock, reads, posts, reopened), lambda request: response(clock, request)
    ) as sender:
        sender.submit(order.client_id, quote=quote(clock.now))
    assert reopened.snapshot()["orders"][0]["state"] == "RECONCILING"


@pytest.mark.parametrize("unknown", [False, True])
def test_code_update_allows_pending_order_reconciliation_and_retains_nonreplay(
    setup, monkeypatch, unknown
):
    clock, _, posts, journal = setup
    order = ready(setup)
    with client(setup, lambda request: response(clock, request)) as sender:
        sender.submit(order.client_id, quote=quote(clock.now))
    if unknown:
        journal.unknown(order.client_id)
    saved = journal.snapshot()
    monkeypatch.setattr("trading.live_journal.implementation_sha256", lambda: "b" * 64)
    reopened = LiveOrderJournal(journal.path.parent, posts, clock=lambda: clock.now)
    assert reopened.snapshot()["orders"] == saved["orders"]
    with pytest.raises(LiveOrderError, match="activation_refused"):
        enable(reopened, clock)  # New approval does not bypass unsettled orders.
    assert (
        reopened.reconcile(
            fixture_evidence(
                order, 101, 201, "EXECUTED", [fill(timestamp=clock.now.isoformat())], clock.now
            )
        )
        == "FILLED"
    )
    assert reopened.snapshot()["orders"][0]["evidence"]["executions"][0]["execution_id"] == 301
    enable(reopened, clock)
    with pytest.raises(LiveOrderError, match="already_claimed"):
        reopened.request(order.client_id)
    reopened.halt()
    with pytest.raises(LiveOrderError, match="activation_refused"):
        enable(reopened, clock)


def test_response_receipt_is_saved_when_code_changes_after_dispatch(setup, monkeypatch):
    clock, _, posts, journal = setup
    order = ready(setup)

    def handler(request):
        monkeypatch.setattr("trading.live_journal.implementation_sha256", lambda: "b" * 64)
        return response(clock, request)

    with client(setup, handler) as sender:
        receipt = sender.submit(order.client_id, quote=quote(clock.now))
    saved = journal.snapshot()
    assert saved["orders"][0]["submission_receipt"]["payload_sha256"] == receipt.payload_sha256
    assert saved["orders"][0]["state"] == "RECONCILING"
    assert posts.snapshot()["phase"] == "READY" and not saved["live_enabled"]


def test_code_change_at_last_dispatch_check_sends_no_http_and_closes_the_claim(setup, monkeypatch):
    clock, _, posts, journal = setup
    order = ready(setup)
    original = journal.validate_dispatch

    def changed(*args):
        monkeypatch.setattr("trading.live_journal.implementation_sha256", lambda: "b" * 64)
        return original(*args)

    monkeypatch.setattr(journal, "validate_dispatch", changed)
    with client(setup, lambda request: pytest.fail("code change dispatched HTTP")) as sender:
        with pytest.raises(OrderTransportError, match="order_not_sent:"):
            sender.submit(order.client_id, quote=quote(clock.now))
    # Refused before the HTTP send: not an unknown outcome, and nothing to resolve.
    assert journal.snapshot()["orders"][0]["state"] == "ABANDONED"
    assert posts.snapshot()["phase"] == "READY" and posts.snapshot()["claim"] is None
    # The changed code still cannot send anything until it is newly approved.
    assert not journal.snapshot()["live_enabled"]


def test_unavailable_code_fingerprint_does_not_prevent_diagnostics_or_emergency_stop(
    setup, monkeypatch
):
    clock, _, posts, journal = setup
    ready(setup)

    def unavailable():
        raise OSError("synthetic missing source file")

    monkeypatch.setattr("trading.live_journal.implementation_sha256", unavailable)
    reopened = LiveOrderJournal(journal.path.parent, posts, clock=lambda: clock.now)
    assert not reopened.snapshot()["live_enabled"]
    with pytest.raises(LiveOrderError, match="implementation_unavailable"):
        reopened.activation_context()
    reopened.halt()
    assert reopened.snapshot()["halted"]


def test_disabled_and_unproved_accounts_never_post(setup):
    clock, _, posts, journal = setup
    order = intent()
    journal.prepare(order)
    calls = []
    with client(setup, lambda request: calls.append(request)) as sender:
        with pytest.raises(OrderTransportError, match="order_preflight_refused"):
            sender.submit(order.client_id, quote=quote(clock.now))
        enable(journal, clock)
        with pytest.raises(OrderTransportError, match="order_preflight_refused"):
            sender.submit(order.client_id, quote=quote(clock.now))
    assert calls == [] and posts.snapshot()["phase"] == "READY"
    assert journal.snapshot()["orders"][0]["state"] == "PREPARED"
    with pytest.raises(OrderBlocked):
        journal.update_account(account(clock.now, complete=False), quote(clock.now), now=clock.now)
    assert journal.snapshot()["halted"]


@pytest.mark.parametrize("kind", ["LIMIT", "MARKET"])
@pytest.mark.parametrize("status", ["WAITING", "EXECUTED", "EXPIRED"])
def test_submit_signs_exact_post_once_with_both_claims_persisted(setup, kind, status):
    clock, _, posts, journal = setup
    order = ready(
        setup,
        intent(
            kind=kind,
            price="150.01" if kind == "LIMIT" else None,
            bound="150.02" if kind == "MARKET" else None,
        ),
    )
    calls = []

    def handler(request):
        assert journal.snapshot()["orders"][0]["state"] == "SUBMITTING"
        state = posts.snapshot()
        assert state["phase"] == "IN_FLIGHT" and state["operation"] == "order"
        assert state["request_sha256"] == hashlib.sha256(request.content).hexdigest()
        headers = dict(request.headers)
        expected = hmac.new(
            b"fixture-secret",
            (headers["api-timestamp"].encode() + b"POST/v1/order" + request.content),
            hashlib.sha256,
        ).hexdigest()
        assert headers["api-sign"] == expected and headers["api-key"] == "fixture-key"
        assert request.url == "https://forex-api.coin.z.com/private/v1/order"
        assert request.extensions["timeout"]["read"] == 5
        calls.append(request)
        return response(clock, request, status)

    with client(setup, handler) as sender:
        receipt = sender.submit(order.client_id, quote=quote(clock.now))
        assert receipt.broker_status == status and clock.mono >= 1.1
        with pytest.raises(OrderTransportError, match="order_preflight_refused"):
            sender.submit(order.client_id, quote=quote(clock.now))
        assert not sender._client.cookies
    assert (
        len(calls) == 1 and "API-KEY" not in calls[0].headers and "API-SIGN" not in calls[0].headers
    )
    row = journal.snapshot()["orders"][0]
    assert row["state"] == "RECONCILING" and row["evidence"] is None
    assert (
        row["submission_receipt"]["broker_status"] == status
        and posts.snapshot()["phase"] == "READY"
    )
    assert (
        b"fixture-key" not in journal.path.read_bytes()
        and b"fixture-secret" not in posts.path.read_bytes()
    )


@pytest.mark.parametrize("kind", ["stale", "market_closed", "spread", "size", "expired_approval"])
def test_risk_is_rechecked_after_post_wait_without_consuming_order_on_known_refusal(setup, kind):
    clock, _, posts, journal = setup
    order = ready(setup)
    calls = []
    current_quote = quote(clock.now)
    if kind == "stale":
        clock.advance(59.5)  # Fresh before pacing; stale after the normal 1.1s wait.
    elif kind == "market_closed":
        current_quote = quote(clock.now, market_open=False)
    elif kind == "spread":
        current_quote = quote(clock.now, ask="151")
    elif kind == "size":
        current_quote = quote(clock.now, bid="900", ask="900.01")
    else:
        clock.advance(3600)
    with client(setup, lambda request: calls.append(request)) as sender:
        with pytest.raises(OrderTransportError):
            sender.submit(order.client_id, quote=current_quote)
    assert calls == [] and journal.snapshot()["orders"][0]["state"] == "PREPARED"
    assert posts.snapshot()["phase"] == "READY"


@pytest.mark.parametrize("mark", ["150", "120"])
@pytest.mark.parametrize("kind", ["MARKET", "LIMIT"])
def test_close_after_restart_and_persistent_entry_loss_stop(setup, mark, kind):
    clock, reads, posts, journal = setup
    opened = ready(setup)
    calls = []

    def handler(request):
        calls.append((request.url.path, json.loads(request.content)))
        return response(clock, request)

    with client(setup, handler) as sender:
        sender.submit(opened.client_id, quote=quote(clock.now))
    journal.reconcile(
        fixture_evidence(
            opened, 101, 201, "EXECUTED", [fill(timestamp=clock.now.isoformat())], clock.now
        )
    )
    clock.advance(1)
    from decimal import Decimal

    bid = Decimal(mark)
    current_quote = quote(clock.now, bid=mark, ask=str(bid + Decimal("0.01")))
    equity = Decimal("999997") + (bid - Decimal("150.01")) * 1000
    journal.update_account(
        account(
            clock.now,
            balance="999997",
            equity=str(equity),
            required_margin="6000.4",
            available_margin=str(equity - Decimal("6000.4")),
            positions=(Position(position_id=401, side="BUY", units=1000, average_price="150.01"),),
        ),
        current_quote,
        now=clock.now,
    )
    assert journal.snapshot()["account_guard"]["entry_halted"] == (mark == "120")
    reopened = LiveOrderJournal(journal.path.parent, posts, clock=lambda: clock.now)
    close = intent(
        client_id="Close001",
        side="SELL",
        effect="CLOSE",
        kind=kind,
        price=str(bid) if kind == "LIMIT" else None,
        bound=str(bid - Decimal("0.01")) if kind == "MARKET" else None,
        positions=(Settlement(position_id=401, units=1000),),
    )
    reopened.prepare(close)
    with client((clock, reads, posts, reopened), handler) as sender:
        sender.submit(close.client_id, quote=current_quote)
    path, body = calls[-1]
    assert path == "/private/v1/closeOrder"
    assert body["settlePosition"] == [{"positionId": 401, "size": "1000"}]
    assert "size" not in body
    assert body["lowerBound" if kind == "MARKET" else "limitPrice"] == (
        str(bid - Decimal("0.01")) if kind == "MARKET" else str(bid)
    )
    assert reopened.snapshot()["orders"][1]["state"] == "RECONCILING"


@pytest.mark.parametrize(
    "failure",
    [
        401,
        403,
        429,
        500,
        302,
        "timeout",
        "api_error",
        "duplicate",
        "identity",
        "encoding",
        "content_type",
        "oversize",
        "clock",
        "cleanup",
        "cookie_cleanup",
    ],
)
def test_http_or_receipt_failure_stops_both_domains_and_never_retries(setup, failure):
    clock, _, posts, journal = setup
    order = ready(setup)
    calls = []

    def handler(request):
        calls.append(request)
        if type(failure) is int:
            return httpx.Response(
                failure, headers={"location": "https://elsewhere.invalid"}, content=b"remote secret"
            )
        if failure == "timeout":
            raise httpx.ReadTimeout("remote secret", request=request)
        if failure == "api_error":
            return httpx.Response(
                200, json={"status": 1, "messages": [{"message": "remote secret"}]}
            )
        result = response(clock, request)
        if failure == "duplicate":
            return httpx.Response(
                200,
                headers={"content-type": "application/json"},
                content=result.content.replace(b'"status":0', b'"status":1,"status":0'),
            )
        if failure == "identity":
            return httpx.Response(
                200,
                json={
                    **result.json(),
                    "data": [{**result.json()["data"][0], "clientOrderId": "Other"}],
                },
            )
        if failure == "encoding":
            result.headers["content-encoding"] = "gzip"
        elif failure == "content_type":
            result.headers["content-type"] = "text/plain"
        elif failure == "oversize":
            result.headers["content-length"] = "64001"
        elif failure == "clock":
            clock.advance(11)
        elif failure == "cleanup":
            result.close = lambda: (_ for _ in ()).throw(RuntimeError("remote secret"))
        elif failure == "cookie_cleanup":
            sender._client.cookies.clear = lambda: (_ for _ in ()).throw(
                RuntimeError("remote secret")
            )
        return result

    with client(setup, handler) as sender:
        with pytest.raises(OrderTransportError) as caught:
            sender.submit(order.client_id, quote=quote(clock.now))
        assert "remote secret" not in str(caught.value)
        with pytest.raises(OrderTransportError):
            sender.submit(order.client_id, quote=quote(clock.now))
    assert len(calls) == 1 and posts.snapshot()["phase"] == "STOPPED"
    row = journal.snapshot()["orders"][0]
    assert row["state"] == "UNKNOWN" and row["submission_receipt"] is None
    assert journal.snapshot()["halted"] and not journal.snapshot()["live_enabled"]
    assert "API-KEY" not in calls[0].headers and "API-SIGN" not in calls[0].headers
    assert b"remote secret" not in journal.path.read_bytes()


def test_stop_after_risk_claim_before_http_closes_the_claim_without_sending(setup, monkeypatch):
    clock, _, posts, journal = setup
    order = ready(setup)
    calls = []
    from trading.private_order import sign_request as original

    def stop(*args):
        result = original(*args)
        journal.halt()
        return result

    monkeypatch.setattr("trading.private_order.sign_request", stop)
    with client(setup, lambda request: calls.append(request)) as sender:
        with pytest.raises(OrderTransportError, match="order_not_sent:"):
            sender.submit(order.client_id, quote=quote(clock.now))
    assert calls == [] and posts.snapshot()["phase"] == "READY"
    saved = journal.snapshot()
    # The operator's stop stays; the never-sent order is closed, not unknown.
    assert saved["halted"] and saved["orders"][0]["state"] == "ABANDONED"


def test_failure_to_record_not_sent_stays_an_unknown_outcome(setup, monkeypatch):
    clock, _, posts, journal = setup
    order = ready(setup)

    def refuse(*args):
        raise LiveOrderError("live_dispatch_claim_mismatch")

    def broken(*args):
        raise OSError("disk full")

    monkeypatch.setattr(journal, "validate_dispatch", refuse)
    monkeypatch.setattr(journal, "record_not_sent", broken)
    with client(setup, lambda request: pytest.fail("dispatched HTTP")) as sender:
        with pytest.raises(OrderTransportError, match="order_submission_unknown"):
            sender.submit(order.client_id, quote=quote(clock.now))
    assert posts.snapshot()["phase"] == "STOPPED"
    assert journal.snapshot()["orders"][0]["state"] == "UNKNOWN"


def test_record_not_sent_requires_the_live_claim_and_the_held_post_operation(setup):
    clock, _, posts, journal = setup
    order = ready(setup)
    plan = journal.request(order.client_id)
    with pytest.raises(LiveOrderError, match="live_not_sent_claim_mismatch"):
        journal.record_not_sent(order.client_id, plan, "x")  # Still PREPARED.


def test_foreign_thread_cannot_reuse_same_limiter_object_owner(setup):
    _, _, posts, journal = setup
    with posts.operation("order", request_sha256="a" * 64):
        assert posts.owns_operation()
        with ThreadPoolExecutor(max_workers=1) as pool:
            assert pool.submit(posts.owns_operation).result() is False
            with pytest.raises(PostControlError):
                pool.submit(posts.require_operation, "order", "a" * 64).result()
            with pytest.raises(PostControlError, match="post_owner_busy"):
                pool.submit(journal.prepare, intent()).result()
    assert journal.snapshot()["orders"] == []


@pytest.mark.parametrize(
    "stage", ["ready", "completed", "operation_unknown", "operator_stop", "clock_invalid"]
)
def test_client_close_failure_records_its_cause_without_overwriting_prior_stop(setup, stage):
    from trading.post_control import TOKEN_CONFIRMATIONS, TOKEN_QUIET_SECONDS

    clock, reads, posts, journal = setup
    order = ready(setup)
    sender = client(
        setup,
        lambda request: (
            httpx.Response(500) if stage == "operation_unknown" else response(clock, request)
        ),
    )
    if stage == "completed":
        sender.submit(order.client_id, quote=quote(clock.now))
    elif stage == "operation_unknown":
        with pytest.raises(OrderTransportError):
            sender.submit(order.client_id, quote=quote(clock.now))
    elif stage in {"operator_stop", "clock_invalid"}:
        posts.stop(stage)
    saved, before = posts.snapshot(), journal.snapshot()
    sender._client.close = lambda: (_ for _ in ()).throw(RuntimeError("remote secret"))
    with pytest.raises(OrderTransportError, match="^order_client_cleanup_failed$"):
        sender.close()
    state = posts.snapshot()
    assert state["phase"] == "STOPPED" and state["claim"] == saved["claim"]
    assert state["reason"] == ("order_cleanup_failed" if stage in {"ready", "completed"} else stage)
    assert (
        sender._closed
        and sender._api_key.get_secret_value() == sender._secret.get_secret_value() == ""
    )
    assert (
        journal.snapshot()["orders"] == before["orders"] and not journal.snapshot()["live_enabled"]
    )
    assert b"remote secret" not in posts.path.read_bytes()
    reopened = PersistentPostLimiter(posts.path.parent, reads, **clock.post_args())
    assert reopened.snapshot() == state
    with pytest.raises(PostControlError), reopened.token_slot("POST"):
        pass
    clock.advance(TOKEN_QUIET_SECONDS)
    with pytest.raises(PostControlError, match="post_token_recovery_refused"):
        reopened.recover_token(
            expected_revision=state["revision"],
            expected_reason=state["reason"],
            expected_claim=state["claim"],
            confirmations=TOKEN_CONFIRMATIONS,
        )
    assert reopened.snapshot() == state


def test_unowned_direct_claim_is_refused_and_stop_cannot_be_reactivated(setup):
    clock, _, posts, journal = setup
    order = ready(setup)
    with pytest.raises(PostControlError):
        journal.begin_submission(order.client_id, quote=quote(clock.now), now=clock.now)
    assert journal.snapshot()["orders"][0]["state"] == "PREPARED"
    journal.halt()
    with pytest.raises(LiveOrderError):
        enable(journal, clock)
    assert not journal.snapshot()["live_enabled"] and posts.snapshot()["phase"] == "READY"


def test_reset_state_cannot_erase_the_durable_consumed_attempt(setup):
    clock, _, _, journal = setup
    order = ready(setup)
    with client(setup, lambda request: response(clock, request)) as sender:
        sender.submit(order.client_id, quote=quote(clock.now))
        with sqlite3.connect(journal.path) as conn:
            conn.execute("UPDATE orders SET state='PREPARED'")
        with pytest.raises(OrderTransportError, match="order_preflight_refused"):
            sender.submit(order.client_id, quote=quote(clock.now))


@pytest.mark.parametrize("damage", ["plan", "proof"])
def test_valid_json_changes_cannot_replace_prepared_plan_or_account_proof(setup, damage):
    clock, _, posts, journal = setup
    order = ready(setup)
    with sqlite3.connect(journal.path) as conn:
        if damage == "plan":
            changed = json.loads(conn.execute("SELECT intent_json FROM orders").fetchone()[0])
            changed["price"] = "150.02"
            conn.execute("UPDATE orders SET intent_json=?", (json.dumps(changed),))
        else:
            changed = json.loads(conn.execute("SELECT proof_json FROM account_gate").fetchone()[0])
            changed["snapshot"]["complete"] = False
            conn.execute("UPDATE account_gate SET proof_json=?", (json.dumps(changed),))
    with client(setup, lambda request: pytest.fail("damaged state sent")) as sender:
        with pytest.raises(OrderTransportError):
            sender.submit(order.client_id, quote=quote(clock.now))
    assert journal.snapshot()["orders"][0]["state"] == "PREPARED"
    assert posts.snapshot()["phase"] == "READY"


def test_transport_uses_secure_defaults_without_environment_configuration(setup, monkeypatch):
    original = httpx.Client
    options = []
    transport_options = []

    def create(**kwargs):
        options.append(kwargs)
        return original(**kwargs)

    monkeypatch.setattr(httpx, "Client", create)

    def transport(**kwargs):
        transport_options.append(kwargs)
        return httpx.MockTransport(lambda request: pytest.fail("unexpected request"))

    monkeypatch.setattr(httpx, "HTTPTransport", transport)
    with PrivateOrderClient(
        SecretStr("fixture-key"),
        SecretStr("fixture-secret"),
        journal=setup[3],
    ):
        pass
    assert options[0]["trust_env"] is False and options[0]["verify"] is True
    assert options[0]["follow_redirects"] is False
    assert transport_options == [{"verify": True, "trust_env": False, "retries": 0}]


@pytest.mark.parametrize("phase", ["claim", "response", "receipt", "post_completion"])
def test_actual_process_exit_across_dispatch_boundaries_keeps_order_and_post_claim(setup, phase):
    clock, reads, posts, journal = setup
    order = ready(setup)
    sent = journal.path.parent / "mock-http-sent"
    code = """
import os, httpx, time
from datetime import datetime
from pathlib import Path
from pydantic import SecretStr
from trading.read_control import PersistentReadLimiter
from trading.post_control import PersistentPostLimiter
from trading.live_journal import LiveOrderJournal
from trading.private_order import PrivateOrderClient
from trading.account_guard import AccountQuote
stamp = datetime.fromisoformat(sys.argv[4])
r = PersistentReadLimiter(Path(sys.argv[1]), 'synthetic')
p = PersistentPostLimiter(Path(sys.argv[2]), r, wall_ns=lambda: int(stamp.timestamp()*1e9))
j = LiveOrderJournal(Path(sys.argv[3]), p, clock=lambda: stamp)
phase = sys.argv[5]
if phase == 'claim':
    original = j.begin_submission
    def claim(*args, **kwargs):
        result = original(*args, **kwargs)
        os._exit(37)
    j.begin_submission = claim
if phase == 'receipt':
    original = j.acknowledge_submission
    def receipt(*args, **kwargs):
        result = original(*args, **kwargs)
        os._exit(37)
    j.acknowledge_submission = receipt
if phase == 'response':
    j.acknowledge_submission = lambda receipt: os._exit(37)
if phase == 'post_completion':
    original = p._write
    def write(conn, state, kind, **changes):
        result = original(conn, state, kind, **changes)
        if kind == 'COMPLETED':
            os._exit(37)
        return result
    p._write = write
def handler(request):
    Path(sys.argv[6]).write_text('once')
    return httpx.Response(200, json={'status':0, 'data':[{
        'rootOrderId':101, 'orderId':201, 'clientOrderId':'Buy001', 'symbol':'USD_JPY',
        'side':'BUY', 'orderType':'NORMAL', 'executionType':'LIMIT', 'settleType':'OPEN',
        'size':'1000', 'price':'150.01', 'status':'WAITING', 'timestamp':stamp.isoformat()
    }], 'responsetime':stamp.isoformat()})
c = PrivateOrderClient(SecretStr('fixture-key'), SecretStr('fixture-secret'), journal=j,
    transport=httpx.MockTransport(handler), clock=lambda: stamp)
c.submit('Buy001', quote=AccountQuote(bid='150',ask='150.01',observed_at=stamp,market_open=True))
"""
    result = subprocess.run(
        python_process_args(
            code,
            reads.path.parent,
            posts.path.parent,
            journal.path.parent,
            clock.now.isoformat(),
            phase,
            sent,
        ),
        timeout=15,
        capture_output=True,
    )
    assert result.returncode == 37, result.stderr
    row = LiveOrderJournal(journal.path.parent, posts, clock=lambda: clock.now).snapshot()[
        "orders"
    ][0]
    assert row["state"] == ("SUBMITTING" if phase in {"claim", "response"} else "RECONCILING")
    assert (row["submission_receipt"] is not None) == (phase not in {"claim", "response"})
    assert sent.exists() == (phase != "claim")
    assert posts.snapshot()["phase"] == "IN_FLIGHT" and posts.snapshot()["claim"] is not None
    with client(setup, lambda request: pytest.fail("resend after process death")) as sender:
        with pytest.raises(OrderTransportError):
            sender.submit(order.client_id, quote=quote(clock.now))


def test_a_not_sent_order_does_not_block_catalog_or_restart(setup, monkeypatch):
    clock, _, posts, journal = setup
    order = ready(setup)
    monkeypatch.setattr(
        journal,
        "validate_dispatch",
        lambda *a: (_ for _ in ()).throw(LiveOrderError("live_authorization_expired")),
    )
    with client(setup, lambda request: pytest.fail("dispatched HTTP")) as sender:
        with pytest.raises(OrderTransportError, match="order_not_sent:live_authorization_expired"):
            sender.submit(order.client_id, quote=quote(clock.now))
    monkeypatch.undo()
    journal.catalog_orders()
    journal.halt()
    try:
        journal.restart_context()
    except LiveOrderError as error:
        # Other restart prerequisites may be missing here, never the refused claim.
        assert "claim" not in str(error) and "unresolved" not in str(error)
    # Tampering with the recorded refusal is detected.
    import sqlite3

    with sqlite3.connect(journal.path) as conn:
        conn.execute(
            'UPDATE events SET payload_json=\'{"reason":"Sent anyway"}\' '
            "WHERE kind='SUBMISSION_NOT_SENT'"
        )
    with pytest.raises(LiveOrderError):
        journal.catalog_orders()
