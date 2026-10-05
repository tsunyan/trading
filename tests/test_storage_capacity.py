"""Capacity refusal before credentials, after pacing, and before mocked order HTTP."""

import socket
from types import SimpleNamespace

import pytest
from test_account_guard import account, fill, intent, quote
from test_cancel_authorization import permit
from test_private_cancel import working
from test_private_order import client, ready, response
from test_private_order import setup as live_setup

from trading.account_guard import Position
from trading.broker_contracts import Settlement
from trading.execution_lab import fixture_evidence
from trading.private_order import OrderTransportError
from trading.storage_capacity import MIN_FREE_BYTES, StorageCapacityError, require_capacity


@pytest.fixture(autouse=True)
def no_network(monkeypatch):
    monkeypatch.setattr(socket, "socket", lambda *a, **k: pytest.fail("real network attempted"))


@pytest.fixture
def setup(tmp_path):
    return live_setup.__wrapped__(tmp_path)


def test_capacity_threshold_and_duplicate_directories(tmp_path, monkeypatch):
    seen = []

    def usage(path):
        seen.append(path)
        return SimpleNamespace(free=MIN_FREE_BYTES)

    monkeypatch.setattr("trading.storage_capacity.shutil.disk_usage", usage)
    require_capacity((tmp_path, tmp_path))
    assert seen == [tmp_path]


@pytest.mark.parametrize("operation", ["submit", "close", "cancel", "restricted_cancel"])
@pytest.mark.parametrize("stage", ["preflight", "after_wait", "before_http"])
@pytest.mark.parametrize("store", ["live", "posts", "reads"])
def test_low_capacity_never_sends_or_leaves_unknown_claims(
    setup, monkeypatch, operation, stage, store
):
    clock, reads, posts, journal = setup
    authorization = None
    if operation == "submit":
        order = ready(setup)
    elif operation == "close":
        opened = ready(setup)
        with client(setup, lambda request: response(clock, request)) as sender:
            sender.submit(opened.client_id, quote=quote(clock.now))
        journal.reconcile(
            fixture_evidence(
                opened, 101, 201, "EXECUTED", [fill(timestamp=clock.now.isoformat())], clock.now
            )
        )
        journal.update_account(
            account(
                clock.now,
                balance="999997",
                equity="999987",
                required_margin="6000.4",
                available_margin="993986.6",
                positions=(
                    Position(position_id=401, side="BUY", units=1000, average_price="150.01"),
                ),
            ),
            quote(clock.now),
        )
        order = intent(
            client_id="Close001",
            side="SELL",
            effect="CLOSE",
            price="150",
            positions=(Settlement(position_id=401, units=1000),),
        )
        journal.prepare(order)
    else:
        order, _ = working(setup, partial=True)
        if operation == "restricted_cancel":
            journal.halt()
            authorization = permit(setup, order)
    target = {"live": journal.path, "posts": posts.path, "reads": reads.path}[store].parent
    low = stage == "preflight"

    def usage(path):
        return SimpleNamespace(free=512 * 2**20 if low and path == target else 2 * MIN_FREE_BYTES)

    monkeypatch.setattr("trading.storage_capacity.shutil.disk_usage", usage)
    if stage == "after_wait":

        def sleep(seconds):
            nonlocal low
            clock.advance(seconds)
            low = True

        monkeypatch.setattr(posts, "_sleep", sleep)
    elif stage == "before_http":
        method = (
            "validate_dispatch" if operation in {"submit", "close"} else "validate_cancel_dispatch"
        )
        original = getattr(journal, method)

        def validate(*args, **kwargs):
            nonlocal low
            low = True
            return original(*args, **kwargs)

        monkeypatch.setattr(journal, method, validate)
    before = journal.snapshot()
    post_before = posts.snapshot()
    with client(setup, lambda _: pytest.fail("capacity refusal sent HTTP")) as sender:
        with pytest.raises(OrderTransportError):
            if operation in {"submit", "close"}:
                sender.submit(order.client_id, quote=quote(clock.now))
            else:
                sender.cancel(order.client_id, authorization_sha256=authorization)
    after = journal.snapshot()
    assert low  # The selected boundary was reached; an earlier unrelated refusal did not pass.
    index = 1 if operation == "close" else 0
    expected = before["orders"][index]["state"]
    if stage == "before_http":
        expected = "ABANDONED" if operation in {"submit", "close"} else "PARTIAL"
        kind = "SUBMISSION_NOT_SENT" if operation in {"submit", "close"} else "CANCEL_NOT_SENT"
        refused = [e["payload"] for e in after["events"] if e["kind"] == kind]
        # The durable record keeps the fixed cause after free space recovers.
        assert [r["reason"] for r in refused] == ["disk_space_low"]
    assert after["orders"][index]["state"] == expected
    assert after["halted"] == before["halted"]
    assert after["live_control"] == before["live_control"]
    assert after["account_guard"] == before["account_guard"]
    assert posts.snapshot()["phase"] == "READY" and posts.snapshot()["claim"] is None
    if stage == "preflight":
        assert after == before and posts.snapshot() == post_before


def test_unavailable_capacity_never_exposes_os_details(setup, monkeypatch):
    _, _, posts, journal = setup
    order = ready(setup)
    before = journal.snapshot()
    post_before = posts.snapshot()

    def unavailable(_):
        raise OSError("sensitive local path")

    monkeypatch.setattr("trading.storage_capacity.shutil.disk_usage", unavailable)
    with pytest.raises(StorageCapacityError, match="^disk_space_unavailable$"):
        require_capacity((journal.path.parent,))
    with client(setup, lambda _: pytest.fail("unavailable capacity sent HTTP")) as sender:
        with pytest.raises(OrderTransportError, match="^order_preflight_refused$"):
            sender.submit(order.client_id, quote=quote())
    assert journal.snapshot() == before and posts.snapshot() == post_before
