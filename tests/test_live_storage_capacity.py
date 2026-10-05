"""Every bound operational store participates in read-only dispatch capacity checks."""

import ctypes
import json
import socket
from types import SimpleNamespace

import pytest
from test_account_guard import quote
from test_cancel_authorization import permit
from test_live_operations import setup as operations_setup
from test_live_operations import unbound as operations_unbound
from test_private_order import client, response

from trading.execution_lab import fixture_evidence
from trading.live_doctor import diagnose
from trading.live_journal import LiveOrderError
from trading.live_operations import LiveOperations
from trading.private_order import OrderTransportError
from trading.storage_capacity import MIN_FREE_BYTES


@pytest.fixture(autouse=True)
def no_native_or_network(monkeypatch):
    def forbidden(*args, **kwargs):
        pytest.fail("real credentials/network attempted")

    monkeypatch.setattr(ctypes, "WinDLL", forbidden, raising=False)
    monkeypatch.setattr(socket, "socket", forbidden)


@pytest.fixture
def setup(tmp_path):
    yield from operations_setup.__wrapped__(operations_unbound.__wrapped__(tmp_path))


def directories(setup):
    workspace = setup[0][5]
    return {
        "sync": workspace.directory,
        "journal": workspace.journal.path.parent,
        "control": workspace.control.path.parent,
        "catalog": workspace.catalog.path.parent,
        "monitor": setup[2].path.parent,
        "cash": workspace.book.path.parent,
    }


# One directory inventory serves every boundary, so each stage/operation pair runs
# once and the bound stores rotate through them; every store is covered at least once.
@pytest.mark.parametrize(
    "store,stage,operation",
    [
        ("sync", "preflight", "submit"),
        ("journal", "after_wait", "submit"),
        ("control", "before_http", "submit"),
        ("catalog", "preflight", "cancel"),
        ("monitor", "after_wait", "cancel"),
        ("cash", "before_http", "cancel"),
        ("journal", "preflight", "restricted_cancel"),
        ("catalog", "after_wait", "restricted_cancel"),
        ("sync", "before_http", "restricted_cancel"),
    ],
)
def test_capacity_of_every_bound_store_is_checked_at_each_send_boundary(
    setup, monkeypatch, store, stage, operation
):
    values, live, _, order, _ = setup
    clock, _, posts, journal = live
    authorization = None
    if operation != "submit":
        with client(live, lambda request: response(clock, request)) as sender:
            sender.submit(order.client_id, quote=quote(clock.now))
        journal.reconcile(fixture_evidence(order, 101, 201, "ORDERED", [], clock.now))
        if operation == "restricted_cancel":
            journal.halt()
            authorization = permit(live, order)
    target = directories(setup)[store].resolve()
    low = stage == "preflight"

    def usage(path):
        return SimpleNamespace(free=512 * 2**20 if low and path == target else 2 * MIN_FREE_BYTES)

    monkeypatch.setattr("trading.storage_capacity.shutil.disk_usage", usage)
    if stage == "after_wait":

        def sleep(seconds):
            nonlocal low
            values[0].advance(seconds)
            low = True

        monkeypatch.setattr(posts, "_sleep", sleep)
    elif stage == "before_http":
        method = "validate_dispatch" if operation == "submit" else "validate_cancel_dispatch"
        original = getattr(journal, method)

        def validate(*args, **kwargs):
            nonlocal low
            low = True
            return original(*args, **kwargs)

        monkeypatch.setattr(journal, method, validate)
    before, post_before = journal.snapshot(), posts.snapshot()
    with client(live, lambda _: pytest.fail("operational capacity refusal sent HTTP")) as sender:
        with pytest.raises(OrderTransportError):
            if operation == "submit":
                sender.submit(order.client_id, quote=quote(clock.now))
            else:
                sender.cancel(order.client_id, authorization_sha256=authorization)
    assert low
    after, post_after = journal.snapshot(), posts.snapshot()
    expected = before["orders"][0]["state"]
    if stage == "before_http":
        expected = "ABANDONED" if operation == "submit" else "WORKING"
    assert after["orders"][0]["state"] == expected
    assert after["halted"] == before["halted"]
    assert after["live_control"] == before["live_control"]
    assert after["account_guard"] == before["account_guard"]
    assert post_after["phase"] == "READY" and post_after["claim"] is None
    if stage == "preflight":
        assert after == before and post_after == post_before


@pytest.mark.parametrize("store,failure", [("cash", "low"), ("monitor", "unavailable")])
def test_doctor_uses_the_same_operational_capacity_checks(setup, monkeypatch, store, failure):
    values, live, _, _, _ = setup
    target = directories(setup)[store].resolve()
    before = live[3].snapshot()

    def usage(path):
        if path == target:
            if failure == "unavailable":
                raise OSError("private volume details")
            return SimpleNamespace(free=512 * 2**20)
        return SimpleNamespace(free=2 * MIN_FREE_BYTES)

    monkeypatch.setattr("trading.storage_capacity.shutil.disk_usage", usage)
    result = diagnose(live[3], values[0].wall)
    reason = "disk_space_low:512MiB" if failure == "low" else "disk_space_unavailable"
    assert result["gates"]["disk_space"] == {"ok": False, "reason": reason}
    assert not result["send_ready"] and live[3].snapshot() == before


def test_changed_cash_plan_is_refused_without_inspecting_or_creating_another_store(setup, tmp_path):
    values, live, _, _, _ = setup
    manifest_path = values[5].directory / "sync-plan.json"
    manifest = json.loads(manifest_path.read_text())
    replacement = tmp_path / "unregistered-cash"
    manifest["plan"]["cash_directory"] = str(replacement.resolve())
    manifest_path.write_text(json.dumps(manifest))
    before, posts = live[3].snapshot(), live[2].snapshot()
    with pytest.raises(LiveOrderError, match="^live_operations_unavailable$"):
        live[3].require_storage_capacity()
    assert not replacement.exists()
    assert live[3].snapshot() == before and live[2].snapshot() == posts


def test_capacity_inventory_does_not_scan_or_start_the_sync(setup, monkeypatch):
    _, live, _, _, _ = setup
    before, posts = live[3].snapshot(), live[2].snapshot()

    def forbidden(*args, **kwargs):
        pytest.fail("capacity inventory scanned or started a workspace")

    monkeypatch.setattr(LiveOperations, "enrollment_check", forbidden)
    monkeypatch.setattr("trading.private_sync.PrivateSyncWorkspace.__init__", forbidden)
    live[3].require_storage_capacity()
    assert live[3].snapshot() == before and live[2].snapshot() == posts
