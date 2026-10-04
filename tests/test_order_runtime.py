"""Order credentials and reviewed dispatch with original stores and synthetic boundaries."""

import ctypes
import json

import httpx
import pytest
from pydantic import SecretStr
from test_account_guard import account, quote
from test_cancel_authorization import permit
from test_credential_store import MemoryBackend
from test_live_operations import release
from test_live_operations import setup as operations_setup
from test_live_operations import unbound as operations_unbound
from test_private_cancel import envelope
from test_private_order import response

from trading import order_credentials, order_runtime
from trading.credential_store import (
    CREDENTIALW,
    ORDER_PREFIX,
    PREFIX,
    CredentialError,
    CredentialVault,
    WindowsCredentialBackend,
    _target,
)
from trading.execution_lab import fixture_evidence
from trading.live_execution import require_checkpoint
from trading.order_credentials import OrderCredentialVault
from trading.order_journal import OrderBlocked
from trading.order_runtime import OrderRuntime, OrderRuntimeError

KEY, SECRET = "order-dummy-key", "order-dummy-secret"


@pytest.fixture(autouse=True)
def no_native_or_network(monkeypatch):
    def forbidden(*args, **kwargs):
        pytest.fail("real credentials/network attempted")

    monkeypatch.setattr(ctypes, "WinDLL", forbidden, raising=False)
    monkeypatch.setattr("socket.socket", forbidden)


@pytest.fixture
def setup(tmp_path):
    yield from operations_setup.__wrapped__(operations_unbound.__wrapped__(tmp_path))


def runtime(setup):
    values, live = setup[0], setup[1]
    clock = values[0]
    return OrderRuntime(
        live[3].path.parent,
        live[1].path.parent,
        "synthetic",
        clock=lambda: clock.wall,
        monotonic=lambda: clock.mono,
        read_clocks={
            "wall_ns": lambda: int(clock.wall.timestamp() * 1e9),
            "monotonic_ns": lambda: int(clock.mono * 1e9),
            "sleep": clock.advance,
        },
        post_sleep=clock.advance,
    )


def stored(setup, journal=None):
    backend = MemoryBackend()
    vault = OrderCredentialVault(backend)
    reference = vault.save(
        journal or setup[1][3], SecretStr(KEY), SecretStr(SECRET), order_permission_confirmed=True
    )
    return backend, vault, reference


def transport(calls, live):
    def handler(request):
        # Signing headers are stripped after dispatch; record them at send time.
        request.extensions["api_key"] = request.headers.get("API-KEY")
        calls.append(request)
        return response(live[0], request)

    return httpx.MockTransport(handler)


def refresh(live, journal=None):
    """A newer local account checkpoint: same order, different reviewed evidence."""
    clock = live[0]
    journal = journal or live[3]
    clock.advance(1)
    journal.update_account(account(clock.now), quote(clock.now), now=clock.now)


class NativeOrders:
    """Records the native write layout without calling a Windows credential API."""

    def __init__(self):
        self.written = []

    def CredReadW(self, target, kind, flags, output):
        assert target.startswith(ORDER_PREFIX)
        return 0

    def CredWriteW(self, pointer, flags):
        record = pointer._obj
        self.written.append(
            (
                record.TargetName,
                record.UserName,
                ctypes.string_at(record.CredentialBlob, record.CredentialBlobSize),
            )
        )
        return 1


def test_order_namespace_is_separate_from_read_only_credentials():
    assert _target("a" * 32, ORDER_PREFIX) == ORDER_PREFIX + "a" * 32
    assert _target("a" * 32) == PREFIX + "a" * 32
    with pytest.raises(CredentialError, match="invalid_credential_namespace"):
        _target("a" * 32, "TradingLab/Other/")
    with pytest.raises(CredentialError, match="invalid_credential_namespace"):
        WindowsCredentialBackend(namespace="TradingLab/Other/")
    with pytest.raises(CredentialError, match="order_credential_namespace_required"):
        OrderCredentialVault(WindowsCredentialBackend())
    native = NativeOrders()
    backend = WindowsCredentialBackend(api=native, error_code=lambda: 1168, namespace=ORDER_PREFIX)
    backend.write_new("b" * 32, b"{}")
    assert native.written == [(ORDER_PREFIX + "b" * 32, "TradingLab orders", b"{}")]
    assert OrderCredentialVault()._backend._namespace == ORDER_PREFIX
    assert CredentialVault()._backend._namespace == PREFIX
    assert CREDENTIALW is not None


def test_save_requires_declaration_and_registered_original_operations(setup):
    _, live, *_ = setup
    backend = MemoryBackend()
    vault = OrderCredentialVault(backend)
    for confirmation in [False, None, 1, "yes"]:
        with pytest.raises(CredentialError, match="order_permission_declaration_required"):
            vault.save(
                live[3], SecretStr(KEY), SecretStr(SECRET), order_permission_confirmed=confirmation
            )
    with pytest.raises(CredentialError, match="dedicated_live_journal_required"):
        vault.save(object(), SecretStr(KEY), SecretStr(SECRET), order_permission_confirmed=True)
    assert backend.records == {}
    reference = vault.save(
        live[3], SecretStr(KEY), SecretStr(SECRET), order_permission_confirmed=True
    )
    payload = json.loads(backend.records[reference])
    assert payload["purpose"] == "orders" and payload["binding"] == live[3].credential_binding()
    assert KEY.encode() not in live[3].path.read_bytes()
    assert live[3].credential_binding()["account_sha256"] != "fixture-account"


def test_unregistered_journal_cannot_bind_order_credentials(tmp_path):
    from test_private_order import setup as order_setup

    clock, _, _, journal = order_setup.__wrapped__(tmp_path)
    vault = OrderCredentialVault(MemoryBackend())
    with pytest.raises(OrderBlocked, match="registered_live_operations_required"):
        journal.credential_binding()
    with pytest.raises(CredentialError, match="order_credential_save_failed"):
        vault.save(journal, SecretStr(KEY), SecretStr(SECRET), order_permission_confirmed=True)
    assert vault._backend.records == {}


def test_context_is_stable_read_only_and_names_the_exact_request(setup):
    values, live, monitor, order, _ = setup
    journal = live[3]
    before, posts = journal.snapshot(), live[2].snapshot()
    current = quote(live[0].now)
    first = journal.execution_context(order.client_id, quote=current)
    second = journal.execution_context(order.client_id, quote=current)
    assert first == second
    assert first["path"] == "/v1/order" and first["body"]["clientOrderId"] == order.client_id
    assert first["risk"]["allowed"] and first["binding"] == journal.credential_binding()
    assert journal.snapshot() == before and live[2].snapshot() == posts
    assert values[3].reads == []
    require_checkpoint(first, first["checkpoint_sha256"])
    for expected in [None, "", "A" * 64, "0" * 64, first["checkpoint_sha256"][:-1]]:
        with pytest.raises(OrderBlocked, match="execution_checkpoint_changed"):
            require_checkpoint(first, expected)


def test_context_refuses_missing_or_older_quote_and_bad_options(setup):
    _, live, _, order, _ = setup
    journal = live[3]
    with pytest.raises(OrderBlocked, match="execution_quote_required"):
        journal.execution_context(order.client_id)
    proof = journal.snapshot()["account_guard"]
    older = quote(live[0].now)
    older = older.model_copy(
        update={"observed_at": older.observed_at.replace(year=older.observed_at.year - 1)}
    )
    with pytest.raises(OrderBlocked):
        journal.execution_context(order.client_id, quote=older)
    with pytest.raises(OrderBlocked, match="invalid_execution_operation"):
        journal.execution_context(order.client_id, operation="close", quote=quote(live[0].now))
    with pytest.raises(OrderBlocked, match="invalid_submission_authorization"):
        journal.execution_context(
            order.client_id, quote=quote(live[0].now), authorization_sha256="a" * 64
        )
    with pytest.raises(OrderBlocked, match="invalid_cancel_quote"):
        journal.execution_context(order.client_id, operation="cancel", quote=quote(live[0].now))
    assert journal.snapshot()["account_guard"] == proof


def test_reviewed_submit_loads_bound_key_and_sends_exactly_once(setup):
    values, live, monitor, order, _ = setup
    backend, vault, reference = stored(setup)
    run = runtime(setup)
    current = quote(live[0].now)
    context = run.context(order.client_id, quote=current)
    assert backend.reads == []
    calls = []
    receipt = run.dispatch(
        order.client_id,
        expected_sha256=context["checkpoint_sha256"],
        credential_reference=reference,
        quote=current,
        order_permission_confirmed=True,
        vault=vault,
        transport=transport(calls, live),
    )
    assert receipt.intent.client_id == order.client_id and receipt.root_order_id == 101
    assert backend.reads == [reference] and len(calls) == 1
    assert calls[0].url.path == "/private/v1/order"
    assert calls[0].extensions["api_key"] == KEY
    assert live[3].snapshot()["orders"][0]["state"] == "RECONCILING"
    assert values[3].reads == []
    with pytest.raises(OrderRuntimeError, match="order_runtime_dispatch_failed"):
        run.dispatch(
            order.client_id,
            expected_sha256=context["checkpoint_sha256"],
            credential_reference=reference,
            quote=current,
            order_permission_confirmed=True,
            vault=vault,
            transport=transport(calls, live),
        )
    assert len(calls) == 1 and backend.reads == [reference]


@pytest.mark.parametrize("change", ["checkpoint", "quote", "account", "declaration", "sync"])
def test_changed_review_refuses_before_reading_credentials_or_claiming(setup, change):
    values, live, monitor, order, _ = setup
    backend, vault, reference = stored(setup)
    run = runtime(setup)
    current = quote(live[0].now)
    expected = run.context(order.client_id, quote=current)["checkpoint_sha256"]
    options = {"order_permission_confirmed": True}
    if change == "checkpoint":
        expected = "f" * 64
    elif change == "quote":
        current = quote(live[0].now, bid="150.001", ask="150.011")
    elif change == "account":
        refresh(live)
    elif change == "declaration":
        options["order_permission_confirmed"] = False
    else:
        release(setup)
    before, posts = live[3].snapshot(), live[2].snapshot()
    calls = []
    with pytest.raises(OrderRuntimeError):
        run.dispatch(
            order.client_id,
            expected_sha256=expected,
            credential_reference=reference,
            quote=current,
            vault=vault,
            transport=transport(calls, live),
            **options,
        )
    assert backend.reads == [] and calls == []
    assert live[3].snapshot() == before and live[2].snapshot()["claim"] is None
    assert live[2].snapshot()["revision"] == posts["revision"]


def test_stop_arriving_during_credential_read_is_rechecked_before_any_client(setup):
    _, live, _, order, _ = setup
    backend, vault, reference = stored(setup)
    run = runtime(setup)
    current = quote(live[0].now)
    expected = run.context(order.client_id, quote=current)["checkpoint_sha256"]
    original = backend.read

    def stopped(ref):
        live[2].stop("operator_stop")
        return original(ref)

    backend.read = stopped
    calls = []
    with pytest.raises(OrderRuntimeError):
        run.dispatch(
            order.client_id,
            expected_sha256=expected,
            credential_reference=reference,
            quote=current,
            order_permission_confirmed=True,
            vault=vault,
            transport=transport(calls, live),
        )
    assert backend.reads == [reference] and calls == []
    assert live[3].snapshot()["orders"][0]["state"] == "PREPARED"


@pytest.mark.parametrize("damage", ["binding", "purpose", "reference", "missing", "extra"])
def test_stored_payload_must_match_reference_purpose_and_original_binding(setup, damage):
    _, live, _, order, _ = setup
    backend, vault, reference = stored(setup)
    payload = json.loads(backend.records[reference])
    if damage == "binding":
        payload["binding"]["scope"] = "other"
    elif damage == "purpose":
        payload["purpose"] = "read-only"
    elif damage == "reference":
        payload["reference"] = "c" * 32
    elif damage == "extra":
        payload["account_id"] = "fixture-account"
    if damage == "missing":
        del backend.records[reference]
    else:
        backend.records[reference] = json.dumps(payload).encode()
    current = quote(live[0].now)
    expected = live[3].execution_context(order.client_id, quote=current)["checkpoint_sha256"]
    with pytest.raises(CredentialError, match="credential_(binding_mismatch|not_found)"):
        vault.load(
            live[3],
            reference,
            order.client_id,
            expected_sha256=expected,
            quote=current,
            order_permission_confirmed=True,
        )


@pytest.mark.parametrize("reference", [None, "", "../x", "A" * 32])
def test_bad_reference_never_reaches_backend(setup, reference):
    _, live, _, order, _ = setup
    backend, vault, _ = stored(setup)
    with pytest.raises(CredentialError):
        vault.load(
            live[3],
            reference,
            order.client_id,
            expected_sha256="a" * 64,
            quote=quote(live[0].now),
            order_permission_confirmed=True,
        )
    assert backend.reads == []


def test_claim_time_review_refuses_changes_after_the_post_wait(setup, monkeypatch):
    _, live, _, order, _ = setup
    backend, vault, reference = stored(setup)
    run = runtime(setup)
    current = quote(live[0].now)
    expected = run.context(order.client_id, quote=current)["checkpoint_sha256"]
    original = run.journal.begin_submission

    def changed(*args, **kwargs):
        refresh(live, run.journal)  # Same process-local POST owner as the claim.
        return original(*args, **kwargs)

    monkeypatch.setattr(run.journal, "begin_submission", changed)
    calls = []
    with pytest.raises(OrderRuntimeError):
        run.dispatch(
            order.client_id,
            expected_sha256=expected,
            credential_reference=reference,
            quote=current,
            order_permission_confirmed=True,
            vault=vault,
            transport=transport(calls, live),
        )
    assert calls == []
    rows = {row["client_id"]: row["state"] for row in live[3].snapshot()["orders"]}
    assert rows[order.client_id] == "PREPARED" and not live[3].snapshot()["halted"]
    assert live[2].snapshot()["claim"] is None


def test_reviewed_cancel_uses_the_target_only_authorization(setup):
    values, live, _, order, _ = setup
    clock, _, _, journal = live
    backend, vault, reference = stored(setup)
    run = runtime(setup)
    current = quote(clock.now)
    run.dispatch(
        order.client_id,
        expected_sha256=run.context(order.client_id, quote=current)["checkpoint_sha256"],
        credential_reference=reference,
        quote=current,
        order_permission_confirmed=True,
        vault=vault,
        transport=transport([], live),
    )
    journal.reconcile(fixture_evidence(order, 101, 201, "ORDERED", [], clock.now))
    release(setup)
    token = permit(live, order)
    context = run.context(order.client_id, operation="cancel", authorization_sha256=token)
    assert context["path"] == "/v1/cancelOrders" and context["quote"] is None
    calls = []

    def handler(request):
        calls.append(request)
        return httpx.Response(200, json=envelope(clock))

    receipt = run.dispatch(
        order.client_id,
        operation="cancel",
        authorization_sha256=token,
        expected_sha256=context["checkpoint_sha256"],
        credential_reference=reference,
        order_permission_confirmed=True,
        vault=vault,
        transport=httpx.MockTransport(handler),
    )
    assert receipt.accepted and len(calls) == 1
    assert backend.reads == [reference, reference]


def test_runtime_requires_the_original_post_binding_and_registration(tmp_path):
    from trading.read_control import PersistentReadLimiter

    PersistentReadLimiter.create(tmp_path / "reads", "synthetic")
    with pytest.raises(OrderRuntimeError, match="order_runtime_post_binding_required"):
        OrderRuntime(tmp_path / "live", tmp_path / "reads", "synthetic")


def test_cli_inspection_is_local_and_dispatch_requires_explicit_checkpoint(setup, tmp_path, capsys):
    _, live, _, order, _ = setup
    common = [
        "--directory",
        str(live[3].path.parent),
        "--read-control-directory",
        str(live[1].path.parent),
        "--scope",
        "synthetic",
        "--client-id",
        order.client_id,
    ]
    for extra in [
        ["context"],
        ["context", "--expected-sha256", "a" * 64],
        ["submit", "--quote", str(tmp_path / "missing.json")],
        ["cancel", "--quote", str(tmp_path / "q.json"), "--expected-sha256", "a" * 64],
    ]:
        with pytest.raises(SystemExit) as raised:
            order_runtime.main([*extra[:1], *common, *extra[1:]])
        assert raised.value.code == 2
    assert "Traceback" not in capsys.readouterr().err
    with pytest.raises(SystemExit) as raised:
        order_credentials.main(
            [
                "save",
                "--directory",
                str(live[3].path.parent),
                "--read-control-directory",
                str(live[1].path.parent),
                "--scope",
                "synthetic",
            ]
        )
    assert raised.value.code == 2


def test_load_requires_declaration_and_a_bytes_record(setup):
    _, live, _, order, _ = setup
    backend, vault, reference = stored(setup)
    current = quote(live[0].now)
    expected = live[3].execution_context(order.client_id, quote=current)["checkpoint_sha256"]
    with pytest.raises(CredentialError, match="order_permission_declaration_required"):
        vault.load(live[3], reference, order.client_id, expected_sha256=expected, quote=current)
    assert backend.reads == []
    backend.records[reference] = backend.records[reference].decode()
    with pytest.raises(CredentialError, match="invalid_credential_blob"):
        vault.load(
            live[3],
            reference,
            order.client_id,
            expected_sha256=expected,
            quote=current,
            order_permission_confirmed=True,
        )
