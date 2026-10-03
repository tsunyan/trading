"""Actual CLI composition with native/HTTP/socket boundaries replaced only."""

import ctypes
import json
import socket
import threading
from datetime import timedelta
from decimal import Decimal

import httpx
import pytest
from pydantic import SecretStr
from test_account_events import execution, raw
from test_credential_store import MemoryBackend
from test_private_stream import Clock, response
from test_private_supervisor import Socket

from trading import private_sync
from trading.account_read_lab import demo_transcript
from trading.account_reader import CollectionError
from trading.broker_contracts import OrderIntent, RequestPlan
from trading.credential_store import CredentialVault
from trading.execution_cash_book import ExecutionCashBook, OpeningCash
from trading.private_supervisor import PrivateStreamSupervisor, SupervisorError
from trading.private_sync import (
    KnownOrder,
    PrivateSyncError,
    PrivateSyncWorkspace,
    SyncPlan,
    _ReaderOwner,
    load_plan,
    main,
)
from trading.read_control import PersistentReadLimiter
from trading.segmented_journal import SegmentedEventJournal
from trading.stream_control import StreamControl, StreamControlError

KEY, SECRET = "synthetic-sync-key", "synthetic-sync-secret"


@pytest.fixture(autouse=True)
def no_native_or_network(monkeypatch):
    def forbidden(*args, **kwargs):
        pytest.fail("real credentials/network attempted")

    monkeypatch.setattr(ctypes, "WinDLL", forbidden, raising=False)
    monkeypatch.setattr(socket, "socket", forbidden)


def make_setup(tmp_path, *, legacy_catalog=False, **plan_options):
    clock = Clock()
    read_clocks = {
        "wall_ns": lambda: int(clock.wall.timestamp() * 1e9),
        "monotonic_ns": lambda: int(clock.mono * 1e9),
        "sleep": clock.advance,
    }
    reads = PersistentReadLimiter.create(tmp_path / "reads", "synthetic", **read_clocks)
    backend = MemoryBackend()
    vault = CredentialVault(backend)
    reference = vault.save(reads, SecretStr(KEY), SecretStr(SECRET), read_only_confirmed=True)
    book = ExecutionCashBook.create(
        tmp_path / "cash",
        "synthetic",
        OpeningCash(balance="1000000", cutoff=clock.wall - timedelta(seconds=1)),
    )
    plan = SyncPlan(
        scope="synthetic",
        read_control_directory=tmp_path / "reads",
        read_control_instance=reads.status()["instance_id"],
        cash_directory=tmp_path / "cash",
        credential_reference=reference,
        **plan_options,
    )
    if legacy_catalog:
        directory = tmp_path / "sync"
        directory.mkdir()
        journal = SegmentedEventJournal.create(directory / "journal", plan.scope)
        control = StreamControl.create(directory / "control", journal, book)
        instance = control.snapshot()["instance"]
        manifest = {
            "plan": plan.model_dump(mode="json"),
            "sha256": private_sync._digest(plan),
            "control_instance": instance,
        }
        (directory / "sync-plan.json").write_text(private_sync._canonical(manifest))
        reads.bind_stream(instance)
        workspace = PrivateSyncWorkspace(
            directory, clock=lambda: clock.wall, monotonic=lambda: clock.mono
        )
    else:
        workspace = PrivateSyncWorkspace.create(
            tmp_path / "sync", plan, clock=lambda: clock.wall, monotonic=lambda: clock.mono
        )
    return clock, read_clocks, reads, backend, vault, workspace


@pytest.fixture
def setup(tmp_path):
    return make_setup(tmp_path)


def options(workspace, **updates):
    return {
        "duration_seconds": 60,
        "expected_plan_sha256": workspace.plan_sha256,
        "expected_revision": workspace.control.snapshot()["revision"],
        "expected_head": workspace.journal.head(),
        "read_only_confirmed": True,
        **updates,
    }


def account_response(clock, request):
    if request.url.path.endswith("/account/assets"):
        data = demo_transcript(clock.wall).exchanges[0].response["data"]
        data[0]["balance"] = "1000000"
    else:
        data = {"list": []}
    return httpx.Response(
        200, json={"status": 0, "data": data, "responsetime": clock.wall.isoformat()}
    )


def test_init_status_reopen_never_load_credentials_and_refuse_existing_directory(setup):
    _, _, _, backend, _, workspace = setup
    before = workspace.control.snapshot()
    reopened = PrivateSyncWorkspace(workspace.directory)
    result = reopened.status()
    assert result["control"] == before and result["known_order_count"] == 0
    assert result["live_enabled"] is False and backend.reads == []
    with pytest.raises(FileExistsError):
        PrivateSyncWorkspace.create(workspace.directory, workspace.plan)
    assert reopened.control.snapshot() == before


@pytest.mark.parametrize("changed", ["plan", "revision", "head", "permission", "duration"])
def test_run_refusals_do_not_load_credentials_or_open_transport(setup, changed):
    _, _, _, backend, _, workspace = setup
    args = options(workspace)
    if changed == "permission":
        args["read_only_confirmed"] = False
    elif changed == "duration":
        args["duration_seconds"] = 0
    else:
        args[
            {
                "plan": "expected_plan_sha256",
                "revision": "expected_revision",
                "head": "expected_head",
            }[changed]
        ] = 999 if changed == "revision" else "f" * 64
    with pytest.raises(ValueError):
        workspace.run(threading.Event(), **args)
    assert backend.reads == [] and workspace.control.snapshot()["phase"] == "READY"


def test_real_read_client_reader_token_and_supervisor_compose_without_orders(setup, monkeypatch):
    clock, read_clocks, _, backend, vault, workspace = setup
    stop = threading.Event()
    original_step = PrivateStreamSupervisor.step
    requests, tokens, sockets = [], [], []

    def step(runner):
        result = original_step(runner)
        if runner.control.snapshot()["sync_successes"]:
            stop.set()
        return result

    def get(request):
        requests.append((request.method, request.url.path))
        assert request.headers["API-KEY"] == KEY
        return account_response(clock, request)

    def token(request):
        tokens.append(request.method)
        return response(clock, request.method)

    def connect(_):
        sock = Socket(clock)
        sockets.append(sock)
        return sock

    monkeypatch.setattr(PrivateStreamSupervisor, "step", step)
    result = workspace.run(
        stop,
        **options(workspace),
        vault=vault,
        read_transport=httpx.MockTransport(get),
        token_transport=httpx.MockTransport(token),
        connector=connect,
        read_clocks=read_clocks,
        stream_sleep=clock.advance,
    )
    assert result["control"]["phase"] == "READY"
    assert result["control"]["sync_successes"] == 1
    assert len(requests) == 8 and all(m == "GET" for m, _ in requests)
    assert tokens == ["POST", "DELETE"] and sockets[0].closed
    assert backend.reads == [workspace.plan.credential_reference]
    assert result["cash"]["executions"] == 0
    assert SECRET not in json.dumps(result, default=str)


def test_read_auth_failure_persists_both_stops_and_never_resumes(setup):
    clock, read_clocks, reads, backend, vault, workspace = setup
    with pytest.raises(SupervisorError):
        workspace.run(
            threading.Event(),
            **options(workspace),
            vault=vault,
            read_transport=httpx.MockTransport(lambda _: httpx.Response(401, text=SECRET)),
            token_transport=httpx.MockTransport(lambda r: response(clock, r.method)),
            connector=lambda _: Socket(clock),
            read_clocks=read_clocks,
            stream_sleep=clock.advance,
        )
    assert reads.status()["stopped"] and workspace.control.snapshot()["phase"] == "STOPPED"
    count = len(backend.reads)
    with pytest.raises(PrivateSyncError, match="dependencies_blocked"):
        workspace.run(threading.Event(), **options(workspace), vault=vault)
    assert len(backend.reads) == count


def test_order_registered_during_capture_is_really_read_and_booked_once(tmp_path, monkeypatch):
    known = KnownOrder(
        order_id=201,
        intent=OrderIntent(
            client_id="DemoOpen", side="BUY", effect="OPEN", units=1000, kind="LIMIT", price="150"
        ),
    )
    clock, read_clocks, _, _, vault, workspace = make_setup(tmp_path)
    row = execution(
        executionSize="1000",
        orderExecutedSize="1000",
        executionTimestamp=clock.wall.isoformat(),
        orderTimestamp=clock.wall.isoformat(),
    )
    order = {
        "rootOrderId": 201,
        "orderId": 201,
        "clientOrderId": "DemoOpen",
        "symbol": "USD_JPY",
        "side": "BUY",
        "settleType": "OPEN",
        "orderType": "NORMAL",
        "executionType": "LIMIT",
        "size": "1000",
        "price": "150",
        "status": "EXECUTED",
        "timestamp": clock.wall.isoformat(),
    }
    fill = {
        k: row[k]
        for k in (
            "executionId",
            "positionId",
            "orderId",
            "clientOrderId",
            "symbol",
            "side",
            "settleType",
            "amount",
            "fee",
            "lossGain",
            "settledSwap",
        )
    }
    fill.update(size="1000", price=row["executionPrice"], timestamp=row["executionTimestamp"])
    release, stop = threading.Event(), threading.Event()
    requests = []

    def get(request):
        requests.append(request.url.path)
        assert request.method == "GET"
        assert release.wait(3)
        if request.url.path.endswith("/orders") or request.url.path.endswith("/executions"):
            assert request.url.params["orderId"] == "201"
            data = {"list": [order if request.url.path.endswith("/orders") else fill]}
            return httpx.Response(
                200, json={"status": 0, "data": data, "responsetime": clock.wall.isoformat()}
            )
        result = account_response(clock, request)
        body = result.json()
        if request.url.path.endswith("/account/assets"):
            body["data"][0]["balance"] = "999998"
        return httpx.Response(200, json=body)

    def connect(_):
        sock = Socket(clock)
        sock.messages.append(raw(row))
        return sock

    original_step = PrivateStreamSupervisor.step
    registered = []

    def step(runner):
        if not registered:
            assert workspace.control.snapshot()["phase"] == "RUNNING"
            registered.append(
                workspace.register_order(
                    known,
                    expected_plan_sha256=workspace.plan_sha256,
                    expected_catalog_head=workspace.catalog.snapshot()["head"],
                    source_ref="broker-export-reviewed",
                    intent_confirmed=True,
                )
            )
        result = original_step(runner)
        release.set()  # First receive completes before the first REST response.
        if runner.control.snapshot()["sync_successes"]:
            stop.set()
        if runner._worker is None:
            clock.advance(1)
        return result

    monkeypatch.setattr(PrivateStreamSupervisor, "step", step)
    result = workspace.run(
        stop,
        **options(workspace),
        vault=vault,
        read_transport=httpx.MockTransport(get),
        token_transport=httpx.MockTransport(lambda r: response(clock, r.method)),
        connector=connect,
        read_clocks=read_clocks,
        stream_sleep=clock.advance,
    )
    assert result["cash"]["executions"] == 1
    assert Decimal(result["cash"]["balance"]) == Decimal("999998")
    assert result["control"]["phase"] == "READY" and result["control"]["sync_retries"] == 1
    assert requests.count("/private/v1/orders") == 2
    assert requests.count("/private/v1/executions") == 2
    assert workspace.plan.known_orders == () and registered[0]["records"] == 1


def test_unknown_order_is_rejected_before_order_get_and_total_budget_covers_orders(setup):
    clock, _, _, _, _, workspace = setup
    calls = []

    class Client:
        def get(self, request):
            calls.append(request)
            clock.advance(4)
            return account_response(
                clock, httpx.Request("GET", "https://synthetic" + request.path)
            ).json()

        def close(self):
            pass

    owner = _ReaderOwner(
        Client(), workspace.plan, clock=lambda: clock.wall, monotonic=lambda: clock.mono
    )
    with pytest.raises(PrivateSyncError, match="intent_missing"):
        owner.orders((999,))
    assert calls == []
    with pytest.raises(ValueError):
        owner.account()  # Eight GETs cannot fit the shared 30-second budget.
    assert len(calls) == 8

    with pytest.raises(PrivateSyncError, match="collection_deadline"):
        owner.get(RequestPlan("GET", "/v1/orders", query=(("orderId", "201"),)))
    assert len(calls) == 8


def test_order_collection_uses_the_remaining_account_budget(setup):
    clock, _, _, _, _, workspace = setup
    known = KnownOrder(
        order_id=201,
        intent=OrderIntent(
            client_id="Known", side="BUY", effect="OPEN", units=1000, kind="LIMIT", price="150"
        ),
    )
    plan = SyncPlan.model_validate({**workspace.plan.model_dump(), "known_orders": (known,)})
    calls = []

    class Client:
        def get(self, request):
            calls.append(request.path)
            if request.path == "/v1/orders":
                clock.advance(7)
                return {}  # Deadline rejection precedes response parsing.
            clock.advance(3)
            return account_response(
                clock, httpx.Request("GET", "https://synthetic" + request.path)
            ).json()

        def close(self):
            pass

    owner = _ReaderOwner(Client(), plan, clock=lambda: clock.wall, monotonic=lambda: clock.mono)
    owner.account()
    assert clock.mono == 24
    with pytest.raises(CollectionError, match="^read_transport_failed$"):
        owner.orders((201,))
    assert calls.count("/v1/orders") == 1 and clock.mono == 31


def test_already_requested_stop_does_not_claim_or_load_keys(setup):
    _, _, _, backend, _, workspace = setup
    stop = threading.Event()
    stop.set()
    result = workspace.run(stop, **options(workspace))
    assert backend.reads == [] and result["control"]["generation"] == 0


def test_deferred_reader_close_never_waits_for_a_hung_callback(setup):
    _, _, _, _, _, workspace = setup
    entered, release, closed = threading.Event(), threading.Event(), threading.Event()

    class Client:
        def close(self):
            closed.set()

    owner = _ReaderOwner(Client(), workspace.plan, clock=lambda: None, monotonic=lambda: 0)

    def callback():
        entered.set()
        release.wait(3)

    worker = threading.Thread(target=lambda: owner._call(callback))
    worker.start()
    assert entered.wait(1)
    try:
        owner.close()
        assert not closed.is_set()
        with pytest.raises(PrivateSyncError, match="reader_closed"):
            owner._call(lambda: None)
    finally:
        release.set()
        worker.join(3)
    assert closed.is_set() and not worker.is_alive()


def test_hung_real_transport_retains_stream_owner_and_defers_client_cleanup(tmp_path, monkeypatch):
    clock, read_clocks, _, _, vault, workspace = make_setup(
        tmp_path, supervisor={"join_timeout_seconds": 0.01}
    )
    entered, release = threading.Event(), threading.Event()
    runners = []
    original_step = PrivateStreamSupervisor.step

    def get(request):
        entered.set()
        assert release.wait(5)
        return account_response(clock, request)

    def step(runner):
        runners.append(runner)
        assert entered.wait(1)
        clock.advance(36)
        return original_step(runner)

    monkeypatch.setattr(PrivateStreamSupervisor, "step", step)
    try:
        with pytest.raises(SupervisorError):
            workspace.run(
                threading.Event(),
                **options(workspace),
                vault=vault,
                read_transport=httpx.MockTransport(get),
                token_transport=httpx.MockTransport(lambda r: response(clock, r.method)),
                connector=lambda _: Socket(clock),
                read_clocks=read_clocks,
                stream_sleep=clock.advance,
            )
        assert workspace.control.snapshot()["phase"] == "STOPPED"
        assert workspace.control.snapshot()["reason"] == "sync_deadline"
        peer = StreamControl(workspace.directory / "control")
        with pytest.raises(StreamControlError, match="owner_busy"), peer.ownership():
            pass
    finally:
        release.set()
        if runners:
            runners[0]._worker.join(3)
            runners[0].close()
    with workspace.control.ownership():
        pass


def test_cli_run_passes_explicit_checkpoint_permission_and_restores_signals(
    setup, monkeypatch, capsys
):
    clock, read_clocks, _, _, vault, workspace = setup
    original_run = workspace.run
    stop_events = []

    def run(stop, **arguments):
        stop_events.append(stop)
        return original_run(
            stop,
            **arguments,
            vault=vault,
            read_transport=httpx.MockTransport(lambda r: account_response(clock, r)),
            token_transport=httpx.MockTransport(lambda r: response(clock, r.method)),
            connector=lambda _: Socket(clock),
            read_clocks=read_clocks,
            stream_sleep=clock.advance,
        )

    original_step = PrivateStreamSupervisor.step

    def step(runner):
        result = original_step(runner)
        if runner.control.snapshot()["sync_successes"]:
            stop_events[0].set()
        return result

    monkeypatch.setattr(workspace, "run", run)
    monkeypatch.setattr(private_sync, "PrivateSyncWorkspace", lambda _: workspace)
    monkeypatch.setattr(PrivateStreamSupervisor, "step", step)
    import signal

    old = signal.getsignal(signal.SIGINT)
    main(
        [
            "run",
            "--directory",
            str(workspace.directory),
            "--expected-plan-sha256",
            workspace.plan_sha256,
            "--expected-revision",
            "0",
            "--expected-head",
            workspace.journal.head(),
            "--duration-seconds",
            "60",
            "--read-only-confirmed",
        ]
    )
    result = json.loads(capsys.readouterr().out)
    assert result["ok"] and result["control"]["phase"] == "READY"
    assert signal.getsignal(signal.SIGINT) is old


def test_config_paths_and_saved_body_are_checked_without_keys(setup, tmp_path):
    _, _, _, backend, _, workspace = setup
    payload = workspace.plan.model_dump(mode="json")
    payload["cash_directory"] = "cash"
    path = tmp_path / "plan.json"
    path.write_text(json.dumps(payload), encoding="utf-8")
    assert load_plan(path).cash_directory == tmp_path / "cash"
    payload["secret"] = SECRET
    path.write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(PrivateSyncError, match="invalid_sync_plan") as error:
        load_plan(path)
    assert SECRET not in str(error.value)
    saved = workspace.directory / "sync-plan.json"
    body = json.loads(saved.read_text())
    body["plan"]["credential_reference"] = "f" * 32
    saved.write_text(json.dumps(body))
    with pytest.raises(PrivateSyncError, match="workspace_invalid"):
        PrivateSyncWorkspace(workspace.directory)
    assert backend.reads == []


def test_duplicate_known_orders_and_deadline_policy_are_rejected(setup):
    _, _, _, _, _, workspace = setup
    known = KnownOrder(
        order_id=201,
        intent=OrderIntent(
            client_id="Known", side="BUY", effect="OPEN", units=1000, kind="LIMIT", price="150"
        ),
    )
    with pytest.raises(ValueError):
        SyncPlan.model_validate({**workspace.plan.model_dump(), "known_orders": (known, known)})
    with pytest.raises(ValueError):
        SyncPlan.model_validate(
            {**workspace.plan.model_dump(), "supervisor": {"sync_timeout_seconds": 30}}
        )


def test_alternate_directory_cannot_bypass_persistent_stream_stop(setup):
    _, _, reads, backend, _, workspace = setup
    with workspace.control.ownership():
        state = workspace.control.begin(
            workspace.journal, expected_revision=0, expected_head=workspace.journal.head()
        )
        workspace.control.finish(state["owner"], workspace.journal, reason="stream_failed")
    other = workspace.directory.with_name("bypass")
    with pytest.raises(PrivateSyncError, match="supervisor_already_bound"):
        PrivateSyncWorkspace.create(other, workspace.plan)
    assert not other.exists() and backend.reads == []
    assert reads.stream_binding() == workspace.control.snapshot()["instance"]
    assert workspace.control.snapshot()["phase"] == "STOPPED"


def test_cli_register_order_is_local_preserves_plan_and_stop_and_reopens(setup, tmp_path, capsys):
    _, _, _, backend, _, workspace = setup
    known = KnownOrder(
        order_id=201,
        intent=OrderIntent(
            client_id="Added", side="BUY", effect="OPEN", units=1000, kind="LIMIT", price="150"
        ),
    )
    path = tmp_path / "known.json"
    path.write_text(known.model_dump_json(), encoding="utf-8")
    with workspace.control.ownership():
        state = workspace.control.begin(
            workspace.journal, expected_revision=0, expected_head=workspace.journal.head()
        )
        workspace.control.finish(state["owner"], workspace.journal, reason="stream_failed")
    before = workspace.control.snapshot()
    plan_bytes = (workspace.directory / "sync-plan.json").read_bytes()
    main(
        [
            "register-order",
            "--directory",
            str(workspace.directory),
            "--order-file",
            str(path),
            "--expected-plan-sha256",
            workspace.plan_sha256,
            "--expected-catalog-head",
            workspace.catalog.snapshot()["head"],
            "--source-ref",
            "broker-export-reviewed",
            "--intent-confirmed",
        ]
    )
    result = json.loads(capsys.readouterr().out)
    assert result["ok"] and result["registered_order_id"] == 201
    reopened = PrivateSyncWorkspace(workspace.directory)
    assert reopened.status()["known_order_count"] == 1 and backend.reads == []
    assert workspace.control.snapshot() == before
    assert (workspace.directory / "sync-plan.json").read_bytes() == plan_bytes


def test_legacy_catalog_requires_explicit_idle_initialization_and_expected_checkpoint(tmp_path):
    _, _, _, backend, _, workspace = make_setup(tmp_path, legacy_catalog=True)
    legacy = PrivateSyncWorkspace(workspace.directory)
    assert legacy.status()["catalog"] is None
    with workspace.control.ownership(), pytest.raises(StreamControlError, match="owner_busy"):
        legacy.initialize_catalog(
            expected_plan_sha256=legacy.plan_sha256,
            expected_revision=0,
            expected_head=legacy.journal.head(),
        )
    with pytest.raises(PrivateSyncError, match="checkpoint_changed"):
        legacy.initialize_catalog(
            expected_plan_sha256=legacy.plan_sha256,
            expected_revision=1,
            expected_head=legacy.journal.head(),
        )
    legacy.initialize_catalog(
        expected_plan_sha256=legacy.plan_sha256,
        expected_revision=0,
        expected_head=legacy.journal.head(),
    )
    assert legacy.catalog.snapshot()["records"] == 0 and backend.reads == []


def test_missing_bound_catalog_is_not_treated_as_a_legacy_workspace(setup):
    _, _, _, backend, _, workspace = setup
    workspace.catalog.path.unlink()
    workspace.catalog.path.parent.rmdir()
    with pytest.raises(PrivateSyncError, match="workspace_invalid"):
        PrivateSyncWorkspace(workspace.directory)
    assert backend.reads == []


@pytest.mark.parametrize("stage", ["before", "after"])
def test_interrupted_catalog_manifest_binding_is_explicit_and_never_recreates_data(
    tmp_path, monkeypatch, stage
):
    _, _, _, backend, _, legacy = make_setup(tmp_path, legacy_catalog=True)
    original = private_sync.os.replace

    def interrupted(source, target):
        if stage == "after":
            original(source, target)
        raise OSError("synthetic rename failure")

    checks = dict(
        expected_plan_sha256=legacy.plan_sha256,
        expected_revision=0,
        expected_head=legacy.journal.head(),
    )
    with monkeypatch.context() as patch:
        patch.setattr(private_sync.os, "replace", interrupted)
        with pytest.raises(OSError):
            legacy.initialize_catalog(**checks)
    instance = legacy.catalog.snapshot()["instance"]
    fresh = PrivateSyncWorkspace(legacy.directory)
    assert fresh.catalog.snapshot()["instance"] == instance
    if stage == "before":
        assert not fresh.status()["catalog_bound"]
        with pytest.raises(PrivateSyncError, match="initialization_required"):
            fresh.run(threading.Event(), **options(fresh))
        fresh.initialize_catalog(**checks)
    else:
        assert fresh.status()["catalog_bound"]
        with pytest.raises(PrivateSyncError, match="manifest_changed"):
            legacy.run(threading.Event(), **options(legacy))
    assert fresh.status()["catalog_bound"] and backend.reads == []


def test_cli_errors_do_not_echo_unknown_arguments_or_configuration_values(setup, capsys):
    _, _, _, _, _, workspace = setup
    with pytest.raises(SystemExit) as result:
        main(["run", "--directory", str(workspace.directory), "--secret", SECRET])
    assert result.value.code == 2 and SECRET not in capsys.readouterr().err
    with pytest.raises(SystemExit) as result:
        main(["run", "--directory", str(workspace.directory)])
    assert result.value.code == 2
    captured = capsys.readouterr()
    assert "Private sync failed" in captured.err and SECRET not in captured.err


def test_cli_status_and_explicit_recovery_are_local_and_keep_fresh_start_required(setup, capsys):
    _, _, _, backend, _, workspace = setup
    main(["status", "--directory", str(workspace.directory)])
    first = json.loads(capsys.readouterr().out)
    assert first["ok"] and first["control"]["phase"] == "READY"
    with workspace.control.ownership():
        workspace.control.begin(
            workspace.journal, expected_revision=0, expected_head=workspace.journal.head()
        )
    state = workspace.control.snapshot()
    main(
        [
            "recover",
            "--directory",
            str(workspace.directory),
            "--expected-plan-sha256",
            workspace.plan_sha256,
            "--expected-revision",
            str(state["revision"]),
            "--expected-head",
            workspace.journal.head(),
            "--expected-reason",
            state["reason"],
            "--acknowledge-token-uncertainty",
        ]
    )
    result = json.loads(capsys.readouterr().out)
    assert result["control"]["phase"] == "READY" and result["control"]["generation"] == 2
    assert result["live_enabled"] is False and backend.reads == []
