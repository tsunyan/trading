import ctypes
import getpass
import json
import socket
import sys
import warnings

import httpx
import pytest
from pydantic import SecretStr

from trading.broker_contracts import RequestPlan
from trading.credential_store import (
    CREDENTIALW,
    MAX_BLOB,
    PREFIX,
    CredentialError,
    CredentialVault,
    WindowsCredentialBackend,
    main,
)
from trading.private_read import PrivateReadError
from trading.read_control import PersistentReadLimiter

KEY, SECRET = "dummy-key-no-account", "dummy-secret-no-account"
REF = "a" * 32


@pytest.fixture(autouse=True)
def forbid_native_and_network(monkeypatch):
    def forbidden(*args, **kwargs):
        pytest.fail("real credential or network access attempted")

    monkeypatch.setattr(ctypes, "WinDLL", forbidden, raising=False)
    monkeypatch.setattr(socket, "socket", forbidden)


class MemoryBackend:
    def __init__(self):
        self.records = {}
        self.reads = []

    def read(self, reference):
        self.reads.append(reference)
        return self.records.get(reference)

    def write_new(self, reference, blob):
        assert reference not in self.records
        self.records[reference] = blob


@pytest.fixture
def setup(tmp_path):
    control = PersistentReadLimiter.create(tmp_path / "control", "synthetic")
    backend = MemoryBackend()
    vault = CredentialVault(backend)
    return control, backend, vault


def save(vault, control, key=KEY):
    return vault.save(control, SecretStr(key), SecretStr(SECRET), read_only_confirmed=True)


def test_constructor_does_not_load_any_credentials():
    vault = CredentialVault()
    assert vault._backend._api is None


def test_save_load_rotate_without_plaintext_files_or_secret_repr(setup):
    control, backend, vault = setup
    first = save(vault, control)
    second = save(vault, control, "another-dummy-key")
    assert first != second and len(backend.records) == 2
    loaded = vault.load(control, first)
    assert loaded.api_key.get_secret_value() == KEY
    assert loaded.secret.get_secret_value() == SECRET
    assert KEY not in repr(loaded) and SECRET not in repr(loaded)
    assert vault.load(control, second).api_key.get_secret_value() == "another-dummy-key"
    assert KEY.encode() not in control.path.read_bytes()
    assert SECRET.encode() not in control.path.read_bytes()


def test_read_only_declaration_required(setup):
    control, backend, vault = setup
    for confirmation in [False, None, 1, "yes"]:
        with pytest.raises(CredentialError):
            vault.save(control, SecretStr(KEY), SecretStr(SECRET), read_only_confirmed=confirmation)
    assert not backend.records


@pytest.mark.parametrize(
    "value",
    ["plaintext", SecretStr(""), SecretStr("\nsecret"), SecretStr("日本語"), SecretStr("x" * 513)],
)
def test_invalid_input_never_reaches_storage(setup, value):
    control, backend, vault = setup
    with pytest.raises(CredentialError):
        vault.save(control, value, SecretStr(SECRET), read_only_confirmed=True)
    assert not backend.records


@pytest.mark.parametrize("reference", [None, "", "../../other", "Microsoft/Other", "A" * 32])
def test_bad_reference_never_reads_store(setup, reference):
    control, backend, vault = setup
    with pytest.raises(CredentialError):
        vault.load(control, reference)
    assert not backend.reads


def test_binding_to_control_instance_not_just_scope(setup, tmp_path):
    control, _, vault = setup
    reference = save(vault, control)
    another = PersistentReadLimiter.create(tmp_path / "other", "synthetic")
    with pytest.raises(CredentialError, match="binding_mismatch"):
        vault.load(another, reference)


def test_rotation_cannot_bypass_stop_and_stopped_load_does_not_read_store(setup):
    control, backend, vault = setup
    first = save(vault, control)
    control.stop("operator_stop")
    second = save(vault, control, "rotated-dummy-key")
    for reference in [first, second]:
        with pytest.raises(CredentialError, match="control_blocked"):
            vault.load(control, reference)
    assert not backend.reads and control.status()["stopped"]


def test_stop_arriving_during_credential_read_rechecked(setup):
    control, backend, vault = setup
    reference = save(vault, control)
    original = backend.read

    def stopped_read(ref):
        control.stop()
        return original(ref)

    backend.read = stopped_read
    with pytest.raises(CredentialError, match="control_blocked"):
        vault.load(control, reference)


@pytest.mark.parametrize(
    "changes",
    [
        {"version": True},
        {"version": 2},
        {"scope": "other"},
        {"control_instance": "f" * 32},
        {"reference": "f" * 32},
        {"read_only_declared": 1},
        {"api_key": None},
        {"secret": ""},
        {"extra": "unrecognized"},
    ],
)
def test_invalid_saved_record_rejected(setup, changes):
    control, backend, vault = setup
    reference = save(vault, control)
    data = json.loads(backend.records[reference])
    data.update(changes)
    backend.records[reference] = json.dumps(data).encode()
    with pytest.raises(CredentialError):
        vault.load(control, reference)


@pytest.mark.parametrize(
    "blob", [None, b"invalid-json", b"[]", b'{"version":1,"version":1}', b"x" * (MAX_BLOB + 1)]
)
def test_missing_or_malformed_store_data(setup, blob):
    control, backend, vault = setup
    backend.records[REF] = blob
    with pytest.raises(CredentialError):
        vault.load(control, REF)


def test_backend_errors_redacted(setup):
    control, backend, vault = setup

    def broken(*args):
        raise RuntimeError(KEY + SECRET)

    backend.write_new = backend.read = broken
    with pytest.raises(CredentialError) as caught:
        save(vault, control)
    assert KEY not in str(caught.value) and caught.value.__suppress_context__
    with pytest.raises(CredentialError) as caught:
        vault.load(control, REF)
    assert SECRET not in str(caught.value) and caught.value.__suppress_context__


def test_client_load_is_explicit_and_stop_after_load_prevents_http(setup):
    control, backend, vault = setup
    reference = save(vault, control)
    calls = []

    def handler(request):
        calls.append(request)
        assert request.headers["API-KEY"] == KEY
        return httpx.Response(200, json={"status": 0, "data": []})

    with vault.open_client(control, reference, transport=httpx.MockTransport(handler)) as client:
        assert backend.reads == [reference] and not calls
        client.get(RequestPlan("GET", "/v1/account/assets"))
        control.stop()
        with pytest.raises(PrivateReadError, match="reads_stopped"):
            client.get(RequestPlan("GET", "/v1/account/assets"))
    assert len(calls) == 1


class FakeNative:
    """Exercises ctypes pointer layout without calling a Windows credential API."""

    def __init__(self):
        self.records = {}
        self.allocations = []
        self.freed = 0
        self.error = 1168
        self.read_failure = False
        self.write_failure = False
        self.bad_metadata = False
        self.last_written = None

    def CredReadW(self, target, kind, flags, output):
        assert target.startswith(PREFIX) and kind == 1 and flags == 0
        if self.read_failure or target not in self.records:
            return 0
        blob = self.records[target]
        buffer = ctypes.create_string_buffer(blob)
        record = CREDENTIALW()
        record.Type, record.Persist = 1, 2
        record.TargetName = target if not self.bad_metadata else "wrong-target"
        record.CredentialBlobSize = len(blob)
        record.CredentialBlob = ctypes.cast(buffer, ctypes.POINTER(ctypes.c_ubyte))
        pointer = ctypes.pointer(record)
        self.allocations.append((buffer, record, pointer))
        ctypes.cast(output, ctypes.POINTER(ctypes.POINTER(CREDENTIALW)))[0] = pointer
        return 1

    def CredFree(self, pointer):
        record = pointer.contents
        assert ctypes.string_at(record.CredentialBlob, record.CredentialBlobSize) == (
            b"\x00" * record.CredentialBlobSize
        )
        self.freed += 1

    def CredWriteW(self, pointer, flags):
        record = pointer._obj
        assert flags == 0 and record.Type == 1 and record.Persist == 2
        assert record.Flags == record.AttributeCount == 0
        assert record.UserName == "TradingLab read-only"
        self.last_written = record
        if self.write_failure:
            return 0
        self.records[record.TargetName] = ctypes.string_at(
            record.CredentialBlob, record.CredentialBlobSize
        )
        return 1


def native():
    api = FakeNative()
    return api, WindowsCredentialBackend(api=api, error_code=lambda: api.error)


def test_native_roundtrip_bounds_and_memory_cleanup():
    api, backend = native()
    assert backend.read(REF) is None
    backend.write_new(REF, b"mock-blob")
    record = api.last_written
    assert ctypes.string_at(record.CredentialBlob, record.CredentialBlobSize) == b"\0" * 9
    assert backend.read(REF) == b"mock-blob"
    assert api.freed == 1
    with pytest.raises(CredentialError, match="reference_exists"):
        backend.write_new(REF, b"do-not-overwrite")
    assert api.records[PREFIX + REF] == b"mock-blob"


def test_native_access_failure_is_not_missing():
    api, backend = native()
    api.read_failure, api.error = True, 5
    with pytest.raises(CredentialError, match="read_failed"):
        backend.read(REF)


def test_native_write_failure_wipes_buffer():
    api, backend = native()
    api.write_failure = True
    with pytest.raises(CredentialError, match="write_failed"):
        backend.write_new(REF, b"mock-blob")
    assert ctypes.string_at(api.last_written.CredentialBlob, 9) == b"\0" * 9


def test_native_invalid_metadata_still_freed():
    api, backend = native()
    api.records[PREFIX + REF] = b"mock"
    api.bad_metadata = True
    with pytest.raises(CredentialError, match="invalid_native"):
        backend.read(REF)
    assert api.freed == 1


def test_native_rejects_out_of_namespace_or_large_writes():
    _, backend = native()
    with pytest.raises(CredentialError):
        backend.read("Microsoft/Other")
    with pytest.raises(CredentialError):
        backend.write_new(REF, b"x" * (MAX_BLOB + 1))


def test_cli_never_accepts_secret_arguments_or_echo_fallback(setup, monkeypatch, capsys):
    control, _, vault = setup
    monkeypatch.setattr("trading.credential_store.CredentialVault", lambda: vault)
    monkeypatch.setattr(sys.stdin, "isatty", lambda: True)

    def warning_input(prompt):
        warnings.warn("Cannot control echo", getpass.GetPassWarning, stacklevel=2)
        pytest.fail("echo fallback reached")

    monkeypatch.setattr(getpass, "getpass", warning_input)
    args = [
        "save",
        "--directory",
        str(control.path.parent),
        "--scope",
        "synthetic",
        "--read-only-confirmed",
    ]
    with pytest.raises(SystemExit) as caught:
        main(args)
    assert caught.value.code == 2
    assert KEY not in capsys.readouterr().err


def test_cli_save_and_check_only_emit_nonsecret_metadata(setup, monkeypatch, capsys):
    control, _, vault = setup
    monkeypatch.setattr("trading.credential_store.CredentialVault", lambda: vault)
    monkeypatch.setattr(sys.stdin, "isatty", lambda: True)
    inputs = iter([KEY, SECRET])
    monkeypatch.setattr(getpass, "getpass", lambda prompt: next(inputs))
    args = ["--directory", str(control.path.parent), "--scope", "synthetic"]
    main(["save", *args, "--read-only-confirmed"])
    output = capsys.readouterr().out
    assert KEY not in output and SECRET not in output
    reference = json.loads(output)["reference"]
    main(["check", *args, "--reference", reference])
    result = json.loads(capsys.readouterr().out)
    assert result["loaded"] and not result["network_used"]
    assert not result["broker_identity_verified"]


def test_unknown_cli_secret_argument_is_not_echoed(capsys):
    with pytest.raises(SystemExit):
        main(["save", "--directory", "unused", "--scope", "synthetic", "--api-key", KEY])
    output = capsys.readouterr()
    assert KEY not in output.out + output.err


def test_native_library_configuration_without_loading_dll(monkeypatch):
    from types import SimpleNamespace

    api = SimpleNamespace(
        CredReadW=lambda *a: None, CredWriteW=lambda *a: None, CredFree=lambda *a: None
    )
    calls = []

    def fake_loader(name, **options):
        calls.append((name, options))
        return api

    monkeypatch.setattr(ctypes, "WinDLL", fake_loader, raising=False)
    monkeypatch.setattr("trading.credential_store.os.name", "nt")
    monkeypatch.setattr(ctypes, "get_last_error", lambda: 0, raising=False)
    backend = WindowsCredentialBackend()
    assert backend._native() is api
    assert calls == [("advapi32.dll", {"use_last_error": True, "winmode": 0x800})]
    assert len(api.CredReadW.argtypes) == 4
    assert len(api.CredWriteW.argtypes) == 2
    assert api.CredFree.restype is None
