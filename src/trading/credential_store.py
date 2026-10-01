"""Opt-in Windows generic credentials for GET clients. No enumeration or key export."""

import argparse
import ctypes
import getpass
import json
import os
import re
import sys
import uuid
import warnings
from ctypes import wintypes
from dataclasses import dataclass, field
from pathlib import Path

from pydantic import SecretStr

from trading.private_read import PrivateReadClient, PrivateReadError
from trading.read_control import PersistentReadLimiter
from trading.wire_validation import unique_object

PREFIX = "TradingLab/GMOFX/ReadOnly/v1/"
MAX_BLOB = 2560


class CredentialError(ValueError):
    """Fixed reason codes only. Never display native errors or input values."""


class CREDENTIALW(ctypes.Structure):
    _fields_ = [
        ("Flags", wintypes.DWORD),
        ("Type", wintypes.DWORD),
        ("TargetName", wintypes.LPWSTR),
        ("Comment", wintypes.LPWSTR),
        ("LastWritten", wintypes.FILETIME),
        ("CredentialBlobSize", wintypes.DWORD),
        ("CredentialBlob", ctypes.POINTER(ctypes.c_ubyte)),
        ("Persist", wintypes.DWORD),
        ("AttributeCount", wintypes.DWORD),
        ("Attributes", ctypes.c_void_p),
        ("TargetAlias", wintypes.LPWSTR),
        ("UserName", wintypes.LPWSTR),
    ]


def _target(reference):
    if not isinstance(reference, str) or not re.fullmatch(r"[a-f0-9]{32}", reference):
        raise CredentialError("invalid_credential_reference")
    return PREFIX + reference


class WindowsCredentialBackend:
    """Lazy native boundary. Injected API is for trusted tests only."""

    def __init__(self, *, api=None, error_code=None):
        self._api = api
        self._error_code = error_code

    def _native(self):
        if self._api is None:
            if os.name != "nt":
                raise CredentialError("windows_credential_store_required")
            # System32 only: do not load a same-named DLL from the working folder.
            api = ctypes.WinDLL("advapi32.dll", use_last_error=True, winmode=0x800)
            pointer = ctypes.POINTER(CREDENTIALW)
            api.CredReadW.argtypes = [
                wintypes.LPCWSTR,
                wintypes.DWORD,
                wintypes.DWORD,
                ctypes.POINTER(pointer),
            ]
            api.CredReadW.restype = wintypes.BOOL
            api.CredWriteW.argtypes = [pointer, wintypes.DWORD]
            api.CredWriteW.restype = wintypes.BOOL
            api.CredFree.argtypes = [ctypes.c_void_p]
            api.CredFree.restype = None
            self._api = api
            self._error_code = ctypes.get_last_error
        return self._api

    def read(self, reference):
        target = _target(reference)
        api = self._native()
        pointer = ctypes.POINTER(CREDENTIALW)()
        if not api.CredReadW(target, 1, 0, ctypes.byref(pointer)):
            if self._error_code() == 1168:  # ERROR_NOT_FOUND, not access/session failure.
                return None
            raise CredentialError("credential_read_failed")
        if not pointer:
            raise CredentialError("invalid_native_credential")
        record = pointer.contents
        size = record.CredentialBlobSize
        try:
            if (
                record.Type != 1
                or record.TargetName != target
                or record.Persist != 2
                or not 0 < size <= MAX_BLOB
                or not record.CredentialBlob
            ):
                raise CredentialError("invalid_native_credential")
            return ctypes.string_at(record.CredentialBlob, size)
        finally:
            if record.CredentialBlob and 0 < size <= MAX_BLOB:
                ctypes.memset(record.CredentialBlob, 0, size)
            api.CredFree(pointer)

    def write_new(self, reference, blob):
        target = _target(reference)
        if type(blob) is not bytes or not 0 < len(blob) <= MAX_BLOB:
            raise CredentialError("invalid_credential_blob")
        # Random revision targets; refuse a detected collision. CredWrite has no CAS.
        if self.read(reference) is not None:
            raise CredentialError("credential_reference_exists")
        api = self._native()
        buffer = ctypes.create_string_buffer(blob)
        record = CREDENTIALW()
        record.Type, record.Persist = 1, 2  # GENERIC, LOCAL_MACHINE (current user).
        record.TargetName = target
        record.UserName = "TradingLab read-only"
        record.CredentialBlobSize = len(blob)
        record.CredentialBlob = ctypes.cast(buffer, ctypes.POINTER(ctypes.c_ubyte))
        try:
            if not api.CredWriteW(ctypes.byref(record), 0):
                raise CredentialError("credential_write_failed")
        finally:
            ctypes.memset(buffer, 0, len(buffer))


@dataclass(frozen=True)
class ReadCredentials:
    api_key: SecretStr = field(repr=False)
    secret: SecretStr = field(repr=False)


def _secret(value):
    if not isinstance(value, SecretStr) or not re.fullmatch(
        r"[\x21-\x7e]{1,512}", value.get_secret_value()
    ):
        raise CredentialError("invalid_secret_value")
    return value.get_secret_value()


def _object(pairs):
    try:
        return unique_object(pairs)
    except ValueError:
        raise CredentialError("invalid_credential_payload") from None


class CredentialVault:
    """Each save creates a NEW explicit reference, never an automatic 'latest' key.

    Keys bind to the existing control instance+scope. This is local consistency,
    not proof of broker account identity or broker-side API permissions.
    """

    def __init__(self, backend=None):
        self._backend = backend if backend is not None else WindowsCredentialBackend()

    @staticmethod
    def _control(control, *, loading=False):
        if not isinstance(control, PersistentReadLimiter):
            raise CredentialError("persistent_control_required")
        status = control.status()
        if loading and status["blocked"]:
            raise CredentialError("credential_control_blocked")
        return status

    def save(self, control, api_key: SecretStr, secret: SecretStr, *, read_only_confirmed=False):
        if read_only_confirmed is not True:
            raise CredentialError("read_only_permission_declaration_required")
        try:
            state = self._control(control)
            reference = uuid.uuid4().hex
            payload = {
                "version": 1,
                "reference": reference,
                "scope": state["scope"],
                "control_instance": state["instance_id"],
                "read_only_declared": True,
                "api_key": _secret(api_key),
                "secret": _secret(secret),
            }
            blob = json.dumps(payload, separators=(",", ":")).encode("ascii")
            if len(blob) > MAX_BLOB:
                raise CredentialError("credential_blob_too_large")
            self._backend.write_new(reference, blob)
            return reference
        except CredentialError:
            raise
        except Exception:
            raise CredentialError("credential_save_failed") from None

    def load(self, control, reference) -> ReadCredentials:
        _target(reference)
        try:
            state = self._control(control, loading=True)
            blob = self._backend.read(reference)
            if blob is None:
                raise CredentialError("credential_not_found")
            if type(blob) is not bytes or not 0 < len(blob) <= MAX_BLOB:
                raise CredentialError("invalid_credential_blob")
            data = json.loads(blob, object_pairs_hook=_object)
            fields = {
                "version",
                "reference",
                "scope",
                "control_instance",
                "read_only_declared",
                "api_key",
                "secret",
            }
            if (
                not isinstance(data, dict)
                or set(data) != fields
                or type(data["version"]) is not int
                or data["version"] != 1
                or data["reference"] != reference
                or data["scope"] != state["scope"]
                or data["control_instance"] != state["instance_id"]
                or data["read_only_declared"] is not True
                or not isinstance(data["api_key"], str)
                or not isinstance(data["secret"], str)
            ):
                raise CredentialError("credential_binding_mismatch")
            credentials = ReadCredentials(SecretStr(data["api_key"]), SecretStr(data["secret"]))
            _secret(credentials.api_key)
            _secret(credentials.secret)
            self._control(control, loading=True)  # Stop may have arrived during native read.
            return credentials
        except CredentialError:
            raise
        except Exception:
            raise CredentialError("credential_load_failed") from None

    def open_client(self, control, reference, **transport_options):
        """Explicit credential read; constructor alone does not send HTTP."""
        credentials = self.load(control, reference)
        return PrivateReadClient(
            credentials.api_key, credentials.secret, limiter=control, **transport_options
        )


class CredentialParser(argparse.ArgumentParser):
    def error(self, message):
        # argparse normally echoes unknown arguments; they might contain a key.
        self.exit(2, "Invalid credential command; secret arguments are not supported.\n")


def main(argv=None):
    parser = CredentialParser(description=__doc__)
    parser.add_argument("command", choices=("save", "check"))
    parser.add_argument("--directory", type=Path, required=True)
    parser.add_argument("--scope", required=True)
    parser.add_argument("--reference")
    parser.add_argument("--read-only-confirmed", action="store_true")
    args = parser.parse_args(argv)
    try:
        control = PersistentReadLimiter(args.directory, args.scope)
        vault = CredentialVault()
        if args.command == "save":
            if args.reference or not args.read_only_confirmed or not sys.stdin.isatty():
                raise CredentialError("interactive_read_only_save_required")
            with warnings.catch_warnings():
                # getpass must NOT fall back to an echoed stdin read.
                warnings.simplefilter("error", getpass.GetPassWarning)
                key = SecretStr(getpass.getpass("Read-only API key: "))
                secret = SecretStr(getpass.getpass("API secret: "))
            reference = vault.save(control, key, secret, read_only_confirmed=True)
            print(json.dumps({"stored": True, "reference": reference, "network_used": False}))
        else:
            if args.read_only_confirmed:
                raise CredentialError("invalid_check_options")
            vault.load(control, args.reference)
            print(
                json.dumps(
                    {"loaded": True, "network_used": False, "broker_identity_verified": False}
                )
            )
    except (CredentialError, PrivateReadError, OSError, EOFError, getpass.GetPassWarning):
        parser.exit(2, "Credential operation failed; no key material is displayed.\n")


if __name__ == "__main__":
    main()
