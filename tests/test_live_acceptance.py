"""Acceptance evidence files and approvals built from them, with synthetic GETs only."""

import ctypes
import hashlib
import json
import socket
from datetime import timedelta

import pytest
from test_account_guard import account, quote
from test_live_account import broker
from test_live_flow import options
from test_live_flow import running as flow_running

from trading import live_acceptance, live_setup
from trading.live_acceptance import LiveAcceptanceError, approval, fingerprint, read_evidence
from trading.live_journal import CONFIRMATIONS, EVIDENCE_KINDS, LiveApproval
from trading.order_runtime import OrderRuntime


@pytest.fixture(autouse=True)
def no_native_or_network(monkeypatch):
    def forbidden(*args, **kwargs):
        pytest.fail("real credentials/network attempted")

    monkeypatch.setattr(ctypes, "WinDLL", forbidden, raising=False)
    monkeypatch.setattr(socket, "socket", forbidden)


@pytest.fixture
def running(tmp_path):
    yield from flow_running.__wrapped__(tmp_path)


def documents(tmp_path, kinds=("identity", "rules", "history")):
    paths = {}
    for kind in kinds:
        path = tmp_path / f"{kind} document.pdf"
        path.write_bytes(f"synthetic {kind} evidence".encode())
        paths[kind] = path
    return paths


def collect(running, tmp_path, kind):
    values, live, _ = running
    clock = values[0]
    clock.advance(1)
    runtime = OrderRuntime(live[3].path.parent, live[1].path.parent, "synthetic", **options(clock))
    return read_evidence(
        runtime.journal,
        runtime.reads,
        kind,
        values[5].plan.credential_reference,
        tmp_path / f"{kind}.json",
        clock=lambda: clock.wall,
        vault=values[4],
        transport=broker(clock, []),
    )


def test_file_fingerprint_matches_sha256_and_refuses_bad_inputs(tmp_path):
    path = documents(tmp_path)["rules"]
    evidence = fingerprint("rules", path)
    assert evidence.sha256 == hashlib.sha256(path.read_bytes()).hexdigest()
    assert evidence.reference == "rules document.pdf"
    with pytest.raises(LiveAcceptanceError, match="invalid_evidence_kind"):
        fingerprint("broker", path)
    (tmp_path / "empty").write_bytes(b"")
    with pytest.raises(LiveAcceptanceError, match="empty_evidence_document"):
        fingerprint("rules", tmp_path / "empty")


def test_read_evidence_is_saved_once_and_binds_the_journal_fingerprint(running, tmp_path):
    values, live, _ = running
    before = live[3].snapshot()
    evidence = collect(running, tmp_path, "account_baseline")
    saved = json.loads((tmp_path / "account_baseline.json").read_text(encoding="utf-8"))
    assert saved["kind"] == "account_baseline" and saved["account_identity_verified"] is False
    assert saved["configuration_sha256"] == live[3].activation_context()["configuration_sha256"]
    assert saved["report"]["assets"]["balance"] == "1000000"
    assert (
        evidence.sha256
        == hashlib.sha256((tmp_path / "account_baseline.json").read_bytes()).hexdigest()
    )
    assert live[3].snapshot() == before
    with pytest.raises(LiveAcceptanceError, match="evidence_file_exists"):
        collect(running, tmp_path, "account_baseline")
    with pytest.raises(LiveAcceptanceError, match="invalid_read_evidence_kind"):
        collect(running, tmp_path, "identity")


def test_built_approval_activates_the_registered_journal(running, tmp_path):
    values, live, _ = running
    clock, journal = values[0], live[3]
    items = [fingerprint(k, p) for k, p in documents(tmp_path).items()]
    items += [collect(running, tmp_path, k) for k in ("read_acceptance", "account_baseline")]
    journal.update_account(account(clock.wall), quote(clock.wall), now=clock.wall)
    built, revision = approval(journal, items, hours=72, now=clock.wall)
    assert isinstance(built, LiveApproval) and built.expires_at - built.accepted_at == timedelta(
        hours=72
    )
    assert {e.kind for e in built.evidence} == EVIDENCE_KINDS
    enabled = live_setup.activate(
        journal, built, expected_revision=revision, confirmations=CONFIRMATIONS
    )
    assert enabled["live_enabled"]


@pytest.mark.parametrize("hours", [0, 169, True])
def test_approval_lifetime_is_bounded(running, tmp_path, hours):
    items = [fingerprint(k, p) for k, p in documents(tmp_path, sorted(EVIDENCE_KINDS)).items()]
    with pytest.raises(LiveAcceptanceError, match="approval_hours_out_of_range"):
        approval(running[1][3], items, hours=hours, now=running[0][0].wall)


def test_each_evidence_kind_is_required_exactly_once(running, tmp_path):
    paths = documents(tmp_path, sorted(EVIDENCE_KINDS))
    items = [fingerprint(k, p) for k, p in paths.items()]
    for broken in (items[:-1], [*items, items[0]]):
        with pytest.raises(LiveAcceptanceError, match="one_evidence_per_kind_required"):
            approval(running[1][3], broken, hours=24, now=running[0][0].wall)


def test_cli_writes_an_approval_file_without_activating(running, tmp_path, capsys):
    live = running[1]
    paths = documents(tmp_path, sorted(EVIDENCE_KINDS))
    stores = [
        "--directory",
        str(live[3].path.parent),
        "--read-control-directory",
        str(live[1].path.parent),
        "--scope",
        "synthetic",
    ]
    before = live[3].snapshot()["live_control"]
    live_acceptance.main(
        [
            "approval",
            *stores,
            *[x for k, p in paths.items() for x in ("--evidence", f"{k}={p}")],
            "--hours",
            "48",
            "--output",
            str(tmp_path / "approval.json"),
        ]
    )
    printed = json.loads(capsys.readouterr().out)
    assert printed["activated"] is False and printed["expected_revision"] == before["revision"]
    saved = LiveApproval.model_validate_json((tmp_path / "approval.json").read_bytes())
    assert saved.account_id == "fixture-account"
    assert live[3].snapshot()["live_control"] == before
    live_acceptance.main(["file-evidence", "--kind", "rules", "--file", str(paths["rules"])])
    assert json.loads(capsys.readouterr().out)["kind"] == "rules"
    with pytest.raises(SystemExit):
        live_acceptance.main(["file-evidence", "--kind", "rules", "--file", str(tmp_path / "x")])
    assert "live_acceptance_failed" in capsys.readouterr().err
