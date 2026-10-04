"""Expiring operator attestations that replace permanent --confirm arguments in tasks."""

import ctypes
import json
import socket
from datetime import UTC, datetime, timedelta

import pytest
from test_live_flow import running as flow_running

from trading import live_attestation, live_cycle
from trading.live_attestation import LiveAttestationError, attest, require
from trading.live_cycle import CYCLE_CONFIRMATIONS

NOW = datetime(2026, 10, 4, 12, tzinfo=UTC)


@pytest.fixture(autouse=True)
def no_native_or_network(monkeypatch):
    def forbidden(*args, **kwargs):
        pytest.fail("real credentials/network attempted")

    monkeypatch.setattr(ctypes, "WinDLL", forbidden, raising=False)
    monkeypatch.setattr(socket, "socket", forbidden)


@pytest.fixture
def journal(tmp_path):
    yield from (values[1][3] for values in flow_running.__wrapped__(tmp_path))


def write(journal, path, *, hours=24, now=NOW, confirmations=CYCLE_CONFIRMATIONS):
    return attest(
        journal,
        path,
        confirmations=confirmations,
        required=CYCLE_CONFIRMATIONS,
        hours=hours,
        now=now,
    )


def test_attestation_is_valid_only_until_it_expires(journal, tmp_path):
    path = tmp_path / "attestation.json"
    built = write(journal, path, hours=24)
    assert set(built.confirmations) == CYCLE_CONFIRMATIONS
    assert require(path, journal, required=CYCLE_CONFIRMATIONS, now=NOW).expires_at == (
        NOW + timedelta(hours=24)
    )
    for moment in (NOW + timedelta(hours=24), NOW - timedelta(seconds=1)):
        with pytest.raises(LiveAttestationError, match="attestation_expired"):
            require(path, journal, required=CYCLE_CONFIRMATIONS, now=moment)
    # Renewal replaces the file and moves the expiry.
    write(journal, path, hours=48, now=NOW + timedelta(hours=23))
    later = NOW + timedelta(hours=30)
    assert require(path, journal, required=CYCLE_CONFIRMATIONS, now=later)


@pytest.mark.parametrize("hours", [0, 73, True])
def test_attestation_lifetime_is_short(journal, tmp_path, hours):
    with pytest.raises(LiveAttestationError, match="attestation_hours_out_of_range"):
        write(journal, tmp_path / "a.json", hours=hours)


def test_partial_confirmations_cannot_be_attested(journal, tmp_path):
    with pytest.raises(LiveAttestationError, match="attestation_confirmations_required"):
        write(journal, tmp_path / "a.json", confirmations=sorted(CYCLE_CONFIRMATIONS)[:-1])


def test_edited_foreign_or_damaged_attestations_are_refused(journal, tmp_path):
    path = tmp_path / "attestation.json"
    write(journal, path)
    saved = json.loads(path.read_text())
    cases = {
        "attestation_not_bound_to_journal": {**saved, "live_instance": "other"},
        "attestation_confirmations_required": {**saved, "confirmations": ["complete-account"]},
        "attestation_hours_out_of_range": {
            **saved,
            "expires_at": (NOW + timedelta(hours=100)).isoformat(),
        },
    }
    for reason, body in cases.items():
        path.write_text(json.dumps(body))
        with pytest.raises(LiveAttestationError, match=reason):
            require(path, journal, required=CYCLE_CONFIRMATIONS, now=NOW)
    for content in ("{", json.dumps(saved).replace("{", '{"format": "x",', 1)):
        path.write_text(content)
        with pytest.raises(LiveAttestationError, match="attestation_unavailable"):
            require(path, journal, required=CYCLE_CONFIRMATIONS, now=NOW)
    with pytest.raises(LiveAttestationError, match="attestation_unavailable"):
        require(tmp_path / "missing.json", journal, required=CYCLE_CONFIRMATIONS, now=NOW)


def cycle_args(journal, tmp_path, *extra):
    config = tmp_path / "fx.toml"
    config.write_text('market = "fx"\nsymbol = "USD_JPY"\nbar_seconds = 3600\n')
    return [
        "--config",
        str(config),
        "--directory",
        str(journal.path.parent),
        "--read-control-directory",
        str(journal.posts.reads.path.parent),
        "--scope",
        "synthetic",
        "--credential-reference",
        "a" * 32,
        "--units",
        "1000",
        "--max-slippage",
        "0.02",
        "--quote-output",
        str(tmp_path / "quote.json"),
        "--result-output",
        str(tmp_path / "cycle.json"),
        *extra,
    ]


def test_cycle_uses_a_valid_attestation_and_fails_closed_after_expiry(
    journal, tmp_path, monkeypatch, capsys
):
    seen = []

    class Recorded(live_cycle.LiveCycle):
        def __init__(self, *args):
            self.journal = journal

        def run(self, *args, **kwargs):
            seen.append(kwargs["confirmations"])
            return {"decision": {"action": "hold", "intent": None}, "orders_sent": False}

    monkeypatch.setattr(live_cycle, "LiveCycle", Recorded)
    monkeypatch.setattr(live_cycle, "approval_expiry", lambda cycle: None)
    path = tmp_path / "attestation.json"
    write(journal, path, now=datetime.now(UTC))
    live_cycle.main(cycle_args(journal, tmp_path, "--attestation", str(path)))
    result = json.loads((tmp_path / "cycle.json").read_text())
    assert result["ok"] and set(seen[0]) == CYCLE_CONFIRMATIONS
    assert result["attestation_expires_at"]

    write(journal, path, hours=1, now=datetime.now(UTC) - timedelta(hours=2))
    with pytest.raises(SystemExit):
        live_cycle.main(cycle_args(journal, tmp_path, "--attestation", str(path)))
    capsys.readouterr()
    result = json.loads((tmp_path / "cycle.json").read_text())
    assert result == {**result, "ok": False, "reason": "attestation_expired"}
    assert len(seen) == 1  # The expired run never reached the cycle.

    write(journal, path, now=datetime.now(UTC))
    both = cycle_args(journal, tmp_path, "--attestation", str(path), "--confirm", "x")
    with pytest.raises(SystemExit):
        live_cycle.main(both)
    capsys.readouterr()
    assert json.loads((tmp_path / "cycle.json").read_text())["reason"] == (
        "confirm_or_attestation_not_both"
    )


def test_cli_writes_and_checks_the_attestation(journal, tmp_path, capsys):
    stores = [
        "--directory",
        str(journal.path.parent),
        "--read-control-directory",
        str(journal.posts.reads.path.parent),
        "--scope",
        "synthetic",
        "--output",
        str(tmp_path / "attestation.json"),
    ]
    confirms = [x for c in sorted(CYCLE_CONFIRMATIONS) for x in ("--confirm", c)]
    live_attestation.main(["attest", *stores, "--hours", "24", *confirms])
    written = json.loads(capsys.readouterr().out)
    live_attestation.main(["status", *stores])
    assert json.loads(capsys.readouterr().out) == written
    with pytest.raises(SystemExit):
        live_attestation.main(["attest", *stores, "--hours", "24"])
    assert "attestation_confirmations_required" in capsys.readouterr().err
