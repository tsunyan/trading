"""Live journal lifecycle CLI with temporary controls; no credentials or HTTP."""

import hashlib
import json
import socket
from datetime import UTC, datetime, timedelta

import pytest
from test_account_guard import account, intent, policy, quote

from trading import live_setup
from trading.broker_contracts import OrderLimits
from trading.live_journal import CONFIRMATIONS, EVIDENCE_KINDS
from trading.post_control import PersistentPostLimiter
from trading.private_order_recovery import PrivateOrderRecovery
from trading.read_control import PersistentReadLimiter


@pytest.fixture(autouse=True)
def no_network(monkeypatch):
    monkeypatch.setattr(socket, "socket", lambda *a, **k: pytest.fail("real network attempted"))


@pytest.fixture
def controls(tmp_path):
    reads = PersistentReadLimiter.create(tmp_path / "reads", "synthetic")
    PersistentPostLimiter.create(tmp_path / "posts", reads)
    config = {
        "limits": OrderLimits(
            min_units=100,
            max_units=1000,
            unit_step=100,
            price_tick="0.001",
            max_reference_notional="200000",
        ).model_dump(mode="json"),
        "policy": policy().model_dump(mode="json"),
    }
    (tmp_path / "config.json").write_text(json.dumps(config))
    return tmp_path


def cli(controls, capsys, *args):
    live_setup.main(
        [
            args[0],
            "--directory",
            str(controls / "live"),
            "--read-control-directory",
            str(controls / "reads"),
            "--scope",
            "synthetic",
            *args[1:],
        ]
    )
    return json.loads(capsys.readouterr().out)


def failure(controls, capsys, *args):
    with pytest.raises(SystemExit) as raised:
        cli(controls, capsys, *args)
    assert raised.value.code == 2
    err = capsys.readouterr().err
    assert "Traceback" not in err
    return err.strip()


def approval_file(controls, context):
    now = datetime.now(UTC)
    approval = {
        "account_id": context["account_id"],
        "configuration_sha256": context["configuration_sha256"],
        "implementation_sha256": context["implementation_sha256"],
        "accepted_at": (now - timedelta(seconds=1)).isoformat(),
        "expires_at": (now + timedelta(hours=1)).isoformat(),
        "evidence": [
            {
                "kind": kind,
                "reference": f"synthetic-{kind}",
                "sha256": hashlib.sha256(kind.encode()).hexdigest(),
            }
            for kind in sorted(EVIDENCE_KINDS)
        ],
    }
    path = controls / "approval.json"
    path.write_text(json.dumps(approval))
    return path


def test_create_prepare_activate_status_and_stop(controls, capsys):
    created = cli(controls, capsys, "create", "--config", str(controls / "config.json"))
    assert created["phase"] == "DISABLED" and not created["live_enabled"]
    assert not created["operations_registered"] and created["network_used"] is False
    assert created["account_id"] == "fixture-account"
    failure(controls, capsys, "create", "--config", str(controls / "config.json"))

    (controls / "intent.json").write_text(intent().model_dump_json())
    prepared = cli(controls, capsys, "prepare", "--intent", str(controls / "intent.json"))
    assert prepared == {"client_id": "Buy001", "state": "PREPARED", "network_used": False}

    journal = PrivateOrderRecovery(controls / "live", controls / "reads", "synthetic").journal
    now = datetime.now(UTC)
    journal.update_account(account(now), quote(now), now=now)
    context = cli(controls, capsys, "activation-context")
    path = approval_file(controls, context)
    confirms = [item for c in sorted(CONFIRMATIONS) for item in ("--confirm", c)]
    assert (
        failure(controls, capsys, "activate", "--approval", str(path), *confirms)
        == "expected_revision_required"
    )
    # Operational activation requires the registered sync/watchdog prerequisites first.
    assert (
        failure(
            controls,
            capsys,
            "activate",
            "--approval",
            str(path),
            "--expected-revision",
            str(context["revision"]),
            *confirms,
        )
        == "register_operations_before_activation"
    )
    assert not cli(controls, capsys, "status")["live_enabled"]
    assert failure(controls, capsys, "stop") == "explicit_stop_confirmation_required"
    stopped = cli(controls, capsys, "stop", "--confirm-stop")
    assert stopped["halted"] and not stopped["live_enabled"]
    assert cli(controls, capsys, "status")["halted"]


def test_registered_journal_activates_through_the_operational_helper(tmp_path):
    from test_live_operations import bind
    from test_live_operations import unbound as operations_unbound
    from test_private_order import approval

    unbound = operations_unbound.__wrapped__(tmp_path)
    values, live, _ = unbound
    clock, _, _, journal = live
    journal.prepare(intent())
    journal.update_account(account(clock.now), quote(clock.now), now=clock.now)
    with pytest.raises(live_setup.LiveSetupError, match="register_operations_before_activation"):
        live_setup.activate(
            journal,
            approval(journal, clock),
            expected_revision=journal.snapshot()["live_control"]["revision"],
            confirmations=CONFIRMATIONS,
        )
    bind(unbound)
    for confirmations in (set(), CONFIRMATIONS - {"live-orders"}):
        with pytest.raises(ValueError):
            live_setup.activate(
                journal,
                approval(journal, clock),
                expected_revision=journal.snapshot()["live_control"]["revision"],
                confirmations=confirmations,
            )
    result = live_setup.activate(
        journal,
        approval(journal, clock),
        expected_revision=journal.snapshot()["live_control"]["revision"],
        confirmations=CONFIRMATIONS,
    )
    assert result["phase"] == "ENABLED" and result["operations_registered"]
    assert result["orders"] == [{"client_id": "Buy001", "state": "PREPARED"}]
    assert values[3].reads == []


@pytest.mark.parametrize(
    "content",
    [
        b"{",
        b'{"limits":{},"limits":{}}',
        b'{"limits":{},"policy":{},"extra":1}',
        b" " * (live_setup.MAX_FILE + 1),
    ],
    ids=["truncated", "duplicate", "extra", "oversized"],
)
def test_invalid_configuration_never_creates_or_echoes(controls, capsys, content):
    (controls / "bad.json").write_bytes(content)
    reason = failure(controls, capsys, "create", "--config", str(controls / "bad.json"))
    assert reason in {"invalid_input_file", "input_file_too_large"}
    assert not (controls / "live").exists()


def test_create_requires_post_control_and_inputs(controls, capsys, tmp_path):
    PersistentReadLimiter.create(tmp_path / "lonely", "synthetic")
    with pytest.raises(SystemExit):
        live_setup.main(
            [
                "create",
                "--directory",
                str(tmp_path / "other-live"),
                "--read-control-directory",
                str(tmp_path / "lonely"),
                "--scope",
                "synthetic",
                "--config",
                str(controls / "config.json"),
            ]
        )
    assert "post_control_binding_required" in capsys.readouterr().err
    assert failure(controls, capsys, "create") == "input_file_required"
    cli(controls, capsys, "create", "--config", str(controls / "config.json"))
    (controls / "intent.json").write_text('{"client_id": "x"}')
    assert failure(controls, capsys, "prepare", "--intent", str(controls / "intent.json")) == (
        "invalid_input_file"
    )


def test_abandon_requires_confirmation_and_only_unsent_orders(controls, capsys):
    cli(controls, capsys, "create", "--config", str(controls / "config.json"))
    (controls / "intent.json").write_text(intent().model_dump_json())
    cli(controls, capsys, "prepare", "--intent", str(controls / "intent.json"))
    assert (
        failure(controls, capsys, "abandon", "--client-id", "Buy001")
        == "explicit_abandon_confirmation_required"
    )
    result = cli(controls, capsys, "abandon", "--client-id", "Buy001", "--confirm-abandon")
    assert result["state"] == "ABANDONED"
    assert cli(controls, capsys, "status")["orders"] == [
        {"client_id": "Buy001", "state": "ABANDONED"}
    ]
    assert (
        failure(controls, capsys, "abandon", "--client-id", "Buy001", "--confirm-abandon")
        == "live_setup_failed"
    )


def test_backup_is_a_consistent_copy_that_never_overwrites(controls, capsys):
    import sqlite3

    cli(controls, capsys, "create", "--config", str(controls / "config.json"))
    (controls / "intent.json").write_text(intent().model_dump_json())
    cli(controls, capsys, "prepare", "--intent", str(controls / "intent.json"))
    copy = controls / "audit" / "live-backup.sqlite"
    copy.parent.mkdir()
    result = cli(controls, capsys, "backup", "--output", str(copy))
    assert result["events"] >= 1 and result["restorable"] is False and len(result["sha256"]) == 64
    with sqlite3.connect(copy) as conn:
        assert conn.execute("SELECT client_id FROM orders").fetchall() == [("Buy001",)]
    assert failure(controls, capsys, "backup", "--output", str(copy)) == "output_exists"
    assert failure(controls, capsys, "backup") == "output_required"


def test_backup_never_replaces_a_file_created_during_the_copy(controls, capsys, monkeypatch):
    import sqlite3 as sqlite

    from trading import live_setup as module

    cli(controls, capsys, "create", "--config", str(controls / "config.json"))
    copy = controls / "late.sqlite"
    real = sqlite.connect

    def racing(target, *args, **kwargs):
        connection = real(target, *args, **kwargs)
        if not copy.exists() and ".backup-" in str(target):
            copy.write_bytes(b"created by another process")
        return connection

    monkeypatch.setattr(module.sqlite3, "connect", racing)
    assert failure(controls, capsys, "backup", "--output", str(copy)) == "output_exists"
    assert copy.read_bytes() == b"created by another process"
    assert [p.name for p in controls.iterdir() if p.name.startswith(".backup-")] == []
