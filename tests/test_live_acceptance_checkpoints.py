"""Restart, resolution and restricted-cancel approvals built by the acceptance CLI helpers."""

import socket

import pytest
from test_order_resolution import reopen, terminal
from test_order_restart import stopped
from test_private_cancel import working
from test_private_order import setup as order_setup

from trading.live_acceptance import (
    LiveAcceptanceError,
    checkpoint_approval,
    fingerprint,
    restart_approval,
)
from trading.live_journal import (
    CANCEL_CONFIRMATIONS,
    CANCEL_EVIDENCE_KINDS,
    EVIDENCE_KINDS,
    RESOLUTION_CONFIRMATIONS,
)


@pytest.fixture(autouse=True)
def no_network(monkeypatch):
    monkeypatch.setattr(socket, "socket", lambda *a, **k: pytest.fail("real network attempted"))


@pytest.fixture
def setup(tmp_path):
    return order_setup.__wrapped__(tmp_path)


def evidence(tmp_path, kinds):
    items = []
    for kind in sorted(kinds):
        path = tmp_path / f"{kind}.txt"
        path.write_text(f"reviewed {kind}")
        items.append(fingerprint(kind, path))
    return items


def test_resolution_approval_resolves_the_exact_terminal_claim(setup, tmp_path):
    clock, _, posts, journal = setup
    order, _ = terminal(setup)
    built = checkpoint_approval(
        journal,
        "resolution",
        order.client_id,
        evidence(tmp_path, EVIDENCE_KINDS),
        minutes=10,
        now=clock.now,
    )
    result = journal.resolve_order_claim(
        order.client_id, built, confirmations=RESOLUTION_CONFIRMATIONS
    )
    assert result["post_claim_resolved"]


def test_cancel_approval_authorizes_one_restricted_cancel(setup, tmp_path):
    clock, _, _, journal = setup
    order, _ = working(setup)
    journal.halt()
    built = checkpoint_approval(
        journal,
        "cancel",
        order.client_id,
        evidence(tmp_path, CANCEL_EVIDENCE_KINDS),
        minutes=5,
        now=clock.now,
    )
    token = journal.authorize_cancel(order.client_id, built, confirmations=CANCEL_CONFIRMATIONS)
    assert isinstance(token, str) and len(token) == 64


def test_restart_approval_restarts_with_the_reported_confirmations(setup, tmp_path):
    setup = stopped(setup)
    clock, _, _, journal = setup
    review = tmp_path / "stop review.md"
    review.write_text("operator stop reviewed")
    built, confirmations = restart_approval(
        journal, evidence(tmp_path, EVIDENCE_KINDS), review, hours=24, now=clock.now
    )
    assert built.stop_review_reference == "stop review.md"
    journal.restart(built, confirmations=confirmations)
    # The restart advanced the POST generation; only reopened stores may be used.
    assert reopen(setup)[3].snapshot()["live_enabled"]


@pytest.mark.parametrize("minutes", [0, 11, True])
def test_checkpoint_approval_lifetime_and_kinds_are_bounded(setup, tmp_path, minutes):
    clock, _, _, journal = setup
    with pytest.raises(LiveAcceptanceError, match="approval_minutes_out_of_range"):
        checkpoint_approval(journal, "cancel", "Buy001", [], minutes=minutes, now=clock.now)
    with pytest.raises(LiveAcceptanceError, match="invalid_checkpoint_approval_kind"):
        checkpoint_approval(journal, "restart", "Buy001", [], minutes=5, now=clock.now)
