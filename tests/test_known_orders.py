"""Immutable declarations, ownership binding, bounded storage and crash-safe append."""

import json
import sqlite3
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from datetime import UTC, datetime, timedelta

import pytest

from trading.broker_contracts import OrderIntent
from trading.known_orders import CatalogError, KnownOrder, KnownOrderCatalog


def order(identity=201, client="Known", units=1000):
    return KnownOrder(
        order_id=identity,
        intent=OrderIntent(
            client_id=client, side="BUY", effect="OPEN", units=units, kind="LIMIT", price="150"
        ),
    )


@pytest.fixture
def setup(tmp_path):
    clock = [datetime(2026, 10, 2, tzinfo=UTC)]
    catalog = KnownOrderCatalog.create(
        tmp_path / "catalog", "synthetic", "a" * 32, "b" * 64, (), clock=lambda: clock[0]
    )
    return clock, catalog


def reopen(catalog):
    return KnownOrderCatalog(catalog.path.parent, "synthetic", "a" * 32, "b" * 64, ())


def register(catalog, declaration=None, **changes):
    return catalog.register(
        declaration or order(),
        source_ref="broker-export-reviewed",
        intent_confirmed=True,
        expected_head=catalog.snapshot()["head"],
        **changes,
    )


def test_append_reopen_lookup_and_idempotence_keep_the_original_declaration(setup):
    _, catalog = setup
    before = catalog.snapshot()["head"]
    first = register(catalog)
    assert first["head"] != before and first["records"] == 1
    assert first["complete"] is False and not first["already_known"]
    saved = catalog.path.read_bytes()
    second = register(catalog)
    assert second["head"] == first["head"] and second["already_known"]
    assert catalog.path.read_bytes() == saved
    peer = reopen(catalog)
    assert peer.lookup((201,)) == {201: order().intent}
    with pytest.raises(FileExistsError):
        KnownOrderCatalog.create(catalog.path.parent, "synthetic", "a" * 32, "b" * 64, ())


def test_seeded_plan_is_verified_and_cannot_be_replaced(tmp_path):
    seeds = (order(),)
    catalog = KnownOrderCatalog.create(tmp_path / "seed", "synthetic", "a" * 32, "b" * 64, seeds)
    assert catalog.lookup((201,))[201] == seeds[0].intent
    for scope, control, plan, initial in (
        ("other", "a" * 32, "b" * 64, seeds),
        ("synthetic", "c" * 32, "b" * 64, seeds),
        ("synthetic", "a" * 32, "c" * 64, seeds),
        ("synthetic", "a" * 32, "b" * 64, ()),
    ):
        with pytest.raises(CatalogError, match="integrity_failed"):
            KnownOrderCatalog(catalog.path.parent, scope, control, plan, initial)


def test_conflicting_order_or_client_is_rejected_without_mutating_the_catalog(setup):
    _, catalog = setup
    register(catalog)
    saved = catalog.path.read_bytes()
    for candidate, reason in (
        (order(units=2000), "order_conflict"),
        (order(202), "client_conflict"),
    ):
        with pytest.raises(CatalogError, match=reason):
            register(catalog, candidate)
        assert catalog.path.read_bytes() == saved


def test_confirmed_source_and_current_head_are_required(setup):
    _, catalog = setup
    head = catalog.snapshot()["head"]
    with pytest.raises(CatalogError, match="confirmation_required"):
        catalog.register(order(), source_ref="reviewed", expected_head=head)
    with pytest.raises(CatalogError, match="invalid_catalog_declaration"):
        catalog.register(order(), source_ref="secret\n", expected_head=head, intent_confirmed=True)
    register(catalog)
    with pytest.raises(CatalogError, match="head_changed"):
        catalog.register(
            order(202, "Next"), source_ref="reviewed", expected_head=head, intent_confirmed=True
        )
    assert catalog.snapshot()["records"] == 1


def test_unknown_lookup_and_backward_clock_do_not_add_records(setup):
    clock, catalog = setup
    with pytest.raises(CatalogError, match="order_unknown"):
        catalog.lookup((999,))
    with pytest.raises(CatalogError, match="invalid_catalog_lookup"):
        catalog.lookup((True,))
    clock[0] -= timedelta(seconds=1)
    with pytest.raises(CatalogError, match="clock_invalid"):
        register(catalog)
    assert catalog.snapshot()["records"] == 0


def test_concurrent_expected_head_registration_has_one_winner(setup):
    _, catalog = setup
    head = catalog.snapshot()["head"]

    def attempt(index):
        try:
            return reopen(catalog).register(
                order(201 + index, f"Known{index}"),
                source_ref="reviewed",
                expected_head=head,
                intent_confirmed=True,
            )
        except CatalogError as error:
            return str(error)

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(attempt, (0, 1)))
    assert sum(isinstance(r, dict) for r in results) == 1
    assert "catalog_head_changed" in results and catalog.snapshot()["records"] == 1


@pytest.mark.parametrize("change", ["body", "digest", "count", "delete", "instance"])
def test_corruption_is_rejected_and_never_repaired(setup, change):
    _, catalog = setup
    register(catalog)
    with closing(sqlite3.connect(catalog.path)) as conn:
        if change in {"count", "instance"}:
            state = json.loads(conn.execute("SELECT body FROM catalog").fetchone()[0])
            state["records" if change == "count" else "instance"] = (
                0 if change == "count" else "f" * 32
            )
            conn.execute(
                "UPDATE catalog SET body=?",
                (json.dumps(state, sort_keys=True, separators=(",", ":")),),
            )
        elif change == "delete":
            conn.execute("DELETE FROM orders")
        else:
            conn.execute(f"UPDATE orders SET {change}=?", ("corrupt",))
        conn.commit()
    with pytest.raises(CatalogError, match="integrity_failed"):
        catalog.snapshot()
    with pytest.raises(CatalogError, match="failed_closed"):
        register(catalog)


def test_missing_store_is_not_recreated(setup):
    _, catalog = setup
    catalog.path.rename(catalog.path.with_suffix(".saved"))
    with pytest.raises(CatalogError, match="storage_failed"):
        reopen(catalog)
    assert not catalog.path.exists()


def test_capacity_refusal_preserves_all_prior_declarations(setup, monkeypatch):
    _, catalog = setup
    monkeypatch.setattr("trading.known_orders.MAX_ORDERS", 1)
    register(catalog)
    with pytest.raises(CatalogError, match="capacity_exceeded"):
        register(catalog, order(202, "Next"))
    assert catalog.lookup((201,))[201] == order().intent and catalog.snapshot()["records"] == 1


@pytest.mark.parametrize("stage,code", [("before", 71), ("after", 72)])
def test_process_exit_before_or_after_commit_never_duplicates_the_declaration(setup, stage, code):
    _, catalog = setup
    script = r"""
import os,sys
from contextlib import contextmanager
from datetime import UTC,datetime
from trading.known_orders import KnownOrderCatalog,KnownOrder
from trading.broker_contracts import OrderIntent
c=KnownOrderCatalog(sys.argv[1],"synthetic","a"*32,"b"*64,(),clock=lambda:datetime(2026,10,4,tzinfo=UTC))
original=KnownOrderCatalog._transaction
@contextmanager
def crash(self):
    with original(self) as conn:
        yield conn
        if sys.argv[2]=="before": os._exit(71)
    os._exit(72)
KnownOrderCatalog._transaction=crash
c.register(KnownOrder(order_id=201,intent=OrderIntent(client_id="Known",side="BUY",effect="OPEN",units=1000,kind="LIMIT",price="150")),source_ref="reviewed",expected_head=sys.argv[3],intent_confirmed=True)
"""
    result = subprocess.run(
        [sys.executable, "-c", script, str(catalog.path.parent), stage, catalog.snapshot()["head"]],
        capture_output=True,
        timeout=10,
    )
    assert result.returncode == code
    peer = reopen(catalog)
    assert peer.snapshot()["records"] == (0 if stage == "before" else 1)
    register(peer)
    assert peer.snapshot()["records"] == 1 and peer.lookup((201,))[201] == order().intent
