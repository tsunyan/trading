"""Append-only operator declarations. Broker evidence is still required for booking."""

import hashlib
import json
import sqlite3
import time
import uuid
from contextlib import closing, contextmanager
from datetime import UTC, datetime
from pathlib import Path
from typing import Literal

from pydantic import AwareDatetime, Field, model_validator

from trading.broker_contracts import Contract, OrderIntent
from trading.storage_init import new_storage_directory

MAX_ORDERS = 10_000
MAX_RECORD_BYTES = 4096
MAX_BYTES = 32_000_000
SCHEMA = """
CREATE TABLE catalog (id INTEGER PRIMARY KEY CHECK(id=1),body TEXT NOT NULL);
CREATE TABLE orders (
 id INTEGER PRIMARY KEY,order_id INTEGER NOT NULL UNIQUE,client_id TEXT NOT NULL UNIQUE,
 body TEXT NOT NULL,digest TEXT NOT NULL
);
"""


class CatalogError(ValueError):
    """Fixed local codes only."""


class _CatalogBusy(CatalogError):
    """A rolled-back transaction may be retried without adding a second record."""


class KnownOrder(Contract):
    order_id: int = Field(strict=True, gt=0, lt=2**63)
    intent: OrderIntent


class Declaration(Contract):
    order: KnownOrder
    source_ref: str = Field(min_length=1, max_length=128)
    registered_at: AwareDatetime

    @model_validator(mode="after")
    def printable(self):
        if any(ord(c) < 32 or ord(c) == 127 for c in self.source_ref):
            raise ValueError("invalid source reference")
        return self


class CatalogState(Contract):
    version: Literal[1] = 1
    instance: str = Field(pattern=r"^[a-f0-9]{32}$")
    scope: str = Field(pattern=r"^[A-Za-z0-9_-]{1,64}$")
    control_instance: str = Field(pattern=r"^[a-f0-9]{32}$")
    plan_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    seed_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    created_at: AwareDatetime
    records: int = Field(strict=True, ge=0, le=MAX_ORDERS)
    bytes: int = Field(strict=True, ge=0, le=MAX_BYTES)
    head: str = Field(pattern=r"^[a-f0-9]{64}$")


def _body(model):
    return json.dumps(model.model_dump(mode="json"), sort_keys=True, separators=(",", ":"))


def _hash(value):
    return hashlib.sha256(value.encode()).hexdigest()


def _seed(state):
    return _hash(_body(state.model_copy(update={"records": 0, "bytes": 0, "head": "0" * 64})))


def _next(previous, index, body):
    return _hash(json.dumps([previous, index, body], separators=(",", ":")))


class KnownOrderCatalog:
    """One immutable intent per broker ID/client ID, including across process restarts.

    Hashes check accidental corruption, not hostile SQL edits or backup rollback.
    Registration may run during capture: it only adds mappings, never replaces one.
    """

    def __init__(self, directory, scope, control_instance, plan_sha256, seeds, *, clock=None):
        self.path = Path(directory).resolve() / "known-orders.sqlite"
        self.scope, self.control_instance, self.plan_sha256 = scope, control_instance, plan_sha256
        self.seeds = tuple(KnownOrder.model_validate(o.model_dump()) for o in seeds)
        self._seed_hash = _hash(json.dumps([_body(o) for o in self.seeds], separators=(",", ":")))
        self._clock = clock or (lambda: datetime.now(UTC))
        self._instance = None
        self._failed = False
        state, _ = self._read()
        self._instance = state.instance

    @classmethod
    def create(cls, directory, scope, control_instance, plan_sha256, seeds, *, clock=None):
        clock = clock or (lambda: datetime.now(UTC))
        seeds = tuple(KnownOrder.model_validate(o.model_dump()) for o in seeds)
        state = CatalogState(
            instance=uuid.uuid4().hex,
            scope=scope,
            control_instance=control_instance,
            plan_sha256=plan_sha256,
            seed_sha256=_hash(json.dumps([_body(o) for o in seeds], separators=(",", ":"))),
            created_at=clock(),
            records=0,
            bytes=0,
            head="0" * 64,
        )
        state = state.model_copy(update={"head": _seed(state)})
        declarations = tuple(
            Declaration(order=o, source_ref="frozen_plan", registered_at=state.created_at)
            for o in seeds
        )
        if len(seeds) > MAX_ORDERS or any(
            len(_body(d).encode()) > MAX_RECORD_BYTES for d in declarations
        ):
            raise CatalogError("catalog_capacity_exceeded")
        directory = Path(directory).resolve()
        with new_storage_directory(
            directory, ("known-orders.sqlite-journal", "known-orders.sqlite")
        ):
            try:
                with closing(sqlite3.connect(directory / "known-orders.sqlite")) as conn:
                    conn.execute("PRAGMA synchronous=FULL")
                    conn.executescript(SCHEMA)
                    for declaration in declarations:
                        body = _body(declaration)
                        state = state.model_copy(
                            update={
                                "records": state.records + 1,
                                "bytes": state.bytes + len(body.encode()),
                                "head": _next(state.head, state.records + 1, body),
                            }
                        )
                        conn.execute(
                            "INSERT INTO orders VALUES(?,?,?,?,?)",
                            (
                                state.records,
                                declaration.order.order_id,
                                declaration.order.intent.client_id,
                                body,
                                state.head,
                            ),
                        )
                    conn.execute("INSERT INTO catalog VALUES(1,?)", (_body(state),))
                    conn.commit()
            except (sqlite3.Error, OSError, ValueError):
                raise CatalogError("catalog_initialization_failed") from None
            return cls(directory, scope, control_instance, plan_sha256, seeds, clock=clock)

    @contextmanager
    def _transaction(self):
        if self._failed:
            raise CatalogError("catalog_failed_closed")
        try:
            with closing(
                sqlite3.connect(self.path.as_uri() + "?mode=rw", uri=True, timeout=1)
            ) as conn:
                conn.execute("PRAGMA synchronous=FULL")
                conn.execute("BEGIN IMMEDIATE")
                try:
                    yield conn
                    conn.commit()
                except BaseException:
                    conn.rollback()
                    raise
        except sqlite3.Error as error:
            if getattr(error, "sqlite_errorcode", 0) & 0xFF in {
                sqlite3.SQLITE_BUSY,
                sqlite3.SQLITE_LOCKED,
            }:
                raise _CatalogBusy("catalog_busy") from None
            self._failed = True
            raise CatalogError("catalog_storage_failed") from None
        except OSError:
            self._failed = True
            raise CatalogError("catalog_storage_failed") from None

    @staticmethod
    def _retry(operation):
        for attempt in range(3):
            try:
                return operation()
            except _CatalogBusy:
                if attempt == 2:
                    raise
                time.sleep(0.05)

    def _read(self):
        def read():
            with self._transaction() as conn:
                return self._verify(conn)

        return self._retry(read)

    def _verify(self, conn):
        try:
            headers = conn.execute(
                "SELECT id,length(CAST(body AS BLOB)) FROM catalog LIMIT 2"
            ).fetchall()
            if len(headers) != 1 or headers[0][0] != 1 or not 1 <= headers[0][1] <= 4096:
                raise ValueError
            body = conn.execute("SELECT body FROM catalog").fetchone()[0]
            state = CatalogState.model_validate_json(body)
            if _body(state) != body or (
                state.scope,
                state.control_instance,
                state.plan_sha256,
                state.seed_sha256,
            ) != (self.scope, self.control_instance, self.plan_sha256, self._seed_hash):
                raise ValueError
            if self._instance is not None and state.instance != self._instance:
                raise ValueError
            rows = conn.execute(
                "SELECT id,order_id,client_id,length(CAST(body AS BLOB)),digest "
                "FROM orders ORDER BY id LIMIT ?",
                (MAX_ORDERS + 1,),
            ).fetchall()
            if len(rows) != state.records or len(rows) < len(self.seeds):
                raise ValueError
            if any(not 1 <= row[3] <= MAX_RECORD_BYTES for row in rows):
                raise ValueError
            if sum(row[3] for row in rows) != state.bytes:
                raise ValueError
            bodies = conn.execute(
                "SELECT body FROM orders ORDER BY id LIMIT ?", (MAX_ORDERS + 1,)
            ).fetchall()
            head, orders, clients, previous_at = _seed(state), {}, set(), state.created_at
            for index, (row, raw) in enumerate(zip(rows, bodies, strict=True), 1):
                declaration = Declaration.model_validate_json(raw[0])
                order = declaration.order
                head = _next(head, index, raw[0])
                if (
                    row[:3] != (index, order.order_id, order.intent.client_id)
                    or row[4] != head
                    or _body(declaration) != raw[0]
                    or order.order_id in orders
                    or order.intent.client_id in clients
                    or declaration.registered_at < previous_at
                    or (
                        index <= len(self.seeds)
                        and (
                            order != self.seeds[index - 1]
                            or declaration.source_ref != "frozen_plan"
                        )
                    )
                ):
                    raise ValueError
                orders[order.order_id] = declaration
                clients.add(order.intent.client_id)
                previous_at = declaration.registered_at
            if state.head != head:
                raise ValueError
            return state, orders
        except (ValueError, TypeError, KeyError):
            self._failed = True
            raise CatalogError("catalog_integrity_failed") from None

    def snapshot(self):
        state, _ = self._read()
        return {"instance": state.instance, **self.snapshot_result(state)}

    def lookup(self, ids):
        if (
            not isinstance(ids, tuple)
            or len(ids) > 1000
            or any(type(i) is not int or i <= 0 for i in ids)
        ):
            raise CatalogError("invalid_catalog_lookup")
        _, orders = self._read()
        if any(i not in orders for i in ids):
            raise CatalogError("catalog_order_unknown")
        return {i: orders[i].order.intent for i in ids}

    def register(self, order, *, source_ref, expected_head, intent_confirmed=False):
        if intent_confirmed is not True:
            raise CatalogError("catalog_intent_confirmation_required")
        try:
            order = KnownOrder.model_validate(order.model_dump())
            declaration = Declaration(
                order=order, source_ref=source_ref, registered_at=self._clock()
            )
            body = _body(declaration)
            if len(body.encode()) > MAX_RECORD_BYTES:
                raise ValueError
        except Exception:
            raise CatalogError("invalid_catalog_declaration") from None
        return self._retry(lambda: self._register(declaration, body, expected_head))

    def _register(self, declaration, body, expected_head):
        order = declaration.order
        with self._transaction() as conn:
            state, orders = self._verify(conn)
            if expected_head != state.head:
                raise CatalogError("catalog_head_changed")
            if order.order_id in orders:
                if orders[order.order_id].order != order:
                    raise CatalogError("catalog_order_conflict")
                return {
                    **self.snapshot_result(state),
                    "registered_order_id": order.order_id,
                    "already_known": True,
                }
            if any(d.order.intent.client_id == order.intent.client_id for d in orders.values()):
                raise CatalogError("catalog_client_conflict")
            last_at = list(orders.values())[-1].registered_at if orders else state.created_at
            if declaration.registered_at < last_at:
                raise CatalogError("catalog_clock_invalid")
            if state.records >= MAX_ORDERS or state.bytes + len(body.encode()) > MAX_BYTES:
                raise CatalogError("catalog_capacity_exceeded")
            updated = state.model_copy(
                update={
                    "records": state.records + 1,
                    "bytes": state.bytes + len(body.encode()),
                    "head": _next(state.head, state.records + 1, body),
                }
            )
            conn.execute(
                "INSERT INTO orders VALUES(?,?,?,?,?)",
                (
                    updated.records,
                    order.order_id,
                    order.intent.client_id,
                    body,
                    updated.head,
                ),
            )
            conn.execute("UPDATE catalog SET body=? WHERE id=1", (_body(updated),))
            return {
                **self.snapshot_result(updated),
                "registered_order_id": order.order_id,
                "already_known": False,
            }

    @staticmethod
    def snapshot_result(state):
        return {
            "head": state.head,
            "records": state.records,
            "complete": False,
            "live_enabled": False,
        }
