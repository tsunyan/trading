"""Local, balanced cash postings for individually matched WS/REST executions.

One database is one explicitly declared scope and opening cash boundary. This
is not a broker account snapshot, position ledger, or proof of complete history.
"""

import hashlib
import json
import re
import sqlite3
import uuid
from contextlib import closing, contextmanager
from datetime import UTC, datetime
from decimal import Decimal, localcontext
from pathlib import Path
from typing import Literal

from pydantic import AwareDatetime, Field

from trading.account_events import AccountEvent
from trading.account_reader import AccountReadReport, OrderReadReport
from trading.broker_contracts import Contract, Execution, OrderIntent, Units
from trading.execution_reconciliation import reconcile_executions
from trading.storage_init import new_storage_directory
from trading.wire_validation import clock_skew, unique_object

SCALE = 100_000_000
MAX_MONEY = Decimal("1000000000000000000")
MAX_ENTRIES = 5000
MAX_PROOF = 2_000_000
MAX_BYTES = 32_000_000
BLOCKERS = (
    "cash_book_scope_not_authenticated",
    "opening_cash_boundary_not_verified",
    "execution_history_not_proven",
    "external_cash_flows_not_reconciled",
    "position_accounting_not_applied",
    "broker_margin_fee_rounding_not_verified",
    "atomic_snapshot_not_proven",
)
SCHEMA = """
CREATE TABLE book (
 id INTEGER PRIMARY KEY CHECK(id=1), version INTEGER NOT NULL,
 instance TEXT NOT NULL, scope TEXT NOT NULL, opening TEXT NOT NULL,
 max_entries INTEGER NOT NULL, count INTEGER NOT NULL, proof_count INTEGER NOT NULL,
 bytes INTEGER NOT NULL, head TEXT NOT NULL, halted INTEGER NOT NULL, reason TEXT
);
CREATE TABLE proofs (id INTEGER PRIMARY KEY, body TEXT NOT NULL);
CREATE TABLE executions (
 sequence INTEGER PRIMARY KEY, execution_id INTEGER UNIQUE NOT NULL,
 proof_id INTEGER NOT NULL, body TEXT NOT NULL, digest TEXT NOT NULL
);
CREATE TABLE postings (
 sequence INTEGER NOT NULL, account TEXT NOT NULL, amount TEXT NOT NULL,
 PRIMARY KEY(sequence, account)
);
"""


class CashBookError(ValueError):
    """Fixed codes only; no input, SQL, credentials, or network exception text."""


class OpeningCash(Contract):
    balance: Decimal
    cutoff: AwareDatetime
    currency: Literal["JPY"] = "JPY"


class ExecutionCashBatch(Contract):
    events: tuple[AccountEvent, ...]
    reports: tuple[OrderReadReport, ...]
    clock_skew_ms: int = Field(default=0, strict=True, ge=0, le=1000)


class _Booking(Contract):
    order_id: Units
    root_order_id: Units
    intent: OrderIntent
    execution: Execution


def _minor(value):
    """Exact fixed scale without depending on the caller's Decimal context."""
    if not isinstance(value, Decimal) or not value.is_finite() or value.copy_abs() > MAX_MONEY:
        raise CashBookError("cash_book_money_out_of_range")
    sign, digits, exponent = value.as_tuple()
    if not any(digits):
        return 0
    digits = list(digits)
    while digits[-1] == 0:
        digits.pop()
        exponent += 1
    if exponent < -8:
        raise CashBookError("cash_book_money_precision")
    integer = int("".join(map(str, digits))) * 10 ** (exponent + 8)
    return -integer if sign else integer


def _money(integer):
    if abs(integer) > 10**26:
        raise CashBookError("cash_book_money_out_of_range")
    sign = "-" if integer < 0 else ""
    integer = abs(integer)
    return f"{sign}{integer // SCALE}.{integer % SCALE:08d}"


def _normalize(value):
    if isinstance(value, Contract):
        return _normalize(value.model_dump(warnings=False))
    if isinstance(value, Decimal):
        return _money(_minor(value))
    if isinstance(value, datetime):
        return value.astimezone(UTC).isoformat()
    if isinstance(value, dict):
        return {key: _normalize(item) for key, item in value.items()}
    if isinstance(value, (tuple, list)):
        return [_normalize(item) for item in value]
    return value


def _json(value):
    return json.dumps(_normalize(value), sort_keys=True, separators=(",", ":"), allow_nan=False)


def _load(body):
    return json.loads(body, object_pairs_hook=unique_object)


def _hash(body):
    return hashlib.sha256(body.encode("utf-8")).hexdigest()


def _legs(fill):
    loss, fee, swap = (_minor(fill.loss_gain), _minor(fill.fee), _minor(fill.settled_swap))
    if fee < 0:
        raise CashBookError("cash_book_negative_fee_debit")
    return {
        "cash": loss - fee + swap,
        "realized_pnl": -loss,
        "fee_expense": fee,
        "settled_swap": -swap,
    }


def _booking(report, fill):
    evidence = report.evidence
    # Settlement ordering is irrelevant to intent identity.
    intent = evidence.intent.model_copy(
        update={"positions": tuple(sorted(evidence.intent.positions, key=lambda p: p.position_id))}
    )
    for identity in (
        evidence.order_id,
        evidence.root_order_id,
        fill.execution_id,
        fill.position_id,
    ):
        if not 1 <= identity < 2**63:
            raise CashBookError("cash_book_identity_out_of_range")
    if intent.units > 10**12 or fill.units > 10**12:
        raise CashBookError("cash_book_quantity_out_of_range")
    return _Booking(
        order_id=evidence.order_id,
        root_order_id=evidence.root_order_id,
        intent=intent,
        execution=fill,
    )


def _batch(source):
    if not isinstance(source, ExecutionCashBatch):
        raise CashBookError("cash_book_batch_required")
    try:
        clock_skew(source.clock_skew_ms)
    except ValueError:
        raise CashBookError("cash_book_input_invalid") from None
    if not 1 <= len(source.events) <= 2000 or not 1 <= len(source.reports) <= 1000:
        raise CashBookError("cash_book_batch_capacity")
    if sum(len(r.evidence.executions) for r in source.reports) > 10_000:
        raise CashBookError("cash_book_batch_capacity")
    # Bound / normalize every numeric field before the Decimal calculations in
    # reconciliation and before serialization (including caller-constructed models).
    body = _json(source)
    if len(body.encode("utf-8")) > MAX_PROOF:
        raise CashBookError("cash_book_proof_capacity")
    batch = ExecutionCashBatch.model_validate(_load(body))
    skew = clock_skew(batch.clock_skew_ms)
    for event in batch.events:
        if event.position is not None or event.order is not None or event.removed:
            raise CashBookError("cash_book_notice_invalid")
        if event.execution is None or event.occurred_at != event.execution.timestamp:
            raise CashBookError("cash_book_notice_invalid")
    for report in batch.reports:
        observations = report.observations
        if len(observations) != 4:
            raise CashBookError("cash_book_order_observations_invalid")
        previous_response = previous_receipt = None
        for index, observation in enumerate(observations):
            if (
                observation.path != ("/v1/orders" if index % 2 == 0 else "/v1/executions")
                or observation.query != (("orderId", str(report.evidence.order_id)),)
                or not re.fullmatch(r"[a-f0-9]{64}", observation.sha256)
                or observation.response_at > observation.received_at + skew
                or (previous_response is not None and observation.response_at < previous_response)
                or (previous_receipt is not None and observation.received_at < previous_receipt)
            ):
                raise CashBookError("cash_book_order_observations_invalid")
            previous_response, previous_receipt = observation.response_at, observation.received_at
        if report.evidence.observed_at != observations[-1].response_at:
            raise CashBookError("cash_book_order_observations_invalid")
    with localcontext() as context:
        context.prec = 80
        matched = reconcile_executions(batch.events, batch.reports)
    if matched.mismatches or matched.unverified_execution_ids:
        raise CashBookError("cash_book_executions_not_matched")
    selected = set(matched.matched_execution_ids)
    records, all_rest = {}, {}
    for report in batch.reports:
        for fill in report.evidence.executions:
            record = _booking(report, fill)
            all_rest[fill.execution_id] = record
            if fill.execution_id in selected:
                records[fill.execution_id] = record
    return body, records, all_rest


class ExecutionCashBook:
    """One transactional cash book. Never auto-books a resync aggregate / replay.

    UNIQUE execution IDs and cash legs commit atomically with their comparison
    evidence. Every read / write verifies the bounded chain and derived postings.
    Hashes detect inconsistency, not hostile SQL edits or restoring an old DB copy.
    """

    def __init__(self, directory: Path, scope: str):
        if not isinstance(scope, str) or not re.fullmatch(r"[A-Za-z0-9_-]{1,64}", scope):
            raise CashBookError("invalid_cash_book_scope")
        self.path = Path(directory).resolve() / "execution-cash.sqlite"
        self.scope, self._instance, self._failed = scope, None, False
        with self._transaction() as conn:
            meta, _, _ = self._verify(conn)
            self._instance = meta["instance"]

    @classmethod
    def create(cls, directory: Path, scope: str, opening: OpeningCash, *, max_entries=MAX_ENTRIES):
        if not isinstance(scope, str) or not re.fullmatch(r"[A-Za-z0-9_-]{1,64}", scope):
            raise CashBookError("invalid_cash_book_scope")
        if type(max_entries) is not int or not 1 <= max_entries <= MAX_ENTRIES:
            raise CashBookError("invalid_cash_book_capacity")
        try:
            opening = OpeningCash.model_validate(opening.model_dump())
            opening_body = _json(opening)
            cash = _minor(opening.balance)
        except Exception:
            raise CashBookError("invalid_cash_book_opening") from None
        directory = Path(directory).resolve()
        with new_storage_directory(
            directory, ("execution-cash.sqlite-journal", "execution-cash.sqlite")
        ):
            try:
                instance = uuid.uuid4().hex
                head = cls._seed(instance, scope, opening_body, max_entries)
                with closing(sqlite3.connect(directory / "execution-cash.sqlite")) as conn:
                    conn.execute("PRAGMA synchronous=FULL")
                    conn.executescript(SCHEMA)
                    conn.execute(
                        "INSERT INTO book VALUES(1,1,?,?,?,?,0,0,0,?,0,NULL)",
                        (instance, scope, opening_body, max_entries, head),
                    )
                    conn.executemany(
                        "INSERT INTO postings VALUES(0,?,?)",
                        (("cash", str(cash)), ("opening_equity", str(-cash))),
                    )
                    conn.commit()
            except (sqlite3.Error, OSError):
                raise CashBookError("cash_book_initialization_failed") from None
            return cls(directory, scope)

    @staticmethod
    def _seed(instance, scope, opening, capacity):
        return _hash(
            _json(
                {
                    "instance": instance,
                    "scope": scope,
                    "opening": opening,
                    "capacity": capacity,
                    "version": 1,
                }
            )
        )

    @staticmethod
    def _digest(previous, sequence, identity, proof_id, proof, body):
        return _hash(
            _json(
                {
                    "previous": previous,
                    "sequence": sequence,
                    "execution_id": identity,
                    "proof_id": proof_id,
                    "proof_hash": _hash(proof),
                    "body": body,
                }
            )
        )

    @contextmanager
    def _transaction(self, *, write=False):
        if self._failed:
            raise CashBookError("cash_book_failed_closed")
        try:
            with closing(
                sqlite3.connect(
                    self.path.as_uri() + ("?mode=rw" if write else "?mode=ro"), uri=True, timeout=1
                )
            ) as conn:
                conn.row_factory = sqlite3.Row
                if write:
                    conn.execute("PRAGMA synchronous=FULL")
                conn.execute("BEGIN IMMEDIATE" if write else "BEGIN")
                try:
                    yield conn
                    conn.commit()
                except BaseException:
                    conn.rollback()
                    raise
        except (sqlite3.Error, OSError):
            self._failed = True
            raise CashBookError("cash_book_storage_failed") from None

    def _verify(self, conn):
        try:
            metas = conn.execute("SELECT * FROM book LIMIT 2").fetchall()
            if len(metas) != 1:
                raise ValueError
            meta = dict(metas[0])
            if (
                meta["id"] != 1
                or meta["version"] != 1
                or meta["scope"] != self.scope
                or not re.fullmatch(r"[a-f0-9]{32}", meta["instance"])
                or (self._instance is not None and meta["instance"] != self._instance)
                or not 1 <= meta["max_entries"] <= MAX_ENTRIES
                or not 0 <= meta["count"] <= meta["max_entries"]
                or not 0 <= meta["proof_count"] <= meta["count"]
                or not 0 <= meta["bytes"] <= MAX_BYTES
                or meta["halted"] not in (0, 1)
                or meta["reason"] != ("cash_book_identity_conflict" if meta["halted"] else None)
            ):
                raise ValueError
            opening = OpeningCash.model_validate(_load(meta["opening"]))
            if meta["opening"] != _json(opening):
                raise ValueError
            expected_legs = {
                (0, "cash"): str(_minor(opening.balance)),
                (0, "opening_equity"): str(-_minor(opening.balance)),
            }
            # Check counts and aggregate byte sizes before loading proof bodies.
            # A corrupted database must not turn the bounded verifier into an
            # unbounded allocation simply by containing large rows.
            for table, count, bound in (
                ("proofs", meta["proof_count"], MAX_PROOF),
                ("executions", meta["count"], MAX_PROOF),
            ):
                dimensions = conn.execute(
                    f"SELECT count(*),coalesce(sum(length(CAST(body AS BLOB))),0),"
                    f"coalesce(max(length(CAST(body AS BLOB))),0) FROM {table}"
                ).fetchone()
                if dimensions[0] != count or dimensions[1] > MAX_BYTES or dimensions[2] > bound:
                    raise ValueError
            proofs = conn.execute(
                "SELECT * FROM proofs ORDER BY id LIMIT ?", (MAX_ENTRIES + 1,)
            ).fetchall()
            if len(proofs) != meta["proof_count"]:
                raise ValueError
            evidence, size = {}, 0
            for index, row in enumerate(proofs, 1):
                if row["id"] != index or len(row["body"].encode()) > MAX_PROOF:
                    raise ValueError
                size += len(row["body"].encode())
                if size > MAX_BYTES:
                    raise ValueError
                body, records, _ = _batch(ExecutionCashBatch.model_validate(_load(row["body"])))
                if body != row["body"]:
                    raise ValueError
                evidence[index] = (body, records)
            rows = conn.execute(
                "SELECT * FROM executions ORDER BY sequence LIMIT ?", (MAX_ENTRIES + 1,)
            ).fetchall()
            if len(rows) != meta["count"]:
                raise ValueError
            previous = self._seed(
                meta["instance"], self.scope, meta["opening"], meta["max_entries"]
            )
            records, used_proofs = {}, set()
            totals = {
                "cash": _minor(opening.balance),
                "loss_gain": 0,
                "fee_debit": 0,
                "settled_swap": 0,
            }
            order_keys, client_ids, order_units = {}, {}, {}
            for index, row in enumerate(rows, 1):
                proof_id, identity = row["proof_id"], row["execution_id"]
                proof_body, matched = evidence[proof_id]
                record = _Booking.model_validate(_load(row["body"]))
                if (
                    row["sequence"] != index
                    or identity != record.execution.execution_id
                    or identity in records
                    or record != matched[identity]
                    or row["body"] != _json(record)
                    or record.execution.timestamp <= opening.cutoff
                ):
                    raise ValueError
                order_key = _json({"intent": record.intent, "root_order_id": record.root_order_id})
                if record.order_id in order_keys and order_keys[record.order_id] != order_key:
                    raise ValueError
                if (
                    record.intent.client_id in client_ids
                    and client_ids[record.intent.client_id] != record.order_id
                ):
                    raise ValueError
                order_keys[record.order_id], client_ids[record.intent.client_id] = (
                    order_key,
                    record.order_id,
                )
                order_units[record.order_id] = (
                    order_units.get(record.order_id, 0) + record.execution.units
                )
                if order_units[record.order_id] > record.intent.units:
                    raise ValueError
                previous = self._digest(
                    previous, index, identity, proof_id, proof_body, row["body"]
                )
                if previous != row["digest"]:
                    raise ValueError
                size += len(row["body"].encode())
                legs = _legs(record.execution)
                expected_legs.update({(index, key): str(value) for key, value in legs.items()})
                totals["cash"] += legs["cash"]
                totals["loss_gain"] -= legs["realized_pnl"]
                totals["fee_debit"] += legs["fee_expense"]
                totals["settled_swap"] -= legs["settled_swap"]
                records[identity] = record
                used_proofs.add(proof_id)
            posted = conn.execute(
                "SELECT * FROM postings LIMIT ?", (4 * MAX_ENTRIES + 3,)
            ).fetchall()
            actual_legs = {(r["sequence"], r["account"]): r["amount"] for r in posted}
            if (
                actual_legs != expected_legs
                or len(actual_legs) != len(posted)
                or used_proofs != evidence.keys()
                or size != meta["bytes"]
                or size > MAX_BYTES
                or previous != meta["head"]
            ):
                raise ValueError
            for value in totals.values():
                _money(value)
            return meta, records, totals
        except Exception:
            self._failed = True
            raise CashBookError("cash_book_integrity_failed") from None

    def apply(self, source: ExecutionCashBatch):
        """All matched new executions, their proof, and four cash legs commit together.

        Historical proof need not be fresh now; this doesn't authorize trading.
        Caller must deliberately supply the notices and known-order REST reports.
        """
        try:
            proof, selected, rest = _batch(source)
        except CashBookError:
            raise
        except Exception:
            raise CashBookError("cash_book_input_invalid") from None
        conflict = False
        with self._transaction(write=True) as conn:
            meta, existing, totals = self._verify(conn)
            if meta["halted"]:
                raise CashBookError("cash_book_halted")
            opening = OpeningCash.model_validate(_load(meta["opening"]))
            if any(record.execution.timestamp <= opening.cutoff for record in selected.values()):
                raise CashBookError("cash_book_before_opening_boundary")
            order_keys = {r.order_id: (r.root_order_id, r.intent) for r in existing.values()}
            clients = {r.intent.client_id: r.order_id for r in existing.values()}
            for identity, record in rest.items():
                if (
                    (identity in existing and record != existing[identity])
                    or (
                        record.order_id in order_keys
                        and order_keys[record.order_id] != (record.root_order_id, record.intent)
                    )
                    or (
                        record.intent.client_id in clients
                        and clients[record.intent.client_id] != record.order_id
                    )
                ):
                    conflict = True
                order_keys[record.order_id] = (record.root_order_id, record.intent)
                clients[record.intent.client_id] = record.order_id
            requested = {r.order_id for r in rest.values()}
            if conflict:
                # Persist the stop without changing the last consistent postings.
                conn.execute(
                    "UPDATE book SET halted=1,reason='cash_book_identity_conflict' WHERE id=1"
                )
            else:
                if any(
                    r.order_id in requested and identity not in rest
                    for identity, r in existing.items()
                ):
                    raise CashBookError("cash_book_known_history_missing")
                new = {
                    identity: selected[identity]
                    for identity in sorted(selected.keys() - existing.keys())
                }
                if meta["count"] + len(new) > meta["max_entries"]:
                    raise CashBookError("cash_book_capacity_reached")
                size = meta["bytes"] + (len(proof.encode()) if new else 0)
                size += sum(len(_json(record).encode()) for record in new.values())
                if size > MAX_BYTES:
                    raise CashBookError("cash_book_capacity_reached")
                cash_delta = sum(_legs(r.execution)["cash"] for r in new.values())
                _money(totals["cash"] + cash_delta)
                for key, field in (
                    ("loss_gain", "loss_gain"),
                    ("fee_debit", "fee"),
                    ("settled_swap", "settled_swap"),
                ):
                    _money(
                        totals[key] + sum(_minor(getattr(r.execution, field)) for r in new.values())
                    )
                proof_id, sequence, head = meta["proof_count"] + 1, meta["count"], meta["head"]
                if new:
                    conn.execute("INSERT INTO proofs VALUES(?,?)", (proof_id, proof))
                    for identity, record in new.items():
                        sequence += 1
                        body = _json(record)
                        head = self._digest(head, sequence, identity, proof_id, proof, body)
                        conn.execute(
                            "INSERT INTO executions VALUES(?,?,?,?,?)",
                            (sequence, identity, proof_id, body, head),
                        )
                        conn.executemany(
                            "INSERT INTO postings VALUES(?,?,?)",
                            (
                                (sequence, key, str(value))
                                for key, value in _legs(record.execution).items()
                            ),
                        )
                    conn.execute(
                        "UPDATE book SET count=?,proof_count=?,bytes=?,head=? WHERE id=1",
                        (sequence, proof_id, size, head),
                    )
                result = {
                    "instance": meta["instance"],
                    "scope": self.scope,
                    "head": head,
                    "applied_execution_ids": tuple(new),
                    "already_applied_execution_ids": tuple(
                        sorted(selected.keys() & existing.keys())
                    ),
                    "cash_delta": _money(cash_delta),
                    "balance": _money(totals["cash"] + cash_delta),
                    "accounting_applied": True,
                    "complete": False,
                    "live_enabled": False,
                    "blockers": BLOCKERS,
                }
        if conflict:
            raise CashBookError("cash_book_identity_conflict")
        return result

    def snapshot(self):
        with self._transaction() as conn:
            meta, records, totals = self._verify(conn)
        return {
            "instance": meta["instance"],
            "scope": self.scope,
            "head": meta["head"],
            "opening": _load(meta["opening"]),
            "executions": len(records),
            "execution_ids": tuple(sorted(records)),
            "proofs": meta["proof_count"],
            "halted": bool(meta["halted"]),
            "reason": meta["reason"],
            "balance": _money(totals["cash"]),
            "loss_gain": _money(totals["loss_gain"]),
            "fee_debit": _money(totals["fee_debit"]),
            "settled_swap": _money(totals["settled_swap"]),
            "complete": False,
            "live_enabled": False,
            "blockers": BLOCKERS,
        }

    def compare_balance(self, source: AccountReadReport, *, clock_skew_ms=0):
        """Non-persistent comparison only; never adjust cash to match an account."""
        try:
            skew = clock_skew(clock_skew_ms)
            report = AccountReadReport.model_validate(source.model_dump())
            observed = _minor(report.assets.balance)
            if (
                not 8 <= len(report.observations) <= 10_000
                or sum(o.path == "/v1/account/assets" for o in report.observations) != 4
            ):
                raise ValueError
            previous = None
            for observation in report.observations:
                if (
                    not re.fullmatch(r"[a-f0-9]{64}", observation.sha256)
                    or observation.path
                    not in {"/v1/account/assets", "/v1/openPositions", "/v1/activeOrders"}
                    or (observation.path == "/v1/account/assets" and observation.query)
                    or observation.response_at > observation.received_at + skew
                    or (previous is not None and observation.received_at < previous)
                ):
                    raise ValueError
                previous = observation.received_at
        except Exception:
            raise CashBookError("cash_book_balance_report_invalid") from None
        with self._transaction() as conn:
            meta, records, totals = self._verify(conn)
        opening = OpeningCash.model_validate(_load(meta["opening"]))
        last_execution = max(
            (r.execution.timestamp for r in records.values()), default=opening.cutoff
        )
        if any(o.response_at < last_execution for o in report.observations):
            raise CashBookError("cash_book_balance_report_before_postings")
        difference = observed - totals["cash"]
        return {
            "scope": self.scope,
            "head": meta["head"],
            "book_balance": _money(totals["cash"]),
            "observed_balance": _money(observed),
            "difference": _money(difference),
            "balance_match": difference == 0,
            "halted": bool(meta["halted"]),
            "complete": False,
            "live_enabled": False,
            "blockers": tuple(
                dict.fromkeys(
                    (
                        *BLOCKERS,
                        *report.blockers,
                        *(("cash_book_halted",) if meta["halted"] else ()),
                        *(("cash_balance_difference_unexplained",) if difference else ()),
                    )
                )
            ),
        }
