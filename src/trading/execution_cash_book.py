"""Local, balanced cash postings for individually matched WS/REST executions.

One database is one explicitly declared scope and opening cash boundary. This
is not a broker account snapshot or proof of complete history. Version 2 also
checks position inventory and realized P&L from an explicit starting basis.
Version 3 adds matched external cash records from two declared evidence sources.
"""

import hashlib
import json
import re
import sqlite3
import uuid
from contextlib import closing, contextmanager
from datetime import UTC, datetime
from decimal import Decimal, localcontext
from fractions import Fraction
from pathlib import Path
from typing import Literal

from pydantic import AwareDatetime, Field

from trading.account_events import AccountEvent
from trading.account_reader import AccountReadReport, OrderReadReport
from trading.broker_contracts import Contract, Execution, OrderIntent, Units
from trading.cash_transfers import (
    CashTransferMatch,
    CashTransferPolicy,
    check_transfer,
    transfer_identity,
)
from trading.execution_positions import (
    PositionAccountingError,
    PositionBasis,
    position_result,
    rebuild_positions,
)
from trading.execution_reconciliation import reconcile_executions
from trading.position_reservations import compare_reservations, validate_reports
from trading.storage_init import new_storage_directory
from trading.wire_validation import clock_skew, unique_object

SCALE = 100_000_000
MAX_MONEY = Decimal("1000000000000000000")
MAX_ENTRIES = 5000
MAX_PROOF = 2_000_000
MAX_BYTES = 32_000_000
MAX_OPENING = 1_000_000
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
TRANSFER_SCHEMA = """
CREATE TABLE transfer_state (
 id INTEGER PRIMARY KEY CHECK(id=1), count INTEGER NOT NULL,
 bytes INTEGER NOT NULL, head TEXT NOT NULL
);
CREATE TABLE cash_transfers (
 sequence INTEGER PRIMARY KEY, transfer_id TEXT UNIQUE NOT NULL,
 body TEXT NOT NULL, digest TEXT NOT NULL
);
CREATE TABLE transfer_postings (
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
    position_basis: PositionBasis | None = None
    transfer_policy: CashTransferPolicy | None = None


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
    if isinstance(value, OpeningCash):
        data = value.model_dump(warnings=False)
        if value.position_basis is None:
            # Preserve v1's exact canonical body / seed, without migration.
            data.pop("position_basis")
        if value.transfer_policy is None:
            data.pop("transfer_policy")
        return _normalize(data)
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


def _blockers(version, has_positions=False):
    result = BLOCKERS
    if version == 2 or has_positions:
        result = tuple(b for b in result if b != "position_accounting_not_applied") + (
            "opening_position_boundary_not_verified",
            "position_cost_rounding_not_verified",
        )
    if version == 3:
        result += ("external_cash_evidence_not_authenticated", "external_cash_history_not_proven")
    return result


def _public_head(execution_head, info):
    if info is None:
        return execution_head
    return _hash(_json({"executions": execution_head, "transfers": info["head"], "version": 3}))


def _transfer_result(info, records):
    if info is None:
        return {}
    return {
        "external_cash_accounting_applied": True,
        "external_transfer_ids": tuple(sorted(records)),
        "external_transfers": info["count"],
        "external_cash_amount": _money(info["cash"]),
        "external_capital_amount": _money(info["capital"]),
        "transfer_fee_debit": _money(info["fee"]),
    }


def _transfer_legs(match):
    record = match.primary.record
    capital = _minor(record.amount) * (1 if record.kind == "DEPOSIT" else -1)
    fee = _minor(record.fee_debit)
    return {"cash": capital - fee, "external_capital": -capital, "fee_expense": fee}


def _transfer_matches(source):
    try:
        if not isinstance(source, tuple) or not 1 <= len(source) <= 1000:
            raise ValueError
        result, size = [], 0
        for item in source:
            if not isinstance(item, CashTransferMatch):
                raise ValueError
            match = CashTransferMatch.model_validate(item.model_dump(warnings=False))
            body = _json(match)
            size += len(body.encode())
            if size > MAX_PROOF:
                raise ValueError
            result.append((match, body))
        return tuple(result)
    except Exception:
        raise CashBookError("cash_book_transfer_input_invalid") from None


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
            if len(opening_body.encode()) > MAX_OPENING:
                raise ValueError
            cash = _minor(opening.balance)
            version = (
                3
                if opening.transfer_policy is not None
                else 1
                if opening.position_basis is None
                else 2
            )
            if opening.position_basis is not None:
                rebuild_positions(opening.position_basis, ())
        except Exception:
            raise CashBookError("invalid_cash_book_opening") from None
        directory = Path(directory).resolve()
        with new_storage_directory(
            directory, ("execution-cash.sqlite-journal", "execution-cash.sqlite")
        ):
            try:
                instance = uuid.uuid4().hex
                head = cls._seed(instance, scope, opening_body, max_entries, version)
                with closing(sqlite3.connect(directory / "execution-cash.sqlite")) as conn:
                    conn.execute("PRAGMA synchronous=FULL")
                    conn.executescript(SCHEMA)
                    if version == 3:
                        conn.executescript(TRANSFER_SCHEMA)
                        transfer_seed = _hash(
                            _json({"opening_seed": head, "kind": "external-cash-v1"})
                        )
                        conn.execute("INSERT INTO transfer_state VALUES(1,0,0,?)", (transfer_seed,))
                    conn.execute(
                        "INSERT INTO book VALUES(1,?,?,?,?,?,0,0,0,?,0,NULL)",
                        (version, instance, scope, opening_body, max_entries, head),
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
    def _seed(instance, scope, opening, capacity, version=1):
        return _hash(
            _json(
                {
                    "instance": instance,
                    "scope": scope,
                    "opening": opening,
                    "capacity": capacity,
                    "version": version,
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
            if any(
                r[0] is None or r[0] > MAX_OPENING
                for r in conn.execute("SELECT length(CAST(opening AS BLOB)) FROM book LIMIT 2")
            ):
                raise ValueError
            metas = conn.execute("SELECT * FROM book LIMIT 2").fetchall()
            if len(metas) != 1:
                raise ValueError
            meta = dict(metas[0])
            if (
                meta["id"] != 1
                or meta["version"] not in (1, 2, 3)
                or meta["scope"] != self.scope
                or not re.fullmatch(r"[a-f0-9]{32}", meta["instance"])
                or (self._instance is not None and meta["instance"] != self._instance)
                or not 1 <= meta["max_entries"] <= MAX_ENTRIES
                or not 0 <= meta["count"] <= meta["max_entries"]
                or not 0 <= meta["proof_count"] <= meta["count"]
                or not 0 <= meta["bytes"] <= MAX_BYTES
                or meta["halted"] not in (0, 1)
                or (not meta["halted"] and meta["reason"] is not None)
                or (
                    meta["halted"]
                    and meta["reason"]
                    not in (
                        "cash_book_identity_conflict",
                        *(
                            ("cash_book_transfer_identity_conflict",)
                            if meta["version"] == 3
                            else ()
                        ),
                    )
                )
            ):
                raise ValueError
            opening = OpeningCash.model_validate(_load(meta["opening"]))
            if meta["opening"] != _json(opening):
                raise ValueError
            expected_version = (
                3
                if opening.transfer_policy is not None
                else 1
                if opening.position_basis is None
                else 2
            )
            if expected_version != meta["version"]:
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
                meta["instance"], self.scope, meta["opening"], meta["max_entries"], meta["version"]
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
            for name, value in totals.items():
                if name != "cash":
                    _money(value)
            totals["position_state"] = (
                rebuild_positions(opening.position_basis, tuple(records.values()))
                if opening.position_basis is not None
                else None
            )
            info, transfers = None, {}
            if meta["version"] == 3:
                info, transfers = self._verify_transfers(conn, meta, opening)
                totals["cash"] += info["cash"]
            _money(totals["cash"])
            totals["transfer_info"], totals["transfers"] = info, transfers
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
                info = totals["transfer_info"]
                transfer_count, transfer_bytes = (info["count"], info["bytes"]) if info else (0, 0)
                if meta["count"] + transfer_count + len(new) > meta["max_entries"]:
                    raise CashBookError("cash_book_capacity_reached")
                size = meta["bytes"] + (len(proof.encode()) if new else 0)
                size += sum(len(_json(record).encode()) for record in new.values())
                if size + transfer_bytes > MAX_BYTES:
                    raise CashBookError("cash_book_capacity_reached")
                position_state = totals["position_state"]
                if opening.position_basis is not None:
                    try:
                        position_state = rebuild_positions(
                            opening.position_basis, tuple({**existing, **new}.values())
                        )
                    except PositionAccountingError as error:
                        raise CashBookError(str(error)) from None
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
                    "head": _public_head(head, info),
                    "applied_execution_ids": tuple(new),
                    "already_applied_execution_ids": tuple(
                        sorted(selected.keys() & existing.keys())
                    ),
                    "cash_delta": _money(cash_delta),
                    "balance": _money(totals["cash"] + cash_delta),
                    "accounting_applied": True,
                    "complete": False,
                    "live_enabled": False,
                    "blockers": _blockers(meta["version"], opening.position_basis is not None),
                    **position_result(position_state),
                    **_transfer_result(info, totals["transfers"]),
                }
        if conflict:
            raise CashBookError("cash_book_identity_conflict")
        return result

    def _verify_transfers(self, conn, meta, opening):
        if any(r[0] != 64 for r in conn.execute("SELECT length(head) FROM transfer_state LIMIT 2")):
            raise ValueError
        states = conn.execute("SELECT * FROM transfer_state LIMIT 2").fetchall()
        if len(states) != 1:
            raise ValueError
        info = dict(states[0])
        if (
            info["id"] != 1
            or type(info["count"]) is not int
            or type(info["bytes"]) is not int
            or not 0 <= info["count"] <= meta["max_entries"] - meta["count"]
            or not 0 <= info["bytes"] <= MAX_BYTES - meta["bytes"]
        ):
            raise ValueError
        dimensions = conn.execute(
            "SELECT count(*),coalesce(sum(length(CAST(body AS BLOB))),0),"
            "coalesce(max(length(CAST(body AS BLOB))),0),"
            "coalesce(max(length(transfer_id)),0),"
            "coalesce(max(length(digest)),0) FROM cash_transfers"
        ).fetchone()
        if (
            dimensions[0] != info["count"]
            or dimensions[1] != info["bytes"]
            or dimensions[2] > MAX_PROOF
            or dimensions[3] > 128
            or dimensions[4] > 64
        ):
            raise ValueError
        dimensions = conn.execute(
            "SELECT count(*),coalesce(sum(length(amount)),0),coalesce(max(length(amount)),0),"
            "coalesce(max(length(account)),0) FROM transfer_postings"
        ).fetchone()
        if (
            dimensions[0] != 3 * info["count"]
            or dimensions[1] > MAX_BYTES
            or dimensions[2] > 64
            or dimensions[3] > 32
        ):
            raise ValueError
        seed = self._seed(meta["instance"], self.scope, meta["opening"], meta["max_entries"], 3)
        previous = _hash(_json({"opening_seed": seed, "kind": "external-cash-v1"}))
        records, refs, legs = {}, set(), {}
        cash = capital = fee = 0
        latest_at = None
        for sequence, row in enumerate(
            conn.execute(
                "SELECT * FROM cash_transfers ORDER BY sequence LIMIT ?", (MAX_ENTRIES + 1,)
            ),
            1,
        ):
            match, body = _transfer_matches(
                (CashTransferMatch.model_validate(_load(row["body"])),)
            )[0]
            check_transfer(match, opening.transfer_policy, opening.cutoff)
            identity = match.primary.record.transfer_id
            references = {(e.source, e.reference) for e in (match.primary, match.confirmation)}
            if (
                row["sequence"] != sequence
                or row["transfer_id"] != identity
                or identity in records
                or refs.intersection(references)
                or row["body"] != body
            ):
                raise ValueError
            previous = self._transfer_digest(previous, sequence, identity, body)
            if row["digest"] != previous:
                raise ValueError
            posting = _transfer_legs(match)
            cash += posting["cash"]
            capital -= posting["external_capital"]
            fee += posting["fee_expense"]
            legs.update({(sequence, key): str(amount) for key, amount in posting.items()})
            latest_at = (
                max(latest_at, match.primary.record.occurred_at)
                if latest_at
                else match.primary.record.occurred_at
            )
            refs.update(references)
            records[identity] = match
        actual = {
            (r["sequence"], r["account"]): r["amount"]
            for r in conn.execute("SELECT * FROM transfer_postings LIMIT ?", (3 * MAX_ENTRIES + 1,))
        }
        if actual != legs or previous != info["head"]:
            raise ValueError
        for value in (cash, capital, fee):
            _money(value)
        return {
            **info,
            "cash": cash,
            "capital": capital,
            "fee": fee,
            "latest_at": latest_at,
        }, records

    @staticmethod
    def _transfer_digest(previous, sequence, identity, body):
        return _hash(
            _json(
                {"previous": previous, "sequence": sequence, "transfer_id": identity, "body": body}
            )
        )

    def apply_transfers(self, source: tuple[CashTransferMatch, ...]):
        """Book explicitly matched settled statement records; never infer from balance."""
        candidates = _transfer_matches(source)
        conflict = False
        with self._transaction(write=True) as conn:
            meta, _, totals = self._verify(conn)
            if meta["halted"]:
                raise CashBookError("cash_book_halted")
            opening = OpeningCash.model_validate(_load(meta["opening"]))
            if opening.transfer_policy is None:
                raise CashBookError("cash_book_transfer_policy_required")
            try:
                for match, _ in candidates:
                    check_transfer(match, opening.transfer_policy, opening.cutoff)
            except Exception:
                raise CashBookError("cash_book_transfer_evidence_not_matched") from None
            existing, info = totals["transfers"], totals["transfer_info"]
            known = dict(existing)
            refs = {
                (e.source, e.reference): identity
                for identity, match in existing.items()
                for e in (match.primary, match.confirmation)
            }
            selected = {}
            for match, body in candidates:
                identity = match.primary.record.transfer_id
                prior = known.get(identity)
                if prior is not None and transfer_identity(prior) != transfer_identity(match):
                    conflict = True
                for evidence in (match.primary, match.confirmation):
                    key = (evidence.source, evidence.reference)
                    if key in refs and refs[key] != identity:
                        conflict = True
                    refs[key] = identity
                known[identity] = match
                selected.setdefault(identity, (match, body))
            if conflict:
                conn.execute(
                    "UPDATE book SET halted=1,reason='cash_book_transfer_identity_conflict' "
                    "WHERE id=1"
                )
            else:
                new = {
                    identity: selected[identity]
                    for identity in sorted(selected.keys() - existing.keys())
                }
                size = info["bytes"] + sum(len(body.encode()) for _, body in new.values())
                if (
                    meta["count"] + info["count"] + len(new) > meta["max_entries"]
                    or meta["bytes"] + size > MAX_BYTES
                ):
                    raise CashBookError("cash_book_capacity_reached")
                cash_delta = sum(_transfer_legs(match)["cash"] for match, _ in new.values())
                capital_delta = sum(
                    -_transfer_legs(match)["external_capital"] for match, _ in new.values()
                )
                fee_delta = sum(_transfer_legs(match)["fee_expense"] for match, _ in new.values())
                for value in (
                    cash_delta,
                    totals["cash"] + cash_delta,
                    info["cash"] + cash_delta,
                    info["capital"] + capital_delta,
                    info["fee"] + fee_delta,
                ):
                    _money(value)
                sequence, head = info["count"], info["head"]
                for identity, (match, body) in new.items():
                    sequence += 1
                    head = self._transfer_digest(head, sequence, identity, body)
                    conn.execute(
                        "INSERT INTO cash_transfers VALUES(?,?,?,?)",
                        (sequence, identity, body, head),
                    )
                    conn.executemany(
                        "INSERT INTO transfer_postings VALUES(?,?,?)",
                        (
                            (sequence, key, str(value))
                            for key, value in _transfer_legs(match).items()
                        ),
                    )
                if new:
                    conn.execute(
                        "UPDATE transfer_state SET count=?,bytes=?,head=? WHERE id=1",
                        (sequence, size, head),
                    )
                result = {
                    "scope": self.scope,
                    "instance": meta["instance"],
                    "head": _public_head(meta["head"], {"head": head}),
                    "applied_transfer_ids": tuple(new),
                    "already_applied_transfer_ids": tuple(
                        sorted(selected.keys() & existing.keys())
                    ),
                    "cash_delta": _money(cash_delta),
                    "balance": _money(totals["cash"] + cash_delta),
                    "external_cash_accounting_applied": True,
                    "complete": False,
                    "live_enabled": False,
                    "blockers": _blockers(3, totals["position_state"] is not None),
                }
        if conflict:
            raise CashBookError("cash_book_transfer_identity_conflict")
        return result

    def snapshot(self):
        with self._transaction() as conn:
            meta, records, totals = self._verify(conn)
        return {
            "instance": meta["instance"],
            "scope": self.scope,
            "head": _public_head(meta["head"], totals["transfer_info"]),
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
            "blockers": _blockers(meta["version"], totals["position_state"] is not None),
            **position_result(totals["position_state"]),
            **_transfer_result(totals["transfer_info"], totals["transfers"]),
        }

    @staticmethod
    def _account_report(source: AccountReadReport, clock_skew_ms):
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
        return report, observed

    @staticmethod
    def _check_report_boundary(report, records, opening, transfer_info=None):
        last_execution = max(
            (r.execution.timestamp for r in records.values()), default=opening.cutoff
        )
        if transfer_info is not None and transfer_info["latest_at"] is not None:
            last_execution = max(last_execution, transfer_info["latest_at"])
        if any(o.response_at < last_execution for o in report.observations):
            raise CashBookError("cash_book_balance_report_before_postings")

    def compare_balance(self, source: AccountReadReport, *, clock_skew_ms=0):
        """Non-persistent comparison only; never adjust cash to match an account."""
        report, observed = self._account_report(source, clock_skew_ms)
        with self._transaction() as conn:
            meta, records, totals = self._verify(conn)
        opening = OpeningCash.model_validate(_load(meta["opening"]))
        self._check_report_boundary(report, records, opening, totals["transfer_info"])
        difference = observed - totals["cash"]
        return {
            "scope": self.scope,
            "head": _public_head(meta["head"], totals["transfer_info"]),
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
                        *_blockers(meta["version"], totals["position_state"] is not None),
                        *report.blockers,
                        *(("cash_book_halted",) if meta["halted"] else ()),
                        *(("cash_balance_difference_unexplained",) if difference else ()),
                    )
                )
            ),
        }

    def compare_positions(
        self, source: AccountReadReport, *, price_tolerance_jpy=Decimal(0), clock_skew_ms=0
    ):
        """Compare reconstructed inventory; never import or repair broker totals."""
        try:
            report, _ = self._account_report(source, clock_skew_ms)
            _minor(price_tolerance_jpy)
            if not 0 <= price_tolerance_jpy <= Decimal("0.01"):
                raise ValueError
            if (
                len(report.positions) > 1000
                or len({p.position_id for p in report.positions}) != len(report.positions)
                or {o.path for o in report.observations}
                != {"/v1/account/assets", "/v1/openPositions", "/v1/activeOrders"}
                or sum(o.path == "/v1/openPositions" for o in report.observations) < 2
                or sum(o.path == "/v1/activeOrders" for o in report.observations) < 2
                or any(
                    b.response_at < a.response_at
                    for a, b in zip(report.observations, report.observations[1:], strict=False)
                )
            ):
                raise ValueError
            for position in report.positions:
                _minor(position.price)
                if (
                    position.timestamp > report.observations[-1].response_at
                    or position.ordered_units > position.units
                ):
                    raise ValueError
        except Exception:
            raise CashBookError("cash_book_position_report_invalid") from None
        with self._transaction() as conn:
            meta, records, totals = self._verify(conn)
        opening = OpeningCash.model_validate(_load(meta["opening"]))
        self._check_report_boundary(report, records, opening, totals["transfer_info"])
        state = totals["position_state"]
        if state is None:
            raise CashBookError("cash_book_position_basis_required")
        observed = {p.position_id: p for p in report.positions}
        problems = []
        tolerance = Fraction(price_tolerance_jpy)
        for pid in sorted(state.positions.keys() | observed.keys()):
            expected, actual = state.positions.get(pid), observed.get(pid)
            if expected is None:
                problems.append(f"position_unexpected:{pid}")
            elif actual is None:
                problems.append(f"position_missing:{pid}")
            else:
                if expected.side != actual.side:
                    problems.append(f"position_side_mismatch:{pid}")
                if expected.units != actual.units:
                    problems.append(f"position_units_mismatch:{pid}")
                if abs(expected.average_price - Fraction(actual.price)) > tolerance:
                    problems.append(f"position_price_mismatch:{pid}")
        return {
            "scope": self.scope,
            "head": _public_head(meta["head"], totals["transfer_info"]),
            "position_match": not problems,
            "mismatches": tuple(problems),
            "price_tolerance_jpy": _money(_minor(price_tolerance_jpy)),
            **position_result(state),
            "halted": bool(meta["halted"]),
            "complete": False,
            "live_enabled": False,
            "blockers": tuple(
                dict.fromkeys(
                    (
                        *_blockers(meta["version"], totals["position_state"] is not None),
                        *report.blockers,
                        "position_reservations_not_reconciled",
                        *(("cash_book_halted",) if meta["halted"] else ()),
                        *(("position_inventory_difference_unexplained",) if problems else ()),
                    )
                )
            ),
        }

    def compare_reservations(
        self, source: AccountReadReport, orders: tuple[OrderReadReport, ...], *, clock_skew_ms=0
    ):
        """Diagnose close-order reservations without writing or granting permission."""
        try:
            report, _ = self._account_report(source, clock_skew_ms)
            if not isinstance(orders, tuple) or len(orders) > 1000:
                raise ValueError
            if sum(len(r.evidence.executions) for r in orders) > 10_000:
                raise ValueError
            # Revalidate caller-constructed nested models and bound all money.
            orders = tuple(OrderReadReport.model_validate(r.model_dump()) for r in orders)
            if len(_json((report, orders)).encode()) > MAX_PROOF:
                raise ValueError
            validate_reports(report, orders, clock_skew(clock_skew_ms))
        except Exception:
            raise CashBookError("cash_book_reservation_report_invalid") from None
        with self._transaction() as conn:
            meta, records, totals = self._verify(conn)
            state = totals["position_state"]
            if state is None:
                raise CashBookError("cash_book_position_basis_required")
            opening = OpeningCash.model_validate(_load(meta["opening"]))
            self._check_report_boundary(report, records, opening, totals["transfer_info"])
            boundary = max(
                (r.execution.timestamp for r in records.values()), default=opening.cutoff
            )
            if any(o.response_at < boundary for r in orders for o in r.observations):
                raise CashBookError("cash_book_reservation_report_before_postings")
            comparison = compare_reservations(report, orders, records, state)
        return {
            "scope": self.scope,
            "head": _public_head(meta["head"], totals["transfer_info"]),
            **comparison,
            "halted": bool(meta["halted"]),
            "complete": False,
            "live_enabled": False,
            "blockers": tuple(
                dict.fromkeys(
                    (
                        *_blockers(meta["version"], True),
                        *report.blockers,
                        "reservation_intents_not_authenticated",
                        "reservation_execution_history_not_proven",
                        "broker_reservation_semantics_not_verified",
                        *(("cash_book_halted",) if meta["halted"] else ()),
                        *(
                            ("position_reservation_difference_unexplained",)
                            if comparison["mismatches"]
                            else ()
                        ),
                    )
                )
            ),
        }
