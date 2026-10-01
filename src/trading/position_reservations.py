"""Read-only reservation diagnostics from declared intents and booked fills."""

import re

from trading.broker_contracts import validate_evidence


def validate_reports(account, orders, skew):
    """Schema / money bounds are checked by the cash book before this function."""
    observations = account.observations
    if (
        len(account.positions) > 1000
        or len(account.active_orders) > 1000
        or len({p.position_id for p in account.positions}) != len(account.positions)
        or len({o.order_id for o in account.active_orders}) != len(account.active_orders)
        or len({o.client_id for o in account.active_orders}) != len(account.active_orders)
        or {o.path for o in observations}
        != {"/v1/account/assets", "/v1/openPositions", "/v1/activeOrders"}
        or sum(o.path == "/v1/openPositions" for o in observations) < 2
        or sum(o.path == "/v1/activeOrders" for o in observations) < 2
        or any(
            b.response_at < a.response_at
            for a, b in zip(observations, observations[1:], strict=False)
        )
        or any(
            p.ordered_units > p.units
            or p.units > 10**12
            or not 1 <= p.position_id < 2**63
            or p.timestamp > observations[-1].response_at
            for p in account.positions
        )
        or any(
            o.units > 10**12
            or o.timestamp > observations[-1].response_at
            or not 1 <= o.order_id < 2**63
            or not 1 <= o.root_order_id < 2**63
            for o in account.active_orders
        )
    ):
        raise ValueError
    ids, clients, fills = set(), set(), set()
    account_start = min(o.response_at for o in observations)
    account_receipt = min(o.received_at for o in observations)
    for report in orders:
        evidence = report.evidence
        validate_evidence(evidence)
        if (
            evidence.executions_complete
            or evidence.order_id in ids
            or evidence.intent.client_id in clients
            or evidence.intent.units > 10**12
            or not 1 <= evidence.order_id < 2**63
            or not 1 <= evidence.root_order_id < 2**63
            or any(not 1 <= p.position_id < 2**63 for p in evidence.intent.positions)
            or len(report.observations) != 4
            or evidence.observed_at != report.observations[-1].response_at
            or (
                evidence.status == "EXECUTED"
                and sum(f.units for f in evidence.executions) != evidence.intent.units
            )
        ):
            raise ValueError
        previous = None
        for index, observation in enumerate(report.observations):
            if (
                observation.path != ("/v1/orders" if index % 2 == 0 else "/v1/executions")
                or observation.query != (("orderId", str(evidence.order_id)),)
                or not re.fullmatch(r"[a-f0-9]{64}", observation.sha256)
                or observation.response_at > observation.received_at + skew
                or observation.response_at > account_start
                or observation.received_at > account_receipt
                or (
                    previous is not None
                    and (
                        observation.response_at < previous.response_at
                        or observation.received_at < previous.received_at
                    )
                )
            ):
                raise ValueError
            previous = observation
        for fill in evidence.executions:
            if (
                fill.execution_id in fills
                or not 1 <= fill.execution_id < 2**63
                or not 1 <= fill.position_id < 2**63
                or fill.units > 10**12
                or fill.fee < 0
            ):
                raise ValueError
            fills.add(fill.execution_id)
        ids.add(evidence.order_id)
        clients.add(evidence.intent.client_id)


def _intent(intent):
    return intent.model_copy(
        update={"positions": tuple(sorted(intent.positions, key=lambda p: p.position_id))}
    )


def compare_reservations(account, orders, records, state):
    """Never use active-order total size as its remaining quantity."""
    active = {o.order_id: o for o in account.active_orders}
    reports = {r.evidence.order_id: r for r in orders}
    by_order, clients = {}, {}
    for execution_id, record in records.items():
        by_order.setdefault(record.order_id, {})[execution_id] = record
        clients[record.intent.client_id] = record.order_id
    problems, unverified, remaining, contributions = [], set(), {}, []
    for identity in sorted(active.keys() - reports.keys()):
        problems.append(f"reservation_order_not_collected:{identity}")
        unverified.add(identity)
    for identity, report in sorted(reports.items()):
        evidence, current = report.evidence, active.get(identity)
        intent = _intent(evidence.intent)
        prior = by_order.get(identity, {})
        observed = {f.execution_id: f for f in evidence.executions}
        start = len(problems)
        if intent.client_id in clients and clients[intent.client_id] != identity:
            problems.append(f"reservation_client_identity_conflict:{identity}")
        if current is not None:
            if (
                current.root_order_id,
                current.client_id,
                current.symbol,
                current.side,
                current.effect,
                current.kind,
                current.units,
                current.price,
                current.status,
            ) != (
                evidence.root_order_id,
                intent.client_id,
                intent.symbol,
                intent.side,
                intent.effect,
                intent.kind,
                intent.units,
                intent.price,
                evidence.status,
            ):
                problems.append(f"reservation_order_mismatch:{identity}")
            if evidence.status != "ORDERED":
                problems.append(f"reservation_order_state_unverified:{identity}")
        elif evidence.status not in {"CANCELED", "EXPIRED", "EXECUTED"}:
            problems.append(f"reservation_active_order_missing:{identity}")
        for record in prior.values():
            if record.root_order_id != evidence.root_order_id or record.intent != intent:
                problems.append(f"reservation_order_identity_conflict:{identity}")
                break
        for execution_id in sorted(prior.keys() - observed.keys()):
            problems.append(f"reservation_booked_execution_missing:{execution_id}")
        for execution_id, fill in sorted(observed.items()):
            booked = records.get(execution_id)
            if booked is None:
                problems.append(f"reservation_execution_not_booked:{execution_id}")
            elif booked.order_id != identity or booked.execution != fill:
                problems.append(f"reservation_execution_conflict:{execution_id}")
        if len(problems) != start:
            unverified.add(identity)
            continue
        if current is None:
            continue  # Terminal evidence releases the unfilled allocation.
        units = intent.units - sum(f.units for f in observed.values())
        if units <= 0:
            problems.append(f"reservation_active_order_fully_filled:{identity}")
            unverified.add(identity)
            continue
        if intent.effect == "OPEN":
            contributions.append({"order_id": identity, "remaining_units": units, "positions": ()})
            continue
        allocated = {p.position_id: p.units for p in intent.positions}
        for fill in observed.values():
            allocated[fill.position_id] -= fill.units
        for pid, quantity in sorted(allocated.items()):
            if not quantity:
                continue
            held = state.positions.get(pid)
            if held is None or held.side == intent.side:
                problems.append(f"reservation_without_matching_inventory:{identity}:{pid}")
                unverified.add(identity)
            remaining[pid] = remaining.get(pid, 0) + quantity
        contributions.append(
            {
                "order_id": identity,
                "remaining_units": units,
                "positions": tuple(
                    {"position_id": pid, "units": q} for pid, q in sorted(allocated.items()) if q
                ),
            }
        )
    observed_positions = {p.position_id: p for p in account.positions}
    rows = []
    for pid in sorted(state.positions.keys() | observed_positions.keys() | remaining.keys()):
        held, observed = state.positions.get(pid), observed_positions.get(pid)
        expected = remaining.get(pid, 0) if not unverified else None
        if held is None:
            problems.append(f"reservation_position_unexpected:{pid}")
        elif observed is None:
            problems.append(f"reservation_position_missing:{pid}")
        elif held.side != observed.side or held.units != observed.units:
            problems.append(f"reservation_position_inventory_mismatch:{pid}")
        if expected is not None:
            if held is not None and expected > held.units:
                problems.append(f"reservation_exceeds_inventory:{pid}")
            if observed is not None and expected != observed.ordered_units:
                problems.append(f"reservation_units_mismatch:{pid}")
        rows.append(
            {
                "position_id": pid,
                "book_units": held.units if held else None,
                "expected_ordered_units": expected,
                "observed_ordered_units": observed.ordered_units if observed else None,
                "unreserved_units": held.units - expected
                if held and expected is not None and expected <= held.units
                else None,
            }
        )
    return {
        "reservation_match": not problems,
        "mismatches": tuple(dict.fromkeys(problems)),
        "unverified_order_ids": tuple(sorted(unverified)),
        "orders": tuple(contributions),
        "reservations": tuple(rows),
    }
