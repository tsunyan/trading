"""Strategy proposals for the live journal from synthetic bars; never prepared or sent."""

import json
import socket
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pandas as pd
import pytest

from trading import live_signal
from trading.account_guard import AccountQuote, Position
from trading.broker_contracts import OrderLimits, order_request
from trading.config import Settings
from trading.live_signal import LiveSignalError, decide, write_intent

NOW = datetime(2026, 10, 5, 10, 0, 5, tzinfo=UTC)
LIMITS = OrderLimits(
    min_units=1000,
    max_units=10000,
    unit_step=1000,
    price_tick="0.001",
    max_reference_notional="2000000",
)


@pytest.fixture(autouse=True)
def no_network(monkeypatch):
    monkeypatch.setattr(socket, "socket", lambda *a, **k: pytest.fail("real network attempted"))


def settings(**changes):
    return Settings(market="fx", symbol="USD_JPY", bar_seconds=3600, fast=2, slow=4, **changes)


def bars(closes, end=NOW):
    start = pd.Timestamp(end).floor("h") - pd.Timedelta(hours=len(closes))
    rows = []
    for i, close in enumerate(closes):
        rows.append(
            {
                "timestamp": start + pd.Timedelta(hours=i),
                "symbol": "USD_JPY",
                "open": close,
                "high": close + 0.05,
                "low": close - 0.05,
                "close": close,
                "volume": 0,
            }
        )
    return pd.DataFrame(rows)


RISING, FALLING = [149.0, 149.2, 149.4, 149.6, 150.0], [151.0, 150.8, 150.6, 150.4, 150.0]


def quote(bid="150.000", ask="150.010", at=NOW - timedelta(seconds=2), market_open=True):
    return AccountQuote(bid=bid, ask=ask, observed_at=at, market_open=market_open)


def run(closes, *, positions=(), pending=False, cfg=None, units=1000, current=None, **options):
    return decide(
        bars(closes),
        current or quote(),
        cfg or settings(),
        positions=positions,
        pending=pending,
        units=units,
        max_slippage=options.pop("max_slippage", "0.0205"),
        limits=LIMITS,
        now=options.pop("now", NOW),
    )


def long(units=1000, pid=401):
    return Position(position_id=pid, side="BUY", units=units, average_price="149.5")


def test_rising_signal_proposes_a_bounded_market_buy_that_the_request_builder_accepts():
    decision = run(RISING)
    intent = decision["intent"]
    assert decision["action"] == "open" and decision["target"] == 1 and decision["current"] == 0
    assert (intent.side, intent.effect, intent.kind, intent.units) == (
        "BUY",
        "OPEN",
        "MARKET",
        1000,
    )
    assert intent.bound == Decimal("150.030")  # ask + slippage, floored to the tick.
    assert intent.client_id == "S2026100510OB"
    plan = order_request(intent, LIMITS)
    assert json.loads(plan.body)["upperBound"] == "150.030"


def test_falling_signal_closes_every_long_lot_with_settlements():
    decision = run(FALLING, positions=(long(), long(2000, 402)))
    intent = decision["intent"]
    assert decision["action"] == "close" and decision["target"] == 0
    assert (intent.side, intent.effect, intent.units) == ("SELL", "CLOSE", 3000)
    assert [(p.position_id, p.units) for p in intent.positions] == [(401, 1000), (402, 2000)]
    assert intent.bound == Decimal("149.980")  # bid - slippage, ceiled to the tick.
    assert intent.client_id == "S2026100510CS"


def test_short_signal_needs_allow_short_and_closes_shorts_first():
    assert run(FALLING)["action"] == "hold"
    shorting = settings(allow_short=True)
    assert run(FALLING, cfg=shorting)["intent"].side == "SELL"
    short = Position(position_id=501, side="SELL", units=1000, average_price="151")
    decision = run(RISING, cfg=shorting, positions=(short,))
    assert decision["action"] == "close" and decision["intent"].side == "BUY"


@pytest.mark.parametrize(
    ("options", "reason"),
    [
        ({"pending": True}, "unsettled_local_order"),
        ({"positions": (long(),)}, "at_target"),
        ({"current": quote(bid="150.000", ask="150.100")}, "spread_exceeds_entry_limit"),
    ],
)
def test_holds_without_an_intent(options, reason):
    decision = run(RISING, **options)
    assert decision["action"] == "hold" and decision["reason"] == reason
    assert decision["intent"] is None


@pytest.mark.parametrize(
    ("options", "reason"),
    [
        ({"units": 1500}, "units_outside_order_limits"),
        ({"units": 20000}, "units_outside_order_limits"),
        ({"max_slippage": "0"}, "positive_slippage_bound_required"),
        ({"current": quote(at=NOW - timedelta(seconds=120))}, "stale_or_future_quote"),
        ({"now": NOW + timedelta(hours=3)}, "stale_or_future_quote"),
    ],
)
def test_invalid_size_slippage_or_stale_inputs_are_refused(options, reason):
    with pytest.raises(LiveSignalError, match=reason):
        run(RISING, **options)


def test_stale_bars_and_hedged_positions_are_refused():
    old = bars(RISING, end=NOW - timedelta(hours=3))
    with pytest.raises(LiveSignalError, match="stale_signal_data"):
        decide(
            old,
            quote(),
            settings(),
            positions=(),
            pending=False,
            units=1000,
            max_slippage="0.02",
            limits=LIMITS,
            now=NOW,
        )
    with pytest.raises(LiveSignalError, match="not_enough_completed_bars"):
        run(RISING[-3:])
    short = Position(position_id=501, side="SELL", units=1000, average_price="151")
    with pytest.raises(LiveSignalError, match="both_sides_held"):
        run(RISING, positions=(long(), short))


def test_incomplete_current_bar_is_not_used():
    # The bar opening at 10:00 closes at 11:00; it must not drive the 10:00 signal.
    frame = bars(RISING + [100.0], end=NOW + timedelta(hours=1))
    decision = decide(
        frame,
        quote(),
        settings(),
        positions=(),
        pending=False,
        units=1000,
        max_slippage="0.02",
        limits=LIMITS,
        now=NOW,
    )
    assert decision["target"] == 1 and decision["signal_time"].startswith("2026-10-05T10:00")


def test_intent_file_is_what_live_setup_prepare_reads(tmp_path):
    from trading.broker_contracts import OrderIntent

    intent = run(RISING)["intent"]
    path = tmp_path / "intent.json"
    write_intent(intent, path)
    assert OrderIntent.model_validate_json(path.read_bytes()) == intent
    assert [p.name for p in tmp_path.iterdir()] == ["intent.json"]


def test_journal_state_requires_a_fresh_proof_and_reports_unsettled_orders(tmp_path):
    from test_account_guard import account
    from test_account_guard import quote as guard_quote
    from test_private_order import setup as order_setup

    clock, _, _, journal = order_setup.__wrapped__(tmp_path)
    with pytest.raises(LiveSignalError, match="account_proof_required"):
        live_signal.journal_state(journal, clock.now)
    journal.update_account(account(clock.now), guard_quote(clock.now), now=clock.now)
    positions, pending, limits = live_signal.journal_state(journal, clock.now)
    assert positions == () and pending is False and limits == journal.limits
    from test_account_guard import intent

    journal.prepare(intent())
    assert live_signal.journal_state(journal, clock.now)[1] is True
    with pytest.raises(LiveSignalError, match="stale_account_proof"):
        live_signal.journal_state(journal, clock.now + timedelta(seconds=61))


def flatten(positions=(), *, pending=False, current=None):
    return decide(
        None,
        current or quote(),
        settings(),
        positions=positions,
        pending=pending,
        units=1000,
        max_slippage="0.0205",
        limits=LIMITS,
        now=NOW,
        flatten=True,
    )


def test_flatten_closes_every_lot_without_bars_or_strategy():
    decision = flatten((long(), long(2000, 402)))
    intent = decision["intent"]
    assert decision["action"] == "close" and decision["reason"] == "flatten_requested"
    assert decision["flatten"] and decision["signal_time"] is None and decision["target"] == 0
    assert (intent.side, intent.effect, intent.units) == ("SELL", "CLOSE", 3000)
    assert intent.client_id == "F202610051000CS"
    short = Position(position_id=501, side="SELL", units=1000, average_price="151")
    assert flatten((short,))["intent"].side == "BUY"


def test_flatten_holds_when_flat_or_unsettled_and_still_checks_the_quote():
    assert flatten()["reason"] == "at_target"
    assert flatten((long(),), pending=True)["reason"] == "unsettled_local_order"
    with pytest.raises(LiveSignalError, match="stale_or_future_quote"):
        flatten((long(),), current=quote(at=NOW - timedelta(seconds=120)))
    with pytest.raises(LiveSignalError, match="invalid_flatten_option"):
        decide(
            None,
            quote(),
            settings(),
            positions=(),
            pending=False,
            units=1000,
            max_slippage="0.02",
            limits=LIMITS,
            now=NOW,
            flatten=1,
        )


def test_loss_halt_holds_new_entries_but_still_proposes_closes():
    halted = decide(
        bars(RISING),
        quote(),
        settings(),
        positions=(),
        pending=False,
        units=1000,
        max_slippage="0.02",
        limits=LIMITS,
        now=NOW,
        entry_halted=True,
    )
    assert halted["action"] == "hold" and halted["reason"] == "entry_loss_halt"
    closing = decide(
        bars(FALLING),
        quote(),
        settings(),
        positions=(long(),),
        pending=False,
        units=1000,
        max_slippage="0.02",
        limits=LIMITS,
        now=NOW,
        entry_halted=True,
    )
    assert closing["action"] == "close"


def test_closed_market_holds_signals_and_flatten():
    closed = quote(market_open=False)
    assert run(RISING, current=closed)["reason"] == "market_closed"
    assert flatten((long(),), current=closed)["reason"] == "market_closed"


def test_explicit_units_are_parsed_strictly():
    for good, expected in (("1000", 1000), (2000, 2000)):
        assert live_signal.resolve_units(good, settings(), None, quote(), LIMITS) == expected
    for bad in ("0", "-1000", "1e3", "x", "1000.0", None):
        with pytest.raises(LiveSignalError, match="invalid_units"):
            live_signal.resolve_units(bad, settings(), None, quote(), LIMITS)


def test_auto_units_follow_the_paper_rule_and_journal_limits(tmp_path):
    from test_account_guard import account
    from test_account_guard import quote as guard_quote
    from test_private_order import setup as order_setup

    clock, _, _, journal = order_setup.__wrapped__(tmp_path)
    with pytest.raises(LiveSignalError, match="account_proof_required"):
        live_signal.resolve_units("auto", settings(), journal, quote(), journal.limits)
    journal.update_account(account(clock.now), guard_quote(clock.now), now=clock.now)
    # 1,000,000 x 20% / 150.01 = 1333 units, stepped to 100s and capped at the 1000 maximum.
    assert live_signal.resolve_units("auto", settings(), journal, quote(), journal.limits) == 1000
    small = settings(allocation=0.0001)  # About 0.67 units: below every allowed lot.
    assert live_signal.resolve_units("auto", small, journal, quote(), journal.limits) == 0


def test_zero_units_hold_instead_of_failing():
    decision = run(RISING, units=0)
    assert decision["action"] == "hold" and decision["reason"] == "size_below_minimum"


def test_decision_invariants_over_random_holdings_quotes_and_bars():
    import random

    rng = random.Random(20261004)
    for _ in range(300):
        side = rng.choice(["BUY", "SELL", None])
        lots = (
            ()
            if side is None
            else tuple(
                Position(
                    position_id=400 + i,
                    side=side,
                    units=rng.choice([1000, 2000, 3000]),
                    average_price="150",
                )
                for i in range(rng.randint(1, 4))
            )
        )
        bid = Decimal("149") + Decimal(rng.randint(0, 2000)) / 1000
        spread = Decimal(rng.randint(1, 80)) / 1000
        current = quote(bid=str(bid), ask=str(bid + spread))
        closes = [150 + rng.uniform(-1, 1) for _ in range(6)]
        decision = decide(
            bars(closes),
            current,
            settings(allow_short=rng.random() < 0.5),
            positions=lots,
            pending=rng.random() < 0.1,
            units=1000,
            max_slippage="0.02",
            limits=LIMITS,
            now=NOW,
            flatten=rng.random() < 0.2,
            entry_halted=rng.random() < 0.2,
        )
        intent = decision["intent"]
        if decision["action"] == "hold":
            assert intent is None
            continue
        assert intent.kind == "MARKET" and intent.bound % LIMITS.price_tick == 0
        if intent.side == "BUY":
            assert current.ask <= intent.bound <= current.ask + Decimal("0.02")
        else:
            assert current.bid - Decimal("0.02") <= intent.bound <= current.bid
        if lots:
            # Holding: only a close of every lot on the held side, never a new entry.
            assert decision["action"] == "close" and intent.effect == "CLOSE"
            assert intent.units == sum(p.units for p in lots)
            assert intent.side != side
        else:
            assert decision["action"] == "open" and intent.effect == "OPEN"
            assert intent.units == 1000 and current.ask - current.bid <= Decimal("0.05")


def test_closed_market_holds_even_when_the_last_bar_is_days_old():
    weekend = bars(RISING, end=NOW - timedelta(days=2))
    decision = decide(
        weekend,
        quote(market_open=False),
        settings(),
        positions=(long(),),
        pending=False,
        units=1000,
        max_slippage="0.02",
        limits=LIMITS,
        now=NOW,
    )
    assert decision["action"] == "hold" and decision["reason"] == "market_closed"
    assert decision["current"] == 1 and decision["intent"] is None


def test_more_than_ten_lots_cannot_be_closed_in_one_order():
    lots = tuple(long(pid=400 + i) for i in range(11))
    with pytest.raises(LiveSignalError, match="too_many_positions_to_close"):
        run(FALLING, positions=lots)


def test_history_days_cover_the_warmup_and_closures():
    assert live_signal.history_days(settings()) == 9  # 4 bars: one day, weekends, a week.
    base = dict(market="fx", symbol="USD_JPY", bar_seconds=3600)
    assert live_signal.history_days(Settings(**base, fast=24, slow=120)) == 14
    # 1000 hourly bars are 42 trading days, 59 calendar days with weekends, plus slack.
    assert live_signal.history_days(Settings(**base, fast=12, slow=1000)) == 66
    with pytest.raises(LiveSignalError, match="strategy_warmup_exceeds_live_history"):
        live_signal.history_days(Settings(**base, fast=12, slow=2000))
