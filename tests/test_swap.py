from datetime import timedelta

import pandas as pd
import pytest

from trading.backtest import run_backtest
from trading.config import Settings
from trading.gmo import Quote
from trading.paper import paper_step
from trading.swap import swap_credit_between, validate_swap_schedule


def swap_schedule(cfg, timestamp, long=100.0, short=-50.0, days=1):
    return pd.DataFrame(
        {
            "timestamp": [timestamp],
            "symbol": [cfg.symbol],
            "long_jpy_per_10k": [long],
            "short_jpy_per_10k": [short],
            "days": [days],
        }
    )


def test_swap_schedule_validates_and_applies_direction_and_days(cfg):
    timestamp = pd.Timestamp("2025-01-06T04:00Z")
    schedule = validate_swap_schedule(swap_schedule(cfg, timestamp, days=3), cfg)

    assert swap_credit_between(
        schedule,
        pd.Timestamp("2025-01-06T03:00Z"),
        pd.Timestamp("2025-01-06T05:00Z"),
        1000,
    ) == pytest.approx(10)
    assert swap_credit_between(
        schedule,
        pd.Timestamp("2025-01-06T03:00Z"),
        pd.Timestamp("2025-01-06T05:00Z"),
        -2000,
    ) == pytest.approx(-10)


@pytest.mark.parametrize("fault", ["naive", "duplicate", "days", "symbol"])
def test_invalid_swap_schedule_rejected(cfg, fault):
    schedule = swap_schedule(cfg, pd.Timestamp("2025-01-06T04:00Z"))
    if fault == "naive":
        schedule["timestamp"] = pd.Timestamp("2025-01-06T04:00")
    elif fault == "duplicate":
        schedule = pd.concat([schedule, schedule], ignore_index=True)
    elif fault == "days":
        schedule["days"] = 1.5
    else:
        schedule["symbol"] = "EUR_USD"

    with pytest.raises(ValueError):
        validate_swap_schedule(schedule, cfg)


def test_backtest_adds_long_swap_to_equity(bars, cfg):
    schedule = swap_schedule(cfg, bars.timestamp.iloc[4], long=100, days=2)
    without_swap, _, _ = run_backtest(bars, cfg)

    with_swap, equity, _ = run_backtest(bars, cfg, swap_schedule=schedule)

    assert with_swap["swap_pnl_jpy"] == pytest.approx(10)
    assert with_swap["final_equity_jpy"] == pytest.approx(without_swap["final_equity_jpy"] + 10)
    assert equity.swap_pnl.max() == pytest.approx(10)


def test_paper_swap_is_applied_once_and_bound_to_database(bars, cfg, tmp_path):
    entry_time = bars.timestamp.iloc[2] + pd.Timedelta(hours=1, seconds=1)
    event_time = entry_time + pd.Timedelta(seconds=2)
    schedule = validate_swap_schedule(swap_schedule(cfg, event_time, long=100, days=2), cfg)
    entry_quote = Quote(
        symbol=cfg.symbol,
        bid=152,
        ask=152.02,
        status="OPEN",
        timestamp=entry_time,
    )
    path = tmp_path / "swap-paper.sqlite"
    opened = paper_step(bars, entry_quote, cfg, path, entry_time, schedule)
    assert opened["action"] == "buy"

    next_quote = entry_quote.model_copy(update={"timestamp": entry_time + timedelta(seconds=5)})
    credited = paper_step(bars, next_quote, cfg, path, next_quote.timestamp, schedule)
    assert credited["swap_credit_jpy"] == pytest.approx(10)
    assert credited["state"]["swap_pnl"] == pytest.approx(10)

    final_quote = next_quote.model_copy(
        update={"timestamp": next_quote.timestamp + timedelta(seconds=1)}
    )
    repeated = paper_step(bars, final_quote, cfg, path, final_quote.timestamp, schedule)
    assert repeated["swap_credit_jpy"] == 0
    assert repeated["state"]["swap_pnl"] == pytest.approx(10)

    changed = schedule.copy()
    changed["long_jpy_per_10k"] = 101
    with pytest.raises(ValueError, match="already accepted"):
        paper_step(bars, final_quote, cfg, path, final_quote.timestamp, changed)
    with pytest.raises(ValueError, match="different config"):
        paper_step(bars, final_quote, cfg, path, final_quote.timestamp)


def test_paper_swap_history_accepts_newly_published_events(bars, cfg, tmp_path):
    entry_time = bars.timestamp.iloc[2] + pd.Timedelta(hours=1, seconds=1)
    first_event = entry_time + pd.Timedelta(seconds=2)
    schedule = validate_swap_schedule(swap_schedule(cfg, first_event, long=100), cfg)
    quote = Quote(symbol=cfg.symbol, bid=152, ask=152.02, status="OPEN", timestamp=entry_time)
    path = tmp_path / "swap-append.sqlite"
    assert paper_step(bars, quote, cfg, path, entry_time, schedule)["action"] == "buy"

    # The next official event is published after the account was created.
    second_event = first_event + pd.Timedelta(seconds=2)
    extended = pd.concat([schedule, swap_schedule(cfg, second_event, long=100)], ignore_index=True)
    later = quote.model_copy(update={"timestamp": entry_time + timedelta(seconds=5)})
    result = paper_step(bars, later, cfg, path, later.timestamp, extended)

    assert result["swap_credit_jpy"] == pytest.approx(20)
    assert result["state"]["swap_history"]["rows"] == 2


def test_swap_schedule_rejects_non_fx_config():
    equity_cfg = Settings(
        market="jp_equity", symbol="7203", bar_seconds=86400, fast=2, slow=3, lot_size=100
    )
    schedule = swap_schedule(equity_cfg, pd.Timestamp("2025-01-06T04:00Z"))

    with pytest.raises(ValueError, match="FX"):
        validate_swap_schedule(schedule, equity_cfg)


def test_paper_swap_accrues_to_the_observation_time_not_the_older_quote(bars, cfg, tmp_path):
    entry_time = bars.timestamp.iloc[2] + pd.Timedelta(hours=1, seconds=1)
    event_time = entry_time + pd.Timedelta(seconds=2)
    schedule = validate_swap_schedule(swap_schedule(cfg, event_time, long=100, days=2), cfg)
    quote = Quote(symbol=cfg.symbol, bid=152, ask=152.02, status="OPEN", timestamp=entry_time)
    path = tmp_path / "swap-clock.sqlite"
    assert paper_step(bars, quote, cfg, path, entry_time, schedule)["action"] == "buy"

    # The quote predates the swap event, but the position was still held when observed.
    older_quote = quote.model_copy(update={"timestamp": entry_time + timedelta(seconds=1)})
    observed = paper_step(bars, older_quote, cfg, path, entry_time + timedelta(seconds=5), schedule)

    assert observed["swap_credit_jpy"] == pytest.approx(10)
