import hashlib
import sqlite3
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta

import pandas as pd
import pytest

from trading.gmo import Quote
from trading.paper import paper_status, paper_step


def quote_at(bars, index=2, bid=152, ask=152.02, seconds=1):
    return Quote(
        symbol="USD_JPY",
        bid=bid,
        ask=ask,
        status="OPEN",
        timestamp=(bars.timestamp.iloc[index] + pd.Timedelta(hours=1, seconds=seconds)),
    )


def test_forward_price_not_historical_open_and_restart_dedup(bars, cfg, tmp_path):
    path = tmp_path / "paper.sqlite"
    quote = quote_at(bars, bid=153, ask=153.02)
    result = paper_step(bars, quote, cfg, path, quote.timestamp)
    assert result["action"] == "buy"
    assert result["fill_price"] == pytest.approx(153.03)
    assert result["state"]["cash"] == pytest.approx(1_000_000 - 153030 - 153.03)
    same = paper_step(bars, quote, cfg, path, quote.timestamp)
    assert same["action"] == "duplicate_quote"
    newer = quote.model_copy(update={"timestamp": quote.timestamp + timedelta(seconds=5)})
    assert paper_step(bars, newer, cfg, path, newer.timestamp)["filled_units"] == 0
    assert paper_status(path)["state"]["units"] == 1000


def test_concurrent_calls_do_not_duplicate_fill(bars, cfg, tmp_path):
    path = tmp_path / "paper.sqlite"
    quote = quote_at(bars)
    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = [
            executor.submit(paper_step, bars, quote, cfg, path, quote.timestamp) for _ in range(2)
        ]
        results = [f.result() for f in futures]
    assert sorted(r["action"] for r in results) == ["buy", "duplicate_quote"]
    assert paper_status(path)["events"] == 1


def test_risk_exit_even_on_same_signal_then_persistent_halt(bars, cfg, tmp_path):
    cfg = cfg.model_copy(update={"max_drawdown": 0.001})
    path = tmp_path / "paper.sqlite"
    first = quote_at(bars)
    paper_step(bars, first, cfg, path, first.timestamp)
    second = quote_at(bars, bid=149, ask=149.02, seconds=10)
    result = paper_step(bars, second, cfg, path, second.timestamp)
    assert result["action"] == "sell"
    assert result["state"]["halted"]
    assert result["state"]["units"] == 0
    third = quote_at(bars, index=3, bid=160, ask=160.02)
    assert paper_step(bars, third, cfg, path, third.timestamp)["filled_units"] == 0


@pytest.mark.parametrize("fault", ["stale", "future", "closed", "wide", "old_signal"])
def test_bad_observation_does_not_trade(bars, cfg, tmp_path, fault):
    quote = quote_at(bars)
    now = quote.timestamp
    if fault == "stale":
        now += timedelta(seconds=61)
    elif fault == "future":
        now -= timedelta(seconds=11)
    elif fault == "closed":
        quote = quote.model_copy(update={"status": "CLOSE"})
    elif fault == "wide":
        quote = quote.model_copy(update={"ask": 153.0})
    else:
        quote = quote.model_copy(update={"timestamp": now + timedelta(days=2)})
        now = quote.timestamp
    path = tmp_path / "paper.sqlite"
    with pytest.raises(ValueError):
        paper_step(bars, quote, cfg, path, now)
    if path.exists():
        assert paper_status(path)["events"] == 0


def test_small_future_quote_is_accepted(bars, cfg, tmp_path):
    quote = quote_at(bars, index=3)
    result = paper_step(
        bars, quote, cfg, tmp_path / "paper.sqlite", quote.timestamp - timedelta(seconds=10)
    )
    assert result["action"] == "buy"
    expected = bars.timestamp.iloc[2] + pd.Timedelta(hours=1)
    assert result["signal_time"] == expected.isoformat()


def test_late_local_clock_does_not_pull_in_a_later_bar(bars, cfg, tmp_path):
    """Bar completeness is judged against the quote clock, not the local one.

    `now` is always taken after the quote round-trip, so it trails the quote by
    up to max_quote_age_seconds. Judging completeness by `now` would count a bar
    that closed after the price we are about to fill at.
    """
    quote = quote_at(bars, index=3, seconds=-15)
    now = quote.timestamp + timedelta(seconds=25)
    bar_closing_between = bars.timestamp.iloc[3] + pd.Timedelta(hours=1)
    assert quote.timestamp < bar_closing_between < now

    result = paper_step(bars, quote, cfg, tmp_path / "paper.sqlite", now)

    expected = bars.timestamp.iloc[2] + pd.Timedelta(hours=1)
    assert result["signal_time"] == expected.isoformat()


def test_signal_freshness_uses_actual_observation_time(bars, cfg, tmp_path):
    cfg = cfg.model_copy(update={"max_signal_age_seconds": 10})
    quote = quote_at(bars)
    now = quote.timestamp + timedelta(seconds=20)

    with pytest.raises(ValueError, match="stale signal"):
        paper_step(bars, quote, cfg, tmp_path / "paper.sqlite", now)


def test_config_change_rejected_without_corrupting_state(bars, cfg, tmp_path):
    path = tmp_path / "paper.sqlite"
    quote = quote_at(bars)
    paper_step(bars, quote, cfg, path, quote.timestamp)
    changed = cfg.model_copy(update={"allocation": 0.5})
    with pytest.raises(ValueError, match="different config"):
        paper_step(bars, quote, changed, path, quote.timestamp)
    assert paper_status(path)["events"] == 1


def test_short_or_leverage_requires_a_separate_paper_database(bars, cfg, tmp_path):
    path = tmp_path / "paper.sqlite"
    quote = quote_at(bars)
    paper_step(bars, quote, cfg, path, quote.timestamp)
    changed = cfg.model_copy(update={"allow_short": True, "max_leverage": 2})

    with pytest.raises(ValueError, match="different config"):
        paper_step(bars, quote, changed, path, quote.timestamp)

    assert paper_status(path)["events"] == 1


def test_quote_tolerance_change_preserves_existing_paper_account(bars, cfg, tmp_path):
    path = tmp_path / "paper.sqlite"
    quote = quote_at(bars)
    paper_step(bars, quote, cfg, path, quote.timestamp)

    changed = cfg.model_copy(update={"max_quote_age_seconds": 60})
    next_quote = quote.model_copy(update={"timestamp": quote.timestamp + timedelta(seconds=5)})
    result = paper_step(bars, next_quote, changed, path, next_quote.timestamp)

    assert result["state"]["units"] == 1000
    assert paper_status(path)["config_sha256"] == changed.fingerprint


def test_pre_margin_fingerprint_preserves_unlevered_paper_account(bars, cfg, tmp_path):
    path = tmp_path / "paper.sqlite"
    quote = quote_at(bars)
    paper_step(bars, quote, cfg, path, quote.timestamp)
    legacy_json = cfg.model_dump_json(
        exclude={
            "strategy",
            "lookback",
            "signal_threshold",
            "allow_short",
            "max_leverage",
            "maintenance_margin_ratio",
        }
    )
    legacy_hash = hashlib.sha256(legacy_json.encode()).hexdigest()
    with sqlite3.connect(path) as connection:
        connection.execute("UPDATE account SET config_hash = ? WHERE id = 1", (legacy_hash,))

    next_quote = quote.model_copy(update={"timestamp": quote.timestamp + timedelta(seconds=5)})
    result = paper_step(bars, next_quote, cfg, path, next_quote.timestamp)

    assert result["action"] == "same_signal"
    assert paper_status(path)["config_sha256"] == cfg.fingerprint


def test_short_paper_marks_at_ask_and_buys_to_cover(bars, cfg, tmp_path):
    cfg = cfg.model_copy(update={"allow_short": True, "max_leverage": 2})
    descending = bars.copy()
    descending.loc[:2, "open"] = [154.0, 153.0, 152.0]
    descending.loc[:2, "close"] = [154.0, 153.0, 152.0]
    descending.loc[:2, "high"] = [155.0, 154.0, 153.0]
    descending.loc[:2, "low"] = [153.0, 152.0, 151.0]
    path = tmp_path / "short.sqlite"
    entry_quote = quote_at(descending, bid=152, ask=152.02)

    opened = paper_step(descending, entry_quote, cfg, path, entry_quote.timestamp)

    assert opened["action"] == "sell_short"
    assert opened["filled_units"] == -1000
    assert opened["fill_price"] == pytest.approx(151.99)
    expected_cash = 1_000_000 + 151_990 - 151.99
    assert opened["state"]["cash"] == pytest.approx(expected_cash)
    assert opened["state"]["equity"] == pytest.approx(expected_cash - 152_020)

    reversed_bars = descending.copy()
    reversed_bars.loc[3, ["open", "high", "low", "close"]] = [200, 201, 199, 200]
    exit_quote = quote_at(reversed_bars, index=3, bid=149, ask=149.02)
    closed = paper_step(reversed_bars, exit_quote, cfg, path, exit_quote.timestamp)

    assert closed["action"] == "buy_to_cover"
    assert closed["filled_units"] == 1000
    assert closed["fill_price"] == pytest.approx(149.03)
    assert closed["state"]["units"] == 0
    assert closed["state"]["equity"] > cfg.initial_cash


def test_short_margin_breach_forces_buy_to_cover(bars, cfg, tmp_path):
    cfg = cfg.model_copy(
        update={
            "allow_short": True,
            "allocation": 1,
            "max_units": 100_000,
            "max_leverage": 10,
            "maintenance_margin_ratio": 0.9,
            "max_drawdown": 0.9,
        }
    )
    descending = bars.copy()
    descending.loc[:2, "open"] = [154.0, 153.0, 152.0]
    descending.loc[:2, "close"] = [154.0, 153.0, 152.0]
    descending.loc[:2, "high"] = [155.0, 154.0, 153.0]
    descending.loc[:2, "low"] = [153.0, 152.0, 151.0]
    path = tmp_path / "short-margin.sqlite"
    entry_quote = quote_at(descending, bid=152, ask=152.02)
    opened = paper_step(descending, entry_quote, cfg, path, entry_quote.timestamp)
    assert opened["state"]["units"] < 0

    adverse = entry_quote.model_copy(
        update={
            "bid": 155.0,
            "ask": 155.02,
            "timestamp": entry_quote.timestamp + timedelta(seconds=5),
        }
    )
    liquidated = paper_step(descending, adverse, cfg, path, adverse.timestamp)

    assert liquidated["action"] == "buy_to_cover"
    assert liquidated["state"]["units"] == 0
    assert liquidated["state"]["halted"]
    assert liquidated["state"]["liquidation_reason"] == "maintenance_margin"


def test_incomplete_future_bars_do_not_influence_signal(bars, cfg, tmp_path):
    quote = quote_at(bars)
    for column in ["open", "high", "low", "close"]:
        bars.loc[3:, column] *= 0.5
    result = paper_step(bars, quote, cfg, tmp_path / "paper.sqlite", quote.timestamp)
    assert result["action"] == "buy"
