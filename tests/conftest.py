from functools import cache

import pandas as pd
import pytest

from trading import live_journal
from trading.config import Settings

_implementation_sha256 = cache(live_journal.implementation_sha256)


@pytest.fixture(autouse=True)
def cached_implementation_sha256(monkeypatch):
    # The live code fingerprint rehashes source files and package metadata on every
    # check; the files cannot change mid-test, so hash once. Tests that simulate a
    # code update still monkeypatch the same attribute, which overrides this one.
    monkeypatch.setattr(live_journal, "implementation_sha256", _implementation_sha256)


@pytest.fixture
def cfg():
    return Settings(
        market="fx",
        symbol="USD_JPY",
        bar_seconds=3600,
        fast=2,
        slow=3,
        min_units=100,
        max_units=1000,
        spread=0.02,
        slippage=0.01,
        commission_rate=0.001,
    )


@pytest.fixture
def bars():
    prices = [150.0, 151.0, 152.0, 154.0, 153.0, 150.0, 149.0, 148.0]
    return pd.DataFrame(
        {
            "timestamp": pd.date_range("2025-01-06", periods=len(prices), freq="h", tz="UTC"),
            "symbol": "USD_JPY",
            "open": prices,
            "high": [p + 1 for p in prices],
            "low": [p - 1 for p in prices],
            "close": prices,
            "volume": 0,
        }
    )
