import sqlite3
from functools import cache

import pandas as pd
import pytest

from trading import live_journal
from trading.config import Settings

_implementation_sha256 = cache(live_journal.implementation_sha256)
_sqlite_connect = sqlite3.connect


class _UnsyncedConnection(sqlite3.Connection):
    # synchronous=FULL protects committed rows from power loss; the tests only simulate
    # process exits, which keep OS-buffered writes, so skipping the flushes is safe here.
    # Likewise the on-disk rollback journal only matters if this process dies mid-commit.
    # Real crashes happen in child processes, which keep it, and hot journals they leave
    # are still rolled back on open. WAL databases created by a test keep their mode.
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        super().execute("PRAGMA synchronous=OFF")
        if super().execute("PRAGMA journal_mode").fetchone()[0] == "delete":
            super().execute("PRAGMA journal_mode=MEMORY")

    def execute(self, sql, *args):
        if sql == "PRAGMA synchronous=FULL":
            sql = "PRAGMA synchronous=OFF"
        return super().execute(sql, *args)


def _unsynced_connect(*args, factory=_UnsyncedConnection, **kwargs):
    return _sqlite_connect(*args, factory=factory, **kwargs)


@pytest.fixture(autouse=True)
def unsynced_sqlite(monkeypatch):
    monkeypatch.setattr(sqlite3, "connect", _unsynced_connect)


@pytest.fixture(autouse=True)
def cached_implementation_sha256(monkeypatch):
    # The live code fingerprint rehashes source files and package metadata on every
    # check; the files cannot change mid-test, so hash once. Tests that simulate a
    # code update still monkeypatch the same attribute, which overrides this one.
    monkeypatch.setattr(live_journal, "implementation_sha256", _implementation_sha256)


@pytest.fixture
def short_owner_wait(monkeypatch):
    """Shorten the operator wait for tests that hold the owner and expect a refusal."""
    for module in ("stream_control", "private_sync", "private_supervisor"):
        monkeypatch.setattr(f"trading.{module}.OPERATOR_OWNER_WAIT_SECONDS", 0.2)
    return 0.2


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
