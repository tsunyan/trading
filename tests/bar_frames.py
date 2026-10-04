"""Shared hourly bars for tests that cannot request the conftest fixture directly."""

import pandas as pd


def bars_frame():
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
