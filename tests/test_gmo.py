from datetime import date

import httpx
import pandas as pd
import pytest

from trading.data import validate_bars
from trading.gmo import GmoPublic, trading_date


def test_public_only_midpoint_and_incomplete_bar_removal(cfg):
    requests = []

    def handler(request):
        requests.append(request)
        offset = 0.02 if request.url.params["priceType"] == "ASK" else 0
        return httpx.Response(
            200,
            json={
                "status": 0,
                "data": [
                    {
                        "openTime": str(int(pd.Timestamp(t).timestamp() * 1000)),
                        "open": str(150 + offset),
                        "high": str(151 + offset),
                        "low": str(149 + offset),
                        "close": str(150.5 + offset),
                    }
                    for t in ["2025-01-06T00:00Z", "2025-01-06T01:00Z"]
                ],
            },
        )

    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        result = GmoPublic(client).candles(
            cfg,
            date(2025, 1, 6),
            date(2025, 1, 6),
            pd.Timestamp("2025-01-06T01:30Z").to_pydatetime(),
        )
    assert len(result) == 1
    assert result.close.iloc[0] == pytest.approx(150.51)
    assert result.bid_close.iloc[0] == pytest.approx(150.5)
    assert result.ask_close.iloc[0] == pytest.approx(150.52)
    assert result.received_at.dt.tz is not None
    assert all(r.method == "GET" and r.url.path == "/public/v1/klines" for r in requests)
    assert all("authorization" not in r.headers for r in requests)


def test_candles_start_at_the_first_published_trading_date(cfg):
    requested = []

    def handler(request):
        requested.append(request.url.params["date"])
        return httpx.Response(
            200,
            json={
                "status": 0,
                "data": [
                    {
                        "openTime": str(int(pd.Timestamp("2023-10-26T21:00Z").timestamp() * 1000)),
                        "open": "150",
                        "high": "151",
                        "low": "149",
                        "close": "150.5",
                    }
                ],
            },
        )

    now = pd.Timestamp("2023-10-28T00:00Z").to_pydatetime()
    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        api = GmoPublic(client)
        assert len(api.candles(cfg, date(2023, 10, 27), date(2023, 10, 27), now)) == 1
        with pytest.raises(ValueError, match="trading-date range"):
            api.candles(cfg, date(2023, 10, 26), date(2023, 10, 27), now)
    assert set(requested) == {"20231027"}


def test_candles_reject_a_trading_date_that_has_not_started(cfg):
    requests = []
    # 02:49 JST on 2026-09-25 still belongs to the 2026-09-24 trading date.
    now = pd.Timestamp("2026-09-24T17:49Z").to_pydatetime()
    with httpx.Client(transport=httpx.MockTransport(requests.append)) as client:
        with pytest.raises(ValueError, match="current GMO trading date 2026-09-24"):
            GmoPublic(client).candles(cfg, date(2026, 9, 1), date(2026, 9, 25), now)
    assert trading_date(pd.Timestamp("2026-09-24T21:00Z").to_pydatetime()) == date(2026, 9, 25)
    assert requests == []


def test_api_error_is_not_empty_success():
    with (
        httpx.Client(
            transport=httpx.MockTransport(
                lambda r: httpx.Response(200, json={"status": 1, "messages": []}),
            )
        ) as client,
        pytest.raises(ValueError, match="API returned"),
    ):
        GmoPublic(client).quote("USD_JPY")


def test_private_endpoint_not_available():
    with httpx.Client() as client, pytest.raises(ValueError, match="unsupported"):
        GmoPublic(client).get("order")


def test_side_columns_must_substantiate_mid_prices(cfg, bars):
    sided = bars.copy()
    for column in ["open", "high", "low", "close"]:
        sided[f"bid_{column}"] = sided[column] - 0.01
        sided[f"ask_{column}"] = sided[column] + 0.01
    validate_bars(sided, cfg)

    sided.loc[2, "close"] += 0.5
    with pytest.raises(ValueError, match="midpoint"):
        validate_bars(sided, cfg)


def test_each_side_must_have_a_valid_ohlc_envelope(cfg, bars):
    sided = bars.copy()
    for column in ["open", "high", "low", "close"]:
        sided[f"bid_{column}"] = sided[column] - 0.01
        sided[f"ask_{column}"] = sided[column] + 0.01
    # BID high below its open, offset on the ASK side so the mid candle stays valid.
    sided.loc[2, "bid_high"] = sided.loc[2, "bid_open"] - 0.5
    sided.loc[2, "ask_high"] += 0.49

    with pytest.raises(ValueError, match="BID OHLC envelope"):
        validate_bars(sided, cfg)
