import math
import time as clock
from datetime import UTC, date, datetime, time, timedelta
from typing import Literal
from zoneinfo import ZoneInfo

import httpx
import pandas as pd
from pydantic import BaseModel, ConfigDict, Field, model_validator

from trading.config import Settings
from trading.data import validate_bars

PUBLIC_URL = "https://forex-api.coin.z.com/public/v1"
# Earliest trading date the public klines endpoint serves; the day before returns 404.
FIRST_TRADING_DATE = date(2023, 10, 27)


SWAP_CALENDAR_URL = "https://coin.z.com/api/v1/fx/master/getAllSwapListByDate"
# Product ids the swap calendar page uses; it has no symbol field of its own.
SWAP_PRODUCT_IDS = {"USD_JPY": 100001}
JST = ZoneInfo("Asia/Tokyo")


def trading_date(now: datetime) -> date:
    """GMO trading dates roll over at 06:00 JST, not at UTC or local midnight."""
    return (now.astimezone(JST) - timedelta(hours=6)).date()


def rollover_time(day: date) -> pd.Timestamp:
    """The 06:00 JST rollover that ends trading date `day`, when its swap is granted."""
    return pd.Timestamp(datetime.combine(day + timedelta(days=1), time(6), JST))


class Quote(BaseModel):
    model_config = ConfigDict(frozen=True, allow_inf_nan=False)

    symbol: str
    bid: float = Field(gt=0)
    ask: float = Field(gt=0)
    timestamp: datetime
    status: str

    @model_validator(mode="after")
    def coherent(self):
        if self.ask < self.bid or self.timestamp.tzinfo is None:
            raise ValueError("invalid quote spread or timezone")
        return self


class GmoPublic:
    """Only unauthenticated GETs to the fixed public host; no order API."""

    def __init__(self, client: httpx.Client):
        self.client = client

    def get(self, route: Literal["klines", "ticker", "symbols"], **params):
        if route not in {"klines", "ticker", "symbols"}:
            raise ValueError("unsupported public endpoint")
        response = self.client.get(f"{PUBLIC_URL}/{route}", params=params, timeout=20)
        response.raise_for_status()
        payload = response.json()
        if payload.get("status") != 0 or not isinstance(payload.get("data"), list):
            raise ValueError("GMO public API returned an error or unexpected schema")
        return payload["data"]

    def quote(self, symbol: str) -> Quote:
        for item in self.get("ticker"):
            if item["symbol"] == symbol:
                return Quote.model_validate(item)
        raise ValueError("requested symbol missing from ticker")

    def validate_rules(self, cfg: Settings) -> None:
        rule = next((item for item in self.get("symbols") if item["symbol"] == cfg.symbol), None)
        if rule is None:
            raise ValueError("symbol missing from trading rules")
        if (
            cfg.min_units < int(rule["minOpenOrderSize"])
            or cfg.lot_size % int(rule["sizeStep"])
            or cfg.max_units > int(rule["maxOrderSize"])
        ):
            raise ValueError("configured quantity limits conflict with current GMO API rules")

    def candles(
        self,
        cfg: Settings,
        start: date,
        end: date,
        now: datetime | None = None,
    ) -> pd.DataFrame:
        if cfg.market != "fx":
            raise ValueError("GMO collector requires an FX configuration")
        if start > end or start < FIRST_TRADING_DATE:
            raise ValueError("invalid GMO trading-date range")
        now = now or datetime.now(UTC)
        if now.tzinfo is None:
            raise ValueError("now must have a timezone")
        # A trading date that has not started returns 404; fail before hundreds of requests.
        if end > trading_date(now):
            raise ValueError(f"end is after the current GMO trading date {trading_date(now)}")
        if (end - start).days > 366:
            raise ValueError("fetch at most 367 trading dates per call")
        sides = {}
        for side in ("BID", "ASK"):
            items = []
            for offset in range((end - start).days + 1):
                day = start + timedelta(days=offset)
                items.extend(
                    self.get(
                        "klines",
                        symbol=cfg.symbol,
                        priceType=side,
                        interval="1hour",
                        date=day.strftime("%Y%m%d"),
                    )
                )
            if not items:
                raise ValueError("no candles returned for requested trading dates")
            frame = pd.DataFrame(items)
            frame["timestamp"] = pd.to_datetime(
                pd.to_numeric(frame.openTime, errors="raise"),
                unit="ms",
                utc=True,
            )
            frame = frame.set_index("timestamp")[["open", "high", "low", "close"]].astype(float)
            if frame.index.duplicated().any():
                raise ValueError("duplicate timestamps in GMO candles")
            sides[side] = frame.sort_index()
        bid, ask = sides["BID"], sides["ASK"]
        if not bid.index.equals(ask.index):
            raise ValueError("BID/ASK candles do not align")
        if (ask < bid).any().any():
            raise ValueError("crossed BID/ASK candles")
        # Midpoint OHLC remains the strategy input; preserve both source sides for later audits.
        frame = (bid + ask) / 2
        for side_name, side_frame in (("bid", bid), ("ask", ask)):
            for column in ("open", "high", "low", "close"):
                frame[f"{side_name}_{column}"] = side_frame[column]
        frame = frame[frame.index + pd.Timedelta(seconds=cfg.bar_seconds) <= pd.Timestamp(now)]
        frame["symbol"] = cfg.symbol
        frame["volume"] = 0
        frame["received_at"] = datetime.now(UTC)
        frame["source"] = "GMO public API"
        return validate_bars(frame.reset_index(), cfg)


class GmoSwapCalendar:
    """Unauthenticated GETs to the swap calendar behind GMO's public swap page.

    The endpoint is not part of the documented FX API, so every row is checked against
    the requested date and product before it is trusted.
    """

    def __init__(self, client: httpx.Client, pause_seconds: float = 0.2):
        self.client = client
        self.pause_seconds = pause_seconds

    def day(self, cfg: Settings, day: date) -> dict:
        product_id = SWAP_PRODUCT_IDS.get(cfg.symbol)
        if product_id is None:
            raise ValueError(f"no swap calendar product id for {cfg.symbol}")
        stamp = day.strftime("%Y%m%d")
        response = self.client.get(SWAP_CALENDAR_URL, params={"date": stamp}, timeout=20)
        response.raise_for_status()
        payload = response.json()
        if payload.get("status") != 0 or not isinstance(payload.get("data"), list):
            raise ValueError("GMO swap calendar returned an error or unexpected schema")
        rows = [item for item in payload["data"] if item.get("productId") == product_id]
        if len(rows) != 1 or str(rows[0].get("swapDate")) != stamp:
            raise ValueError(f"GMO swap calendar has no single {cfg.symbol} row for {day}")
        return rows[0]

    def history(
        self,
        cfg: Settings,
        start: date,
        end: date,
        now: datetime | None = None,
    ) -> pd.DataFrame:
        """Swap events in the repository schema, one row per trading date that grants swap.

        A date is fetched only after its rollover has passed: the calendar lists planned
        amounts before then, and GMO may still revise them.
        """
        if cfg.market != "fx":
            raise ValueError("swap history requires an FX configuration")
        if start > end or start < FIRST_TRADING_DATE:
            raise ValueError("invalid GMO trading-date range")
        now = now or datetime.now(UTC)
        if now.tzinfo is None:
            raise ValueError("now must have a timezone")
        if rollover_time(end) > pd.Timestamp(now):
            raise ValueError(f"the swap for {end} is not granted until {rollover_time(end)}")
        records = []
        for offset in range((end - start).days + 1):
            day = start + timedelta(days=offset)
            if offset and self.pause_seconds:
                clock.sleep(self.pause_seconds)
            row = self.day(cfg, day)
            days = int(row["swapDays"])
            buy, sell = float(row["swapBuy"]), float(row["swapSell"])
            if days < 0 or not (math.isfinite(buy) and math.isfinite(sell)):
                raise ValueError(f"invalid swap calendar row for {day}")
            if days == 0:
                if buy or sell:
                    raise ValueError(f"swap amounts on a zero-day row for {day}")
                continue
            records.append(
                {
                    "timestamp": rollover_time(day).isoformat(),
                    "symbol": cfg.symbol,
                    "long_jpy_per_10k": buy,
                    "short_jpy_per_10k": sell,
                    "days": days,
                }
            )
        if not records:
            raise ValueError("no swap was granted in the requested trading dates")
        return pd.DataFrame(records)
