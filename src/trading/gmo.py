from datetime import UTC, date, datetime, timedelta
from typing import Literal

import httpx
import pandas as pd
from pydantic import BaseModel, ConfigDict, Field, model_validator

from trading.config import Settings
from trading.data import validate_bars

PUBLIC_URL = "https://forex-api.coin.z.com/public/v1"


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
        if start > end or start < date(2023, 10, 28):
            raise ValueError("invalid GMO trading-date range")
        now = now or datetime.now(UTC)
        if now.tzinfo is None:
            raise ValueError("now must have a timezone")
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
        # Midpoint OHLC is an approximation: side extrema need not occur simultaneously.
        frame = (bid + ask) / 2
        frame = frame[frame.index + pd.Timedelta(seconds=cfg.bar_seconds) <= pd.Timestamp(now)]
        frame["symbol"] = cfg.symbol
        frame["volume"] = 0
        return validate_bars(frame.reset_index(), cfg)
