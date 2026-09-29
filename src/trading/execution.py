"""Research-only execution assumptions, separate from paper-account settings."""

import math
from typing import Literal

import numpy as np
import pandas as pd
from pydantic import BaseModel, ConfigDict, Field, model_validator

from trading.config import Settings
from trading.data import SIDE_PRICE_COLUMNS


class ExecutionModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, allow_inf_nan=False)

    mode: Literal["fixed", "bid_ask"] = "fixed"
    observed_spread_multiplier: float = Field(default=1.0, ge=1.0)

    @model_validator(mode="after")
    def validate_fixed_multiplier(self):
        if self.mode == "fixed" and self.observed_spread_multiplier != 1:
            raise ValueError("fixed execution requires observed_spread_multiplier=1")
        return self

    def stressed(self, multiplier: float) -> "ExecutionModel":
        if not math.isfinite(multiplier) or multiplier < 1:
            raise ValueError("stress multiplier must be finite and >= 1")
        if self.mode == "fixed":
            return self
        return ExecutionModel(
            mode=self.mode,
            observed_spread_multiplier=self.observed_spread_multiplier * multiplier,
        )


def execution_prices(frame: pd.DataFrame, cfg: Settings, execution: ExecutionModel) -> pd.DataFrame:
    """Compute side prices from validated bars, never borrowing another bar's quotes.

    Fixed mode preserves legacy mid marks. BID/ASK mode marks longs at effective BID
    and shorts at effective ASK, without exit commission/slippage until liquidation.
    The configured spread is a floor; cfg costs are already scaled in stress runs.
    OHLC sides need not be synchronous, so these remain candle-level approximations.
    """
    observed = execution.mode == "bid_ask"
    if observed and not set(SIDE_PRICE_COLUMNS).issubset(frame.columns):
        raise ValueError("bid_ask execution requires complete BID/ASK OHLC columns")
    prices = pd.DataFrame(index=frame.index)
    for field in ("open", "close", "low", "high"):
        mid = frame[field].astype(float)
        spread = (
            (frame[f"ask_{field}"] - frame[f"bid_{field}"]) * execution.observed_spread_multiplier
            if observed
            else pd.Series(cfg.spread, index=frame.index)
        )
        half = spread.clip(lower=cfg.spread) / 2
        if field in ("open", "close"):
            prices[f"buy_{field}"] = mid + half + cfg.slippage
            prices[f"sell_{field}"] = mid - half - cfg.slippage
        if field == "close":
            prices["mark_bid"] = mid - half if observed else mid
            prices["mark_ask"] = mid + half if observed else mid
        elif field == "low":
            prices["risk_bid_low"] = mid - half if observed else mid
        elif field == "high":
            prices["risk_ask_high"] = mid + half if observed else mid
    if not np.isfinite(prices.to_numpy()).all() or (prices <= 0).any().any():
        raise ValueError("execution costs imply non-finite or non-positive prices")
    return prices
