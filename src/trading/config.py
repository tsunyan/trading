import hashlib
import tomllib
from pathlib import Path
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator


class Settings(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, allow_inf_nan=False)

    market: Literal["fx", "jp_equity"]
    symbol: str
    bar_seconds: int = Field(gt=0)
    initial_cash: float = Field(default=1_000_000, gt=0)
    fast: int = Field(default=12, ge=2)
    slow: int = Field(default=48, ge=3)
    allocation: float = Field(default=0.2, gt=0, le=1)
    lot_size: int = Field(default=1, ge=1)
    min_units: int = Field(default=100, ge=1)
    max_units: int = Field(default=10000, ge=1)
    commission_rate: float = Field(default=0.00002, ge=0, lt=0.1)
    spread: float = Field(default=0.01, ge=0)
    slippage: float = Field(default=0.002, ge=0)
    max_drawdown: float = Field(default=0.05, gt=0, lt=1)
    max_quote_age_seconds: int = Field(default=30, gt=0)
    max_signal_age_seconds: int = Field(default=3900, gt=0)
    max_spread: float = Field(default=0.05, gt=0)

    @model_validator(mode="after")
    def validate_relationships(self):
        if self.fast >= self.slow:
            raise ValueError("fast must be smaller than slow")
        if self.max_units < self.min_units or self.min_units % self.lot_size:
            raise ValueError("invalid quantity limits / lot size")
        if self.market == "fx" and (self.symbol != "USD_JPY" or self.bar_seconds != 3600):
            raise ValueError("initial FX adapter supports USD_JPY hourly bars only")
        if self.market == "jp_equity" and self.bar_seconds != 86400:
            raise ValueError("initial equity adapter supports daily bars only")
        return self

    @property
    def fingerprint(self) -> str:
        return hashlib.sha256(self.model_dump_json().encode()).hexdigest()


def load_settings(path: Path) -> Settings:
    with path.open("rb") as stream:
        return Settings.model_validate(tomllib.load(stream))
