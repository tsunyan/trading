import hashlib
import tomllib
from pathlib import Path
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator


class Settings(BaseModel):
    """Frozen research settings; the fingerprint binds a paper account to them."""

    model_config = ConfigDict(extra="forbid", frozen=True, allow_inf_nan=False)

    market: Literal["fx", "jp_equity"]
    symbol: str
    bar_seconds: int = Field(gt=0)
    initial_cash: float = Field(default=1_000_000, gt=0)
    strategy: Literal["sma_cross", "momentum", "mean_reversion"] = "sma_cross"
    fast: int = Field(default=12, ge=2)
    slow: int = Field(default=48, ge=3)
    lookback: int = Field(default=24, ge=2)
    signal_threshold: float = Field(default=0.005, ge=0, lt=1)
    allocation: float = Field(default=0.2, gt=0, le=1)
    lot_size: int = Field(default=1, ge=1)
    min_units: int = Field(default=100, ge=1)
    max_units: int = Field(default=10000, ge=1)
    allow_short: bool = False
    max_leverage: float = Field(default=1.0, ge=1.0)
    maintenance_margin_ratio: float = Field(default=0.5, gt=0, le=1)
    commission_rate: float = Field(default=0.00002, ge=0, lt=0.1)
    spread: float = Field(default=0.01, ge=0)
    slippage: float = Field(default=0.002, ge=0)
    max_drawdown: float = Field(default=0.05, gt=0, lt=1)
    max_quote_age_seconds: int = Field(default=30, gt=0)
    max_future_quote_seconds: int = Field(default=10, ge=0)
    max_signal_age_seconds: int = Field(default=3900, gt=0)
    max_spread: float = Field(default=0.05, gt=0)

    @model_validator(mode="after")
    def validate_relationships(self):
        if self.strategy == "sma_cross" and self.fast >= self.slow:
            raise ValueError("fast must be smaller than slow")
        if self.max_units < self.min_units or self.min_units % self.lot_size:
            raise ValueError("invalid quantity limits / lot size")
        if self.market == "fx" and (self.symbol != "USD_JPY" or self.bar_seconds != 3600):
            raise ValueError("initial FX adapter supports USD_JPY hourly bars only")
        if self.market == "jp_equity" and self.bar_seconds != 86400:
            raise ValueError("initial equity adapter supports daily bars only")
        return self

    @property
    def warmup_bars(self) -> int:
        return self.slow if self.strategy == "sma_cross" else self.lookback + 1

    @property
    def strategy_parameters(self) -> dict:
        if self.strategy == "sma_cross":
            return {"fast": self.fast, "slow": self.slow}
        return {"lookback": self.lookback, "signal_threshold": self.signal_threshold}

    @property
    def fingerprint(self) -> str:
        """Hash of the full settings; a paper database is bound to this value."""
        return hashlib.sha256(self.model_dump_json().encode()).hexdigest()

    @property
    def paper_tolerance_legacy_fingerprints(self) -> frozenset[str]:
        """Compatible hashes from tolerance changes and the pre-margin schema."""
        if (
            self.strategy != "sma_cross"
            or self.lookback != 24
            or self.signal_threshold != 0.005
            or self.allow_short
            or self.max_leverage != 1.0
            or self.maintenance_margin_ratio != 0.5
        ):
            return frozenset()
        previous_tolerance = self.model_copy(update={"max_quote_age_seconds": 30})
        excluded_strategy_fields = {"strategy", "lookback", "signal_threshold"}
        excluded_margin_fields = {
            "allow_short",
            "max_leverage",
            "maintenance_margin_ratio",
        }
        legacy_payloads = [
            self.model_dump_json(exclude=excluded_strategy_fields),
            previous_tolerance.model_dump_json(exclude=excluded_strategy_fields),
            self.model_dump_json(exclude=excluded_margin_fields | excluded_strategy_fields),
            previous_tolerance.model_dump_json(
                exclude=excluded_margin_fields | excluded_strategy_fields
            ),
            previous_tolerance.model_dump_json(
                exclude=(
                    excluded_margin_fields | excluded_strategy_fields | {"max_future_quote_seconds"}
                )
            ),
        ]
        hashes = {hashlib.sha256(payload.encode()).hexdigest() for payload in legacy_payloads}
        hashes.add(previous_tolerance.fingerprint)
        return frozenset(hashes)


def load_settings(path: Path) -> Settings:
    with path.open("rb") as stream:
        return Settings.model_validate(tomllib.load(stream))
