from collections.abc import Sequence
from math import floor

from trading.config import Settings


def signal_direction(closes: Sequence[float], cfg: Settings) -> int:
    """Return 1 for long, -1 for short, and 0 for no position."""
    if len(closes) < cfg.warmup_bars:
        return 0
    if cfg.strategy == "sma_cross":
        fast_average = sum(closes[-cfg.fast :]) / cfg.fast
        slow_average = sum(closes[-cfg.slow :]) / cfg.slow
        score = fast_average / slow_average - 1
    else:
        score = closes[-1] / closes[-1 - cfg.lookback] - 1
        if cfg.strategy == "mean_reversion":
            score = -score
    threshold = cfg.signal_threshold if cfg.strategy != "sma_cross" else 0.0
    if score > threshold:
        return 1
    return -1 if cfg.allow_short and score < -threshold else 0


def wants_long(closes: Sequence[float], cfg: Settings) -> bool:
    """Compatibility wrapper for callers that only distinguish long from flat."""
    return signal_direction(closes, cfg) == 1


def entry_units(cash: float, equity: float, price: float, cfg: Settings) -> int:
    if min(cash, equity, price) <= 0:
        return 0
    budget = equity * cfg.allocation * cfg.max_leverage
    budget /= 1 + cfg.commission_rate * cfg.max_leverage
    units = min(floor(budget / price), cfg.max_units)
    units = units // cfg.lot_size * cfg.lot_size
    return units if units >= cfg.min_units else 0


def drawdown_halt(equity: float, peak: float, cfg: Settings) -> bool:
    return equity <= peak * (1 - cfg.max_drawdown)


def margin_metrics(units: int, equity: float, price: float, cfg: Settings) -> dict:
    """Return notional and margin measures for a marked position."""
    gross_notional = abs(units) * price
    required_margin = gross_notional / cfg.max_leverage
    return {
        "gross_notional": gross_notional,
        # None rather than infinity when equity is exhausted, so reports stay strict JSON.
        "effective_leverage": gross_notional / equity if equity > 0 else None,
        "required_margin": required_margin,
        "available_margin": equity - required_margin,
        "margin_ratio": equity / required_margin if required_margin else None,
    }


def maintenance_margin_halt(units: int, equity: float, price: float, cfg: Settings) -> bool:
    metrics = margin_metrics(units, equity, price, cfg)
    return bool(units and equity <= metrics["required_margin"] * cfg.maintenance_margin_ratio)
