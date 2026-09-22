from collections.abc import Sequence
from math import floor

from trading.config import Settings


def wants_long(closes: Sequence[float], cfg: Settings) -> bool:
    if len(closes) < cfg.slow:
        return False
    return sum(closes[-cfg.fast :]) / cfg.fast > sum(closes[-cfg.slow :]) / cfg.slow


def entry_units(cash: float, equity: float, price: float, cfg: Settings) -> int:
    if min(cash, equity, price) <= 0:
        return 0
    budget = min(equity * cfg.allocation, cash / (1 + cfg.commission_rate))
    units = min(floor(budget / price), cfg.max_units)
    units = units // cfg.lot_size * cfg.lot_size
    return units if units >= cfg.min_units else 0


def drawdown_halt(equity: float, peak: float, cfg: Settings) -> bool:
    return equity <= peak * (1 - cfg.max_drawdown)
