import pytest
from pydantic import ValidationError

from trading.config import Settings


@pytest.mark.parametrize(
    "changes",
    [
        {"initial_cash": float("nan")},
        {"allocation": 1.1},
        {"fast": 50},
        {"symbol": "EUR_USD"},
        {"mode": "live"},
        {"lot_size": 101},
    ],
)
def test_invalid_configuration_rejected(cfg, changes):
    with pytest.raises(ValidationError):
        Settings.model_validate(cfg.model_dump() | changes)
