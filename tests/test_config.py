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
        {"max_leverage": 0.5},
        {"maintenance_margin_ratio": 1.1},
        {"strategy": "unknown"},
    ],
)
def test_invalid_configuration_rejected(cfg, changes):
    with pytest.raises(ValidationError):
        Settings.model_validate(cfg.model_dump() | changes)


def test_strategy_specific_warmup_and_parameters(cfg):
    momentum = cfg.model_copy(
        update={"strategy": "momentum", "lookback": 24, "signal_threshold": 0.01}
    )

    assert cfg.warmup_bars == cfg.slow
    assert cfg.strategy_parameters == {"fast": cfg.fast, "slow": cfg.slow}
    assert momentum.warmup_bars == 25
    assert momentum.strategy_parameters == {"lookback": 24, "signal_threshold": 0.01}


def test_experiment_id_changes_with_runtime_versions(cfg, monkeypatch):
    from trading import provenance

    before = provenance.reproducibility_fields(cfg, "data", None, {"mode": "backtest"})
    monkeypatch.setattr(
        provenance,
        "runtime_versions",
        lambda: {**before["runtime"], "backtrader": "0.0.0"},
    )
    after = provenance.reproducibility_fields(cfg, "data", None, {"mode": "backtest"})

    assert before["runtime"]["python"]
    assert before["experiment_id"] != after["experiment_id"]
