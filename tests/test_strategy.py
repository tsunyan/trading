import pytest

from trading.strategy import signal_direction


@pytest.mark.parametrize(
    ("strategy", "closes", "expected"),
    [
        ("momentum", [100.0, 100.0, 101.0], 1),
        ("momentum", [100.0, 100.0, 99.0], -1),
        ("mean_reversion", [100.0, 100.0, 101.0], -1),
        ("mean_reversion", [100.0, 100.0, 99.0], 1),
    ],
)
def test_fixed_return_hypotheses_choose_direction(cfg, strategy, closes, expected):
    cfg = cfg.model_copy(
        update={
            "strategy": strategy,
            "lookback": 2,
            "signal_threshold": 0.005,
            "allow_short": True,
        }
    )

    assert signal_direction(closes, cfg) == expected


def test_fixed_return_hypotheses_respect_threshold_and_short_setting(cfg):
    cfg = cfg.model_copy(
        update={
            "strategy": "momentum",
            "lookback": 2,
            "signal_threshold": 0.02,
            "allow_short": False,
        }
    )

    assert signal_direction([100.0, 100.0, 101.0], cfg) == 0
    assert signal_direction([100.0, 100.0, 97.0], cfg) == 0
