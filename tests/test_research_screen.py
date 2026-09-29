import importlib.util
from pathlib import Path

import numpy as np
import pandas as pd
import pytest


def load_research_script(name):
    path = Path(__file__).resolve().parents[1] / "research" / f"{name}.py"
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


screen = load_research_script("screen")
block_pvalue, daily_pnl = screen.block_pvalue, screen.daily_pnl
extra_fill_cost = load_research_script("execution_audit").extra_fill_cost


def test_daily_pnl_keeps_initial_and_final_costs_and_missing_days():
    equity = pd.DataFrame(
        {
            "timestamp": ["2024-01-01T22:00:00Z", "2024-01-01T23:00:00Z", "2024-01-04T12:00:00Z"],
            "equity": [990.0, 1010.0, 1050.0],
        }
    )
    pnl = daily_pnl(equity, 1000.0, 1040.0)
    assert pnl.tolist() == [-10.0, 20.0, 0.0, 30.0]
    assert pnl.sum() == 40.0


def test_bootstrap_negative_or_zero_pnl_cannot_pass():
    assert block_pvalue(np.array([-3.0, 1.0, -2.0, 0.0]), 2, 100, 7) == 1.0
    assert block_pvalue(np.zeros(4), 2, 100, 7) == 1.0


def test_bootstrap_constant_positive_pnl_has_finite_monte_carlo_floor():
    assert block_pvalue(np.ones(20), 5, 999, 7) == pytest.approx(0.001)


def test_bootstrap_reproducible_and_scale_invariant():
    pnl = np.array([1.0, -3.0, 5.0, 4.0, -2.0] * 10)
    assert block_pvalue(pnl, 5, 999, 7) == block_pvalue(pnl * 100, 5, 999, 7)


@pytest.mark.parametrize("pnl,block", [(np.array([1.0]), 2), (np.array([np.nan]), 1)])
def test_bootstrap_rejects_invalid_series(pnl, block):
    with pytest.raises(ValueError):
        block_pvalue(pnl, block, 100, 7)


def test_spread_sensitivity_accounts_for_both_commission_directions():
    # 2 sen widening costs 1 yen for 100 units, plus/minus the fee-base change.
    assert extra_fill_cost(100, 0.03, 0.01, 0.001) == pytest.approx(1.001)
    assert extra_fill_cost(-100, 0.03, 0.01, 0.001) == pytest.approx(0.999)
    # Conservative: never reward a narrower-than-assumed spread.
    assert extra_fill_cost(100, 0.003, 0.01, 0.001) == 0
