import pandas as pd
import pytest

from trading.cli import execute, parser
from trading.data import merge_bars, read_bars, write_bars


def sided(bars, received_at="2025-01-07T00:00Z"):
    frame = bars.copy()
    for column in ["open", "high", "low", "close"]:
        frame[f"bid_{column}"] = frame[column] - 0.01
        frame[f"ask_{column}"] = frame[column] + 0.01
    frame["received_at"] = pd.Timestamp(received_at)
    frame["source"] = "GMO public API"
    return frame


def test_merge_joins_overlapping_files_and_keeps_the_first_copy(cfg, bars):
    early = sided(bars.iloc[:5], "2025-01-07T00:00Z")
    late = sided(bars.iloc[3:], "2025-01-08T00:00Z")

    merged = merge_bars([early, late], cfg)

    assert merged.timestamp.tolist() == bars.timestamp.tolist()
    assert merged.close.tolist() == bars.close.tolist()
    # Overlapping bars keep the first input's receipt time; later rows come from the second.
    assert (merged.received_at.iloc[:5] == pd.Timestamp("2025-01-07T00:00Z")).all()
    assert (merged.received_at.iloc[5:] == pd.Timestamp("2025-01-08T00:00Z")).all()


def test_merge_orders_inputs_by_time(cfg, bars):
    merged = merge_bars([sided(bars.iloc[4:]), sided(bars.iloc[:4])], cfg)

    assert merged.timestamp.tolist() == bars.timestamp.tolist()


def test_merge_rejects_overlapping_bars_that_disagree(cfg, bars):
    early = sided(bars.iloc[:5])
    late = sided(bars.iloc[3:]).reset_index(drop=True)
    late.loc[1, "bid_close"] -= 0.002
    late.loc[1, "ask_close"] += 0.002

    with pytest.raises(ValueError, match="1 overlapping bars differ"):
        merge_bars([early, late], cfg)


def test_merge_rejects_mixed_schemas(cfg, bars):
    with pytest.raises(ValueError, match="different columns"):
        merge_bars([sided(bars.iloc[:4]), bars.iloc[4:]], cfg)


def test_merge_requires_an_input(cfg):
    with pytest.raises(ValueError, match="no bar files"):
        merge_bars([], cfg)


def test_write_bars_refuses_to_replace_an_existing_file(cfg, bars, tmp_path):
    path = tmp_path / "bars.parquet"
    write_bars(bars, path)

    with pytest.raises(FileExistsError):
        write_bars(bars.iloc[:4], path)
    assert len(read_bars(path, cfg)) == len(bars)

    write_bars(bars.iloc[:4], path, overwrite=True)
    assert len(read_bars(path, cfg)) == 4


def test_merge_bars_command_reports_overlap_and_gaps(bars, tmp_path):
    config = tmp_path / "fx.toml"
    config.write_text(
        'market = "fx"\nsymbol = "USD_JPY"\nbar_seconds = 3600\nfast = 2\nslow = 3\n',
        encoding="utf-8",
    )
    first, second = tmp_path / "a.parquet", tmp_path / "b.parquet"
    write_bars(sided(bars.iloc[:5]), first)
    # Leave out one hour so the merged series carries a gap for the quality report.
    write_bars(sided(bars.drop(index=6).iloc[3:]), second)
    output = tmp_path / "merged.parquet"
    args = ["merge-bars", "--config", str(config), "--input", str(first), "--input", str(second)]

    result = execute(parser().parse_args([*args, "--output", str(output)]))

    assert result["bars"] == len(bars) - 1
    assert result["overlapping_bars"] == 2
    assert result["data_quality"]["gap_count"] == 1
    with pytest.raises(FileExistsError):
        execute(parser().parse_args([*args, "--output", str(output)]))
