import json

import httpx
import pandas as pd
import pytest

from trading.cli import execute, parser
from trading.config import load_settings
from trading.data import (
    lineage_path,
    merge_bars,
    publish_new_file,
    read_bars,
    write_bars,
    write_data_lineage,
)


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
    write_data_lineage(
        first,
        {
            "operation": "test-fixture",
            "collection": {"empty_trading_dates": ["2025-01-06"]},
        },
    )
    # Leave out one hour so the merged series carries a gap for the quality report.
    write_bars(sided(bars.drop(index=6).iloc[3:]), second)
    output = tmp_path / "merged.parquet"
    args = ["merge-bars", "--config", str(config), "--input", str(first), "--input", str(second)]

    result = execute(parser().parse_args([*args, "--output", str(output)]))

    assert result["bars"] == len(bars) - 1
    assert result["overlapping_bars"] == 2
    assert result["data_quality"]["gap_count"] == 1
    assert result["data_quality"]["unexplained_bar_intervals"] == 1
    lineage = json.loads((tmp_path / "merged.parquet.lineage.json").read_text(encoding="utf-8"))
    assert lineage["operation"] == "merge-bars"
    assert lineage["collection"]["empty_trading_dates"] == []
    assert lineage["collection"]["superseded_empty_trading_dates"] == ["2025-01-06"]
    assert lineage["artifact"]["path"] == "merged.parquet"
    assert len(lineage["artifact"]["sha256"]) == 64
    assert len(lineage["inputs"]) == 2
    assert lineage["inputs"][0]["path"] == "a.parquet"
    assert lineage["inputs"][0]["lineage_path"] == "a.parquet.lineage.json"
    assert len(lineage["inputs"][0]["lineage_sha256"]) == 64
    with pytest.raises(FileExistsError):
        execute(parser().parse_args([*args, "--output", str(output)]))


def test_fetch_fx_resumes_completed_dates_after_an_interruption(bars, tmp_path, monkeypatch):
    config = tmp_path / "fx.toml"
    config.write_text(
        'market = "fx"\nsymbol = "USD_JPY"\nbar_seconds = 3600\nfast = 2\nslow = 3\n',
        encoding="utf-8",
    )
    empty_day = pd.Timestamp("2025-01-06").date()
    failed_day = pd.Timestamp("2025-01-08").date()

    class InterruptOnce:
        def __init__(self):
            self.calls = []
            self.interrupted = False

        def validate_candle_range(self, cfg, start, end):
            return None

        def candle_days(self, cfg, start, end):
            assert start == end
            self.calls.append(start)
            if start == failed_day and not self.interrupted:
                self.interrupted = True
                raise httpx.ReadTimeout("temporary failure")
            if start == empty_day:
                frame = pd.DataFrame()
                frame.attrs["empty_reason"] = "provider_empty"
                yield start, frame
                return
            frame = sided(bars.iloc[[0]]).copy()
            frame["timestamp"] = pd.Timestamp(start, tz="UTC")
            yield start, frame

    api = InterruptOnce()
    monkeypatch.setattr("trading.cli.GmoPublic", lambda client: api)
    output = tmp_path / "bars.parquet"
    common = [
        "fetch-fx",
        "--config",
        str(config),
        "--start",
        "2025-01-06",
        "--end",
        "2025-01-09",
        "--output",
        str(output),
    ]

    with pytest.raises(httpx.ReadTimeout):
        execute(parser().parse_args(common))
    checkpoint = tmp_path / "bars.parquet.fetch-fx"
    assert not output.exists()
    assert (checkpoint / "2025-01-06.empty").exists()
    assert (checkpoint / "2025-01-07.parquet").exists()

    changed = [*common]
    changed[changed.index("2025-01-09")] = "2025-01-10"
    with pytest.raises(ValueError, match="different request"):
        execute(parser().parse_args(changed))

    part = checkpoint / "2025-01-07.parquet"
    saved_part = read_bars(part, load_settings(config))
    wrong_day = saved_part.copy()
    wrong_day["timestamp"] = pd.Timestamp("2025-01-08", tz="UTC")
    write_bars(wrong_day, part, overwrite=True)
    with pytest.raises(ValueError, match="contains bars from"):
        execute(parser().parse_args(common))
    write_bars(saved_part, part, overwrite=True)

    result = execute(parser().parse_args(common))

    assert api.calls == [
        pd.Timestamp("2025-01-06").date(),
        pd.Timestamp("2025-01-07").date(),
        pd.Timestamp("2025-01-08").date(),
        pd.Timestamp("2025-01-08").date(),
        pd.Timestamp("2025-01-09").date(),
    ]
    assert result["resumed_dates"] == 2
    assert result["fetched_dates"] == 2
    loaded = read_bars(output, load_settings(config))
    assert len(loaded) == 3
    assert not checkpoint.exists()
    lineage = json.loads((tmp_path / "bars.parquet.lineage.json").read_text(encoding="utf-8"))
    assert lineage["operation"] == "fetch-fx"
    assert lineage["collection"]["empty_trading_dates"] == ["2025-01-06"]
    assert lineage["artifact"]["path"] == "bars.parquet"
    assert lineage["transformation"]["derived_columns"]["close"] == "(bid_close + ask_close) / 2"

    changed_artifact = loaded.copy()
    changed_artifact["received_at"] += pd.Timedelta(seconds=1)
    write_bars(changed_artifact, output, overwrite=True)
    with pytest.raises(ValueError, match="does not match"):
        read_bars(output, load_settings(config))


def test_fetch_fx_does_not_checkpoint_a_day_with_only_incomplete_bars(tmp_path, monkeypatch):
    config = tmp_path / "fx.toml"
    config.write_text(
        'market = "fx"\nsymbol = "USD_JPY"\nbar_seconds = 3600\nfast = 2\nslow = 3\n',
        encoding="utf-8",
    )

    class IncompleteOnly:
        def validate_candle_range(self, cfg, start, end):
            return None

        def candle_days(self, cfg, start, end):
            frame = pd.DataFrame()
            frame.attrs["empty_reason"] = "incomplete_only"
            yield start, frame

    monkeypatch.setattr("trading.cli.GmoPublic", lambda client: IncompleteOnly())
    output = tmp_path / "bars.parquet"
    args = [
        "fetch-fx",
        "--config",
        str(config),
        "--start",
        "2025-01-06",
        "--end",
        "2025-01-06",
        "--output",
        str(output),
    ]

    with pytest.raises(ValueError, match="no completed candles"):
        execute(parser().parse_args(args))

    checkpoint = tmp_path / "bars.parquet.fetch-fx"
    assert (checkpoint / "request.json").exists()
    assert not (checkpoint / "2025-01-06.empty").exists()
    assert not output.exists()
    assert not lineage_path(output).exists()


def test_fetch_fx_does_not_finalize_a_current_trading_date(bars, tmp_path, monkeypatch):
    config = tmp_path / "fx.toml"
    config.write_text(
        'market = "fx"\nsymbol = "USD_JPY"\nbar_seconds = 3600\nfast = 2\nslow = 3\n',
        encoding="utf-8",
    )

    class CurrentTradingDate:
        def validate_candle_range(self, cfg, start, end):
            return None

        def candle_days(self, cfg, start, end):
            frame = sided(bars.iloc[[0]]).copy()
            frame["timestamp"] = pd.Timestamp(start, tz="UTC")
            frame.attrs["trading_date_complete"] = False
            yield start, frame

    monkeypatch.setattr("trading.cli.GmoPublic", lambda client: CurrentTradingDate())
    output = tmp_path / "bars.parquet"
    args = [
        "fetch-fx",
        "--config",
        str(config),
        "--start",
        "2025-01-06",
        "--end",
        "2025-01-06",
        "--output",
        str(output),
    ]

    with pytest.raises(ValueError, match="still in progress"):
        execute(parser().parse_args(args))

    checkpoint = tmp_path / "bars.parquet.fetch-fx"
    assert not (checkpoint / "2025-01-06.parquet").exists()
    assert not output.exists()


def test_merge_rolls_back_output_when_lineage_publication_fails(bars, tmp_path, monkeypatch):
    config = tmp_path / "fx.toml"
    config.write_text(
        'market = "fx"\nsymbol = "USD_JPY"\nbar_seconds = 3600\nfast = 2\nslow = 3\n',
        encoding="utf-8",
    )
    source = tmp_path / "source.parquet"
    write_bars(sided(bars), source)
    output = tmp_path / "merged.parquet"

    def fail_lineage_publication(temporary, target):
        if target == lineage_path(output):
            raise OSError("simulated lineage failure")
        publish_new_file(temporary, target)

    monkeypatch.setattr("trading.cli.publish_new_file", fail_lineage_publication)
    args = [
        "merge-bars",
        "--config",
        str(config),
        "--input",
        str(source),
        "--output",
        str(output),
    ]

    with pytest.raises(OSError, match="simulated lineage failure"):
        execute(parser().parse_args(args))

    assert not output.exists()
    assert not lineage_path(output).exists()


def test_publish_new_file_explains_the_hardlink_requirement(tmp_path, monkeypatch):
    source = tmp_path / "source"
    source.write_bytes(b"complete")

    def unsupported(source, target):
        raise OSError("operation not supported")

    monkeypatch.setattr("trading.data.os.link", unsupported)

    with pytest.raises(OSError, match="filesystem must support hard links"):
        publish_new_file(source, tmp_path / "target")
