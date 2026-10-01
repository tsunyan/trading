import json
import socket
from datetime import UTC, datetime

import pytest

from trading.account_sync_lab import SyncTranscript, demo_transcript_events, main, replay

NOW = datetime(2026, 9, 30, tzinfo=UTC)


def test_demo_and_replay_are_identical_offline(tmp_path, capsys, monkeypatch):
    monkeypatch.setattr(socket, "socket", lambda *a, **k: pytest.fail("network attempted"))
    directory = tmp_path / "demo"
    main(["demo", "--directory", str(directory)])
    summary = json.loads(capsys.readouterr().out)
    assert summary["synthetic_only"] and not summary["live_enabled"]
    assert summary["steps"] == 9
    main(["replay", "--input", str(directory / "transcript.json")])
    result = json.loads(capsys.readouterr().out)
    assert result == json.loads((directory / "report.json").read_text())
    assert result["offline_only"]


def test_scenario_exhibits_delay_gap_resync_and_liveness():
    transcript = demo_transcript_events(NOW)
    first = replay(transcript)
    assert first == replay(transcript)
    rows = first["steps"]
    assert rows[3]["assessment"]["mismatches"] == ["position_event_mismatch:401"]
    assert rows[4]["assessment"]["structural_match"]
    assert rows[5]["error"] == "local_receive_gap_or_reorder"
    assert rows[7]["assessment"]["structural_match"]
    assert "history_gap_not_repaired" in rows[7]["assessment"]["blockers"]
    assert rows[8]["status"]["reason"] == "stream_liveness_expired"
    for row in rows:
        assert not row["status"]["complete"] and not row["status"]["live_enabled"]


def test_output_directory_never_overwritten(tmp_path, capsys):
    with pytest.raises(SystemExit) as caught:
        main(["demo", "--directory", str(tmp_path)])
    assert caught.value.code == 2
    assert not (tmp_path / "report.json").exists()


@pytest.mark.parametrize(
    "contents",
    [
        b"x" * 2_000_001,
        b'{"private_data":"must-not-echo"}',
        b'{"version":1,"version":1,"steps":[]}',
        b'{"steps":[],"version":NaN}',
    ],
    ids=["oversized", "unknown_fields", "duplicate_key", "nonfinite"],
)
def test_invalid_input_is_bounded_and_redacted(tmp_path, capsys, contents):
    path = tmp_path / "input.json"
    path.write_bytes(contents)
    with pytest.raises(SystemExit) as caught:
        main(["replay", "--input", str(path)])
    assert caught.value.code == 2
    captured = capsys.readouterr()
    assert not captured.out and "must-not-echo" not in captured.err


def test_invalid_raw_event_survives_envelope_until_parser_rejects_it():
    data = demo_transcript_events(NOW).model_dump(mode="json")
    data["steps"][2]["payload"] = '{"channel":"positionEvents","channel":"positionEvents"}'
    results = replay(SyncTranscript.model_validate(data))
    assert results["steps"][2]["error"] == "event_rejected"
    assert results["steps"][2]["status"]["phase"] == "DISCONNECTED"
