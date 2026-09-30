import json
import socket
from datetime import UTC, datetime, timedelta

import pytest

from trading.account_read_lab import Transcript, demo_transcript, main, replay
from trading.account_reader import CollectionError

NOW = datetime(2026, 9, 30, tzinfo=UTC)


def test_demo_and_replay_without_network(tmp_path, monkeypatch, capsys):
    def no_network(*args, **kwargs):
        pytest.fail("network attempted")

    monkeypatch.setattr(socket, "socket", no_network)
    output = tmp_path / "demo"
    main(["demo", "--directory", str(output)])
    summary = json.loads(capsys.readouterr().out)
    assert summary["synthetic_only"] and not summary["live_enabled"]
    assert summary["requests"] == 12
    main(["replay", "--input", str(output / "transcript.json")])
    result = json.loads(capsys.readouterr().out)
    assert result == json.loads((output / "report.json").read_text())
    assert result["positions"][0]["units"] == 400
    assert result["active_orders"][0]["units"] == 1000


def test_demo_never_overwrites_existing_directory(tmp_path, capsys):
    with pytest.raises(SystemExit) as caught:
        main(["demo", "--directory", str(tmp_path)])
    assert caught.value.code == 2
    assert not (tmp_path / "report.json").exists()


@pytest.mark.parametrize("case", ["missing", "extra", "query", "time", "path"])
def test_bad_transcripts(case):
    data = demo_transcript(NOW).model_dump(mode="json")
    if case == "missing":
        data["exchanges"].pop()
    elif case == "extra":
        data["exchanges"].append(data["exchanges"][-1])
    elif case == "query":
        data["exchanges"][1]["query"] = [["count", "10"]]
    elif case == "time":
        data["exchanges"][0]["received_at"] = (NOW - timedelta(seconds=1)).isoformat()
    else:
        data["exchanges"][0]["path"] = "/v1/order"
    with pytest.raises(CollectionError):
        replay(Transcript.model_validate(data))


def test_cli_does_not_echo_bad_input(tmp_path, capsys):
    fixture = tmp_path / "bad.json"
    fixture.write_text('{"private_account_data":"do-not-echo"}', encoding="utf-8")
    with pytest.raises(SystemExit) as caught:
        main(["replay", "--input", str(fixture)])
    assert caught.value.code == 2
    output = capsys.readouterr()
    assert "do-not-echo" not in output.err and not output.out
