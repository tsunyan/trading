import json
import socket

import pytest

from trading.event_journal_lab import main


def test_offline_demo_and_replay_preserve_unknown_delivery(tmp_path, capsys, monkeypatch):
    monkeypatch.setattr(socket, "socket", lambda *a, **k: pytest.fail("network attempted"))
    path = tmp_path / "demo"
    args = ["--directory", str(path), "--scope", "synthetic"]
    main(["demo", *args])
    output = json.loads(capsys.readouterr().out)
    assert output == json.loads((path / "report.json").read_text())
    assert output["synthetic_only"] and output["journal"]["unacknowledged_records"] == [4]
    before = (path / "event-journal.sqlite").read_bytes()
    main(["replay", *args])
    replay = json.loads(capsys.readouterr().out)
    assert replay == output["replay"] and replay["resync_required"]
    assert replay["outcomes"][-1]["phase"] == "DISCONNECTED"
    assert before == (path / "event-journal.sqlite").read_bytes()


def test_init_status_and_no_overwrite(tmp_path, capsys):
    path = tmp_path / "journal"
    args = ["--directory", str(path), "--scope", "synthetic"]
    main(["init", *args])
    initialized = json.loads(capsys.readouterr().out)
    main(["status", *args])
    assert json.loads(capsys.readouterr().out) == initialized
    for command in ("init", "demo"):
        with pytest.raises(SystemExit) as caught:
            main([command, *args])
        assert caught.value.code == 2
    assert not (path / "report.json").exists()


def test_missing_journal_is_not_created(tmp_path, capsys):
    path = tmp_path / "missing"
    with pytest.raises(SystemExit) as caught:
        main(["status", "--directory", str(path), "--scope", "synthetic"])
    assert caught.value.code == 2 and not path.exists()
    assert not capsys.readouterr().out
