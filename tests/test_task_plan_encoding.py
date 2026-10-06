"""Actual Windows PowerShell PlanOnly must preserve Japanese and spaced paths."""

import json
import os
import subprocess
from pathlib import Path

import pytest
from test_private_sync import make_setup

from trading.observer import initialize
from trading.private_operations import PrivateOperations


@pytest.mark.skipif(os.name != "nt", reason="Windows PowerShell boundary")
@pytest.mark.parametrize("kind,code_page", [("paper", 932), ("private", 65001)])
def test_plan_only_outputs_ascii_json_on_japanese_or_utf8_console(tmp_path, cfg, kind, code_page):
    root = tmp_path / "検証口座 with spaces"
    root.mkdir()
    if kind == "paper":
        directory = root / "paper"
        initialize(directory, cfg)
        files = [directory / "manifest.json"]
        script_name = "install-paper-tasks.ps1"
    else:
        workspace = make_setup(root)[-1]
        directory = workspace.directory
        operations = PrivateOperations.create(directory)
        files = [
            workspace.control.path,
            workspace.journal.path,
            workspace.book.path,
            operations.path,
        ]
        script_name = "install-private-watchdog.ps1"
    before = [path.read_bytes() for path in files]
    powershell = Path(os.environ["SystemRoot"]) / "System32/WindowsPowerShell/v1.0/powershell.exe"
    script = Path(__file__).resolve().parents[1] / "scripts" / script_name

    def literal(path):
        return "'" + str(path).replace("'", "''") + "'"

    command = (
        "$savedEncoding = [Console]::OutputEncoding; try { "
        f"[Console]::OutputEncoding = [Text.Encoding]::GetEncoding({code_page}); "
        f"& {literal(script)} -Directory {literal(directory)} -PlanOnly "
        "} finally { [Console]::OutputEncoding = $savedEncoding }"
    )
    result = subprocess.run(
        [
            str(powershell),
            "-NoProfile",
            "-NonInteractive",
            "-WindowStyle",
            "Hidden",
            "-ExecutionPolicy",
            "Bypass",
            "-Command",
            command,
        ],
        capture_output=True,
        timeout=20,
    )
    assert result.returncode == 0, result.stderr
    # Decoding as ASCII also fails if PowerShell has re-expanded escaped paths.
    plan = json.loads(result.stdout.decode("ascii"))
    assert plan["directory"] == str(directory.resolve())
    assert len(plan["tasks"]) == (2 if kind == "paper" else 1)
    assert all(str(directory) in task["arguments"] for task in plan["tasks"])
    assert [path.read_bytes() for path in files] == before
