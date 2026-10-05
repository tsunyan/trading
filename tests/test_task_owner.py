"""The shared installer guard against overwriting another user's or an unrelated task."""

import os
import subprocess
from pathlib import Path

import pytest

pytestmark = pytest.mark.skipif(os.name != "nt", reason="Windows PowerShell boundary")

SCRIPTS = Path(__file__).resolve().parents[1] / "scripts"


def check(user_id, description="TradingLab task"):
    """Run the guard against a synthetic existing task; Task Scheduler is never touched."""
    powershell = Path(os.environ["SystemRoot"]) / "System32/WindowsPowerShell/v1.0/powershell.exe"
    existing = (
        "$null"
        if user_id is None
        else "[pscustomobject]@{Description=$env:TASK_DESCRIPTION;"
        "Principal=[pscustomobject]@{UserId=$user}}"
    )
    command = (
        f". '{SCRIPTS / 'task-owner.ps1'}'; "
        "$sid = [Security.Principal.WindowsIdentity]::GetCurrent().User.Value; "
        "$user = $env:TASK_USER; "
        "if ($user -eq 'CURRENT_SID') { $user = $sid } "
        "if ($user -eq 'CURRENT_NAME') "
        "{ $user = [Security.Principal.WindowsIdentity]::GetCurrent().Name } "
        f"Assert-OwnScheduledTask -Existing ({existing}) -Name 'Task' "
        "-Description 'TradingLab task' -CurrentSid $sid; 'ok'"
    )
    return subprocess.run(
        [
            str(powershell),
            "-NoProfile",
            "-NonInteractive",
            "-ExecutionPolicy",
            "Bypass",  # This test process only; user/machine policy stays unchanged.
            "-Command",
            command,
        ],
        capture_output=True,
        text=True,
        errors="replace",
        env={**os.environ, "TASK_USER": user_id or "", "TASK_DESCRIPTION": description},
        timeout=60,
    )


@pytest.mark.parametrize("user_id", [None, "CURRENT_SID", "CURRENT_NAME"])
def test_own_task_or_no_task_may_be_updated(user_id):
    result = check(user_id)
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == "ok"


@pytest.mark.parametrize(
    "user_id,description,message",
    [
        ("CURRENT_SID", "Someone else's task", "unrelated scheduled task"),
        ("S-1-5-18", "TradingLab task", "unrelated scheduled task"),  # LocalSystem
        ("NoSuchDomain\\no-such-user-xyz", "TradingLab task", "Cannot verify"),
    ],
)
def test_other_owner_description_or_unresolvable_owner_is_refused(user_id, description, message):
    result = check(user_id, description)
    assert result.returncode != 0
    assert message in result.stderr


def test_every_installer_uses_the_shared_guard():
    for name in (
        "install-paper-tasks.ps1",
        "install-private-watchdog.ps1",
        "install-live-cycle.ps1",
    ):
        text = (SCRIPTS / name).read_text(encoding="utf-8")
        assert "task-owner.ps1" in text and "Assert-OwnScheduledTask" in text
        assert "Register-ScheduledTask -TaskPath '\\'" in text
