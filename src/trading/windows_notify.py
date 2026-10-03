"""Submit local Windows toasts and keep delivery failures separate from trading."""

import base64
import json
import os
import sqlite3
import subprocess
import time
from contextlib import closing
from datetime import UTC, datetime
from pathlib import Path
from xml.etree.ElementTree import Element, SubElement, tostring

from trading.observer import load_manifest
from trading.paper_runner import _process_lock

# Use the installed Windows PowerShell shortcut's real AppUserModelID. No new registry
# identity, executable activation, or notification action is installed.
TOAST_SCRIPT = r"""
$ErrorActionPreference = 'Stop'
$appId = '{1AC14E77-02E7-4E5D-B744-2EB1AE5198B7}\WindowsPowerShell\v1.0\powershell.exe'
if (-not (Get-StartApps | Where-Object AppID -eq $appId)) {
    throw 'Windows PowerShell notification shortcut is unavailable'
}
_MANAGER_TYPE_ > $null
_TOAST_TYPE_ > $null
_XML_TYPE_ > $null
_SETTING_TYPE_ > $null
$xml = New-Object Windows.Data.Xml.Dom.XmlDocument
$xml.LoadXml([Text.Encoding]::UTF8.GetString([Convert]::FromBase64String($env:TRADINGLAB_TOAST_XML)))
$toast = [Windows.UI.Notifications.ToastNotification]::new($xml)
$toast.Tag = $env:TRADINGLAB_TOAST_TAG
$toast.Group = 'TradingLab'
$notifier = [Windows.UI.Notifications.ToastNotificationManager]::CreateToastNotifier($appId)
$setting = $notifier.get_Setting()
if ($setting.ToString() -ne 'Enabled') {
    throw ('Windows notifications are disabled: ' + $setting)
}
$notifier.Show($toast)
Write-Output 'submitted'
"""
TOAST_SCRIPT = (
    TOAST_SCRIPT.replace(
        "_MANAGER_TYPE_",
        "[Windows.UI.Notifications.ToastNotificationManager,"
        "Windows.UI.Notifications,ContentType=WindowsRuntime]",
    )
    .replace(
        "_TOAST_TYPE_",
        "[Windows.UI.Notifications.ToastNotification,"
        "Windows.UI.Notifications,ContentType=WindowsRuntime]",
    )
    .replace(
        "_XML_TYPE_",
        "[Windows.Data.Xml.Dom.XmlDocument,Windows.Data.Xml.Dom.XmlDocument,"
        "ContentType=WindowsRuntime]",
    )
    .replace(
        "_SETTING_TYPE_",
        "[Windows.UI.Notifications.NotificationSetting,"
        "Windows.UI.Notifications,ContentType=WindowsRuntime]",
    )
)

LABELS = {
    "notification_test": "Windows通知の接続テストです（売買は実行しません）",
    "paper_fill": "模擬口座で新しい約定を記録しました",
    "consecutive_failures": "模擬観測が連続して失敗しています",
    "stale": "模擬観測の定期実行が遅延・停止しています",
    "interrupted": "模擬観測の実行中断を検出しました",
    "risk_halted": "模擬口座の損失停止を検出しました",
    "recovered": "模擬観測の障害から復旧しました",
}


def send_toast(alert, observer_id):
    if os.name != "nt":
        raise OSError("Windows desktop notifications require Windows")
    toast = Element("toast")
    binding = SubElement(SubElement(toast, "visual"), "binding", template="ToastGeneric")
    SubElement(binding, "text").text = "Trading Lab"
    SubElement(binding, "text").text = LABELS.get(alert["kind"], "模擬観測の通知")
    SubElement(binding, "text").text = (
        f"口座 {observer_id[:12]} / 通知 {alert['id']}\n"
        "operations.sqlite または paper_runner status で詳細を確認してください。"
    )
    environment = {
        **os.environ,
        "TRADINGLAB_TOAST_XML": base64.b64encode(tostring(toast, encoding="utf-8")).decode(),
        "TRADINGLAB_TOAST_TAG": f"{observer_id[:6]}-{alert['id']}"[:16],
    }
    powershell = Path(os.environ["SystemRoot"]) / "System32/WindowsPowerShell/v1.0/powershell.exe"
    result = subprocess.run(
        [
            str(powershell),
            "-NoProfile",
            "-NonInteractive",
            "-WindowStyle",
            "Hidden",
            "-Command",
            TOAST_SCRIPT,
        ],
        capture_output=True,
        env=environment,
        timeout=20,
        creationflags=subprocess.CREATE_NO_WINDOW,
    )
    if result.returncode or b"submitted" not in result.stdout:
        raise OSError("Windows toast submission failed; check desktop notification settings")


def deliver_alerts(
    directory,
    *,
    send=send_toast,
    clock=lambda: datetime.now(UTC),
    retry_seconds=300,
    monotonic=time.monotonic,
):
    """At-least-once submission. Repeated submissions replace the same toast by tag.

    Submission means Windows accepted the toast, not that a human saw it. Unread
    alerts remain in the outbox until explicitly acknowledged by the operator.
    """
    directory = Path(directory).resolve()
    now = clock()
    if now.tzinfo is None or now.utcoffset() is None or retry_seconds < 1:
        raise ValueError("valid aware clock and positive retry interval required")
    database = directory / "operations.sqlite"
    if not database.exists():
        return {"status": "not_started", "submitted": 0, "failed": 0}
    with _process_lock(directory / "operations.lock") as owned:
        if not owned:
            return {"status": "busy", "submitted": 0, "failed": 0}
        with closing(sqlite3.connect(database.as_uri() + "?mode=rw", uri=True)) as conn:
            identity = conn.execute("SELECT observer_id FROM identity WHERE id=1").fetchone()
            if identity is None or identity[0] != load_manifest(directory)["observer_id"]:
                raise ValueError("notification outbox identity mismatch")
            conn.execute(
                "CREATE TABLE IF NOT EXISTS deliveries ("
                "alert_id INTEGER PRIMARY KEY, last_attempt_at TEXT NOT NULL, "
                "attempts INTEGER NOT NULL, submitted_at TEXT, error_type TEXT)"
            )
            rows = conn.execute(
                "SELECT a.id,a.kind,a.detail_json,d.last_attempt_at "
                "FROM alerts a LEFT JOIN deliveries d ON a.id=d.alert_id "
                "WHERE a.acknowledged_at IS NULL AND d.submitted_at IS NULL "
                "ORDER BY a.id LIMIT 10"
            ).fetchall()
            submitted, failed = 0, 0
            began = monotonic()
            for alert_id, kind, detail_json, previous in rows:
                if monotonic() - began >= 25:
                    break
                if (
                    previous
                    and (now - datetime.fromisoformat(previous)).total_seconds() < retry_seconds
                ):
                    continue
                try:
                    send(
                        {"id": alert_id, "kind": kind, "detail": json.loads(detail_json)},
                        identity[0],
                    )
                    submitted += 1
                    error, submitted_at = None, now.isoformat()
                except (OSError, subprocess.SubprocessError) as exc:
                    failed += 1
                    error, submitted_at = type(exc).__name__, None
                conn.execute(
                    "INSERT INTO deliveries VALUES (?,?,1,?,?) "
                    "ON CONFLICT(alert_id) DO UPDATE SET last_attempt_at=excluded.last_attempt_at, "
                    "attempts=deliveries.attempts+1, submitted_at=excluded.submitted_at, "
                    "error_type=excluded.error_type",
                    (alert_id, now.isoformat(), submitted_at, error),
                )
                conn.commit()
            return {"status": "error" if failed else "ok", "submitted": submitted, "failed": failed}
