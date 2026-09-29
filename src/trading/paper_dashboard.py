"""Read-only, loopback-only paper observation dashboard. No trading HTTP endpoints."""

import argparse
import json
import os
import sqlite3
import subprocess
import sys
import time
from contextlib import closing
from datetime import UTC, datetime, timedelta
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import urlsplit
from urllib.request import urlopen

import pandas as pd

from trading.backtest import completed_trades
from trading.observer import load_manifest

DEFAULT_PORT = 10010
# The external scheduler runs `observer step` every 5 minutes. The account manifest keeps its
# original 15-minute value because observer.py is hash-locked into the running account.
OBSERVATION_INTERVAL_MINUTES = 5
STALE_AFTER_MINUTES = 3 * OBSERVATION_INTERVAL_MINUTES


def read_events(path: Path) -> list[dict]:
    if not path.exists():
        return []
    with closing(sqlite3.connect(path.resolve().as_uri() + "?mode=ro", uri=True)) as conn:
        return [
            json.loads(row[0])
            for row in conn.execute("SELECT payload_json FROM events ORDER BY id")
        ]


def snapshot(directory: Path, now: datetime | None = None) -> dict:
    now = now or datetime.now(UTC)
    manifest = load_manifest(directory)
    cfg = manifest["config"]
    events = read_events(directory / "paper.sqlite")
    with closing(
        sqlite3.connect(
            (directory / "observations.sqlite").resolve().as_uri() + "?mode=ro", uri=True
        )
    ) as conn:
        attempts = [
            {
                "started_at": row[0],
                "finished_at": row[1],
                "status": row[2],
                "error": json.loads(row[3]).get("error"),
            }
            for row in conn.execute(
                "SELECT started_at,finished_at,status,detail_json FROM attempts "
                "ORDER BY id DESC LIMIT 40"
            )
        ]
        attempt_count = conn.execute("SELECT COUNT(*) FROM attempts").fetchone()[0]
        error_count = conn.execute("SELECT COUNT(*) FROM attempts WHERE status='error'").fetchone()[
            0
        ]
    series, fills, daily = [], [], {}
    for event in events:
        state = event["state"]
        timestamp = event["observed_at"]
        series.append(
            {
                "time": timestamp,
                "equity": state["equity"],
                "notional": state["gross_notional"],
                "units": state["units"],
                "drawdown_pct": (1 - state["equity"] / state["peak"]) * 100,
                "swap": state.get("swap_pnl", 0),
                "commission": state["commission"],
            }
        )
        if event["filled_units"]:
            amount = abs(event["filled_units"]) * event["fill_price"]
            fills.append(
                {
                    "timestamp": timestamp,
                    "action": event["action"],
                    "filled_units": event["filled_units"],
                    "price": event["fill_price"],
                    "commission": event["commission_jpy"],
                    "notional": amount,
                }
            )
            day = pd.Timestamp(timestamp).tz_convert("Asia/Tokyo").date().isoformat()
            bucket = daily.setdefault(day, {"day": day, "fills": 0, "notional": 0.0})
            bucket["fills"] += 1
            bucket["notional"] += amount
    trades = completed_trades(pd.DataFrame(fills)) if fills else pd.DataFrame()
    latest = events[-1] if events else None
    state = latest["state"] if latest else None
    last_attempt = attempts[0] if attempts else None
    last_time = (
        datetime.fromisoformat(last_attempt["finished_at"])
        if last_attempt and last_attempt["finished_at"]
        else None
    )
    stale = last_time is None or (now - last_time).total_seconds() > STALE_AFTER_MINUTES * 60
    days = max((now - datetime.fromisoformat(manifest["created_at"])).total_seconds() / 86400, 0)
    last_mark_time = datetime.fromisoformat(latest["observed_at"]) if latest else None
    return {
        "observer_id": manifest["observer_id"],
        "generated_at": now.isoformat(),
        "created_at": manifest["created_at"],
        "config": cfg,
        "observation_interval_minutes": OBSERVATION_INTERVAL_MINUTES,
        "stale_after_minutes": STALE_AFTER_MINUTES,
        "status": "stale"
        if stale
        else ("halted" if state and state["halted"] else last_attempt["status"]),
        "latest_attempt": last_attempt,
        "attempts": attempts,
        "attempt_count": attempt_count,
        "error_count": error_count,
        "state": state,
        "last_observed_at": latest["observed_at"] if latest else None,
        "mark_age_minutes": (now - last_mark_time).total_seconds() / 60 if last_mark_time else None,
        "quote": latest["quote"] if latest else None,
        "series": series,
        "fills": fills,
        "daily": list(daily.values()),
        "stats": {
            "observation_days": days,
            "observations": len(events),
            "fills": len(fills),
            "closed_trades": len(trades),
            "realized_net_ex_swap_jpy": float(trades.net_pnl_jpy.sum()) if len(trades) else 0.0,
            "traded_notional_jpy": sum(fill["notional"] for fill in fills),
            "fills_7d": sum(
                datetime.fromisoformat(f["timestamp"]) >= now - timedelta(days=7) for f in fills
            ),
            "fills_30d": sum(
                datetime.fromisoformat(f["timestamp"]) >= now - timedelta(days=30) for f in fills
            ),
            "max_drawdown_pct": max((row["drawdown_pct"] for row in series), default=0),
        },
        "limitations": manifest["limitations"],
    }


def make_server(directory: Path, port: int) -> ThreadingHTTPServer:
    directory = directory.resolve()
    assets = Path(__file__).with_name("dashboard.html")

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            if self.headers.get("Host") not in {
                f"127.0.0.1:{self.server.server_port}",
                f"localhost:{self.server.server_port}",
            }:
                self.send_error(403)
                return
            route = urlsplit(self.path).path
            try:
                if route == "/":
                    body, content_type = assets.read_bytes(), "text/html; charset=utf-8"
                elif route == "/api/snapshot":
                    body = json.dumps(
                        snapshot(directory), ensure_ascii=False, allow_nan=False
                    ).encode()
                    content_type = "application/json; charset=utf-8"
                else:
                    self.send_error(404)
                    return
            except (OSError, ValueError, sqlite3.Error, KeyError):
                self.send_error(503, "Observation data unavailable")
                return
            self.send_response(200)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Cache-Control", "no-store")
            self.send_header("X-Content-Type-Options", "nosniff")
            self.send_header(
                "Content-Security-Policy",
                "default-src 'self'; script-src 'unsafe-inline'; style-src 'unsafe-inline'; "
                "connect-src 'self'; frame-ancestors 'none'",
            )
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, format, *args):
            pass

    return ThreadingHTTPServer(("127.0.0.1", port), Handler)


def ensure_server(directory: Path, port: int) -> dict:
    url = f"http://127.0.0.1:{port}"
    expected = load_manifest(directory)["observer_id"]

    def running():
        try:
            with urlopen(url + "/api/snapshot", timeout=2) as response:
                current = json.load(response)
        except HTTPError as exc:
            raise RuntimeError(
                f"dashboard endpoint responded with HTTP {exc.code}; retry when healthy"
            ) from exc
        except TimeoutError as exc:
            raise RuntimeError("dashboard endpoint timed out; server status is unknown") from exc
        except URLError as exc:
            if isinstance(exc.reason, ConnectionRefusedError):
                return False
            raise RuntimeError("dashboard endpoint unavailable; server status is unknown") from exc
        if current.get("observer_id") != expected:
            raise ValueError("port belongs to another observation account")
        return True

    if running():
        return {"url": url, "already_running": True}
    with (directory / "dashboard.log").open("ab") as log:
        process = subprocess.Popen(
            [
                sys.executable,
                "-m",
                "trading.paper_dashboard",
                "serve",
                "--directory",
                str(directory.resolve()),
                "--port",
                str(port),
            ],
            stdin=subprocess.DEVNULL,
            stdout=log,
            stderr=log,
            creationflags=subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0,
            start_new_session=os.name != "nt",
        )
    (directory / "dashboard.pid").write_text(str(process.pid), encoding="ascii")
    for _ in range(30):
        if process.poll() is not None:
            raise RuntimeError("dashboard exited; inspect dashboard.log (port may be occupied)")
        if running():
            return {"url": url, "pid": process.pid}
        time.sleep(0.1)
    raise RuntimeError("dashboard did not become ready; inspect dashboard.log")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=["serve", "ensure", "snapshot"])
    parser.add_argument("--directory", type=Path, required=True)
    parser.add_argument("--port", type=int, default=DEFAULT_PORT)
    args = parser.parse_args()
    if args.command == "serve":
        make_server(args.directory, args.port).serve_forever()
    elif args.command == "ensure":
        print(json.dumps(ensure_server(args.directory, args.port)))
    else:
        print(json.dumps(snapshot(args.directory), ensure_ascii=True, indent=2))


if __name__ == "__main__":
    main()
