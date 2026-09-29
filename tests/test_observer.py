import io
import json
import sqlite3
import threading
from datetime import timedelta
from types import SimpleNamespace
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

import pandas as pd
import pytest

from trading import paper_dashboard
from trading.gmo import Quote, rollover_time, trading_date
from trading.observer import collect_swaps, initialize, observe
from trading.paper_dashboard import make_server, snapshot


@pytest.mark.parametrize(
    "error,reason",
    [
        (HTTPError("http://localhost", 503, "unavailable", {}, None), "HTTP 503"),
        (TimeoutError("slow response"), "timed out"),
        (URLError(TimeoutError("slow connection")), "status is unknown"),
    ],
)
def test_dashboard_unhealthy_endpoint_does_not_start_another_server(
    bars, cfg, tmp_path, monkeypatch, error, reason
):
    directory, _, _, _ = setup(tmp_path, bars, cfg)

    def unavailable(*args, **kwargs):
        raise error

    def forbidden(*args, **kwargs):
        pytest.fail("must not spawn a second dashboard")

    monkeypatch.setattr(paper_dashboard, "urlopen", unavailable)
    monkeypatch.setattr(paper_dashboard.subprocess, "Popen", forbidden)
    with pytest.raises(RuntimeError, match=reason):
        paper_dashboard.ensure_server(directory, 8765)


@pytest.mark.parametrize("already_running", [False, True])
def test_dashboard_reuses_existing_server_or_starts_after_connection_refused(
    bars, cfg, tmp_path, monkeypatch, already_running
):
    directory, _, _, _ = setup(tmp_path, bars, cfg)
    observer_id = paper_dashboard.load_manifest(directory)["observer_id"]
    calls, starts = [], []

    def response(*args, **kwargs):
        calls.append(True)
        if not already_running and len(calls) == 1:
            raise URLError(ConnectionRefusedError("no listener"))
        return io.BytesIO(json.dumps({"observer_id": observer_id}).encode())

    def start(*args, **kwargs):
        starts.append(True)
        return SimpleNamespace(pid=123, poll=lambda: None)

    monkeypatch.setattr(paper_dashboard, "urlopen", response)
    monkeypatch.setattr(paper_dashboard.subprocess, "Popen", start)
    result = paper_dashboard.ensure_server(directory, 8765)
    assert len(starts) == (0 if already_running else 1)
    assert result == {
        "url": "http://127.0.0.1:8765",
        **({"already_running": True} if already_running else {"pid": 123}),
    }


class FakeAPI:
    def __init__(self, bars, now, status="OPEN"):
        self.bars, self.now, self.status = bars, now, status
        self.price = 153

    def validate_rules(self, cfg):
        pass

    def quote(self, symbol):
        return Quote(
            symbol=symbol,
            bid=self.price,
            ask=self.price + 0.02,
            status=self.status,
            timestamp=self.now,
        )

    def candle_days(self, cfg, start, end, now):
        yield start, self.bars if start == trading_date(self.now) else pd.DataFrame()


class FakeCalendar:
    def history(self, cfg, start, end, now):
        return pd.DataFrame(
            [
                {
                    "timestamp": rollover_time(day.date()),
                    "symbol": cfg.symbol,
                    "long_jpy_per_10k": 100,
                    "short_jpy_per_10k": -100,
                    "days": 1,
                }
                for day in pd.date_range(start, end, freq="D")
            ]
        )


def setup(tmp_path, bars, cfg):
    now = (bars.timestamp.iloc[2] + pd.Timedelta(hours=1, seconds=5)).to_pydatetime()
    directory = tmp_path / "observer"
    initialize(directory, cfg, now)
    return directory, now, FakeAPI(bars, now), FakeCalendar()


def test_observation_separate_account_restart_and_dashboard(bars, cfg, tmp_path):
    directory, now, api, calendar = setup(tmp_path, bars, cfg)
    first = observe(directory, api, calendar, lambda: now)
    assert first["status"] == "ok"
    assert first["action"] == "buy"
    again = observe(directory, api, calendar, lambda: now)
    assert again["action"] == "duplicate_quote"
    api.now += timedelta(minutes=15)
    later = observe(directory, api, calendar, lambda: api.now)
    assert later["action"] == "same_signal"
    report = snapshot(directory, api.now)
    assert report["status"] == "ok"
    assert report["stats"]["fills"] == 1
    assert report["stats"]["observations"] == 2
    assert report["stats"]["closed_trades"] == 0
    assert report["attempt_count"] == 3
    assert report["stats"]["traded_notional_jpy"] == pytest.approx(153030)
    assert report["series"][-1]["notional"] == pytest.approx(153000)
    assert list((directory / "inputs").glob("*.parquet"))


def test_observation_skips_missed_bars_instead_of_replaying(bars, cfg, tmp_path):
    directory, now, api, calendar = setup(tmp_path, bars, cfg)
    observe(directory, api, calendar, lambda: now)
    api.now = (bars.timestamp.iloc[-1] + pd.Timedelta(hours=1, seconds=5)).to_pydatetime()
    api.price = 148
    result = observe(directory, api, calendar, lambda: api.now)
    assert result["action"] == "sell"
    report = snapshot(directory, api.now)
    assert report["stats"]["observations"] == 2
    assert report["stats"]["fills"] == 2
    assert report["stats"]["closed_trades"] == 1
    buy, sell = report["fills"]
    expected = 1000 * (sell["price"] - buy["price"]) - buy["commission"] - sell["commission"]
    assert report["stats"]["realized_net_ex_swap_jpy"] == pytest.approx(expected)


def test_code_change_is_logged_without_trading(bars, cfg, tmp_path):
    directory, now, api, calendar = setup(tmp_path, bars, cfg)
    path = directory / "manifest.json"
    manifest = json.loads(path.read_text(encoding="utf-8"))
    manifest["core_sha256"]["paper.py"] = "changed"
    path.write_text(json.dumps(manifest), encoding="utf-8")
    result = observe(directory, api, calendar, lambda: now)
    assert result["status"] == "error"
    assert "spec/code changed" in result["error"]
    assert not (directory / "paper.sqlite").exists()
    assert snapshot(directory, now)["error_count"] == 1


def test_closed_market_and_stale_status(bars, cfg, tmp_path):
    directory, now, api, calendar = setup(tmp_path, bars, cfg)
    api.status = "CLOSE"
    assert observe(directory, api, calendar, lambda: now)["status"] == "market_closed"
    assert not (directory / "paper.sqlite").exists()
    assert snapshot(directory, now + timedelta(minutes=15))["status"] == "market_closed"
    assert snapshot(directory, now + timedelta(minutes=16))["status"] == "stale"


def test_errors_do_not_overwrite_last_equity(bars, cfg, tmp_path):
    directory, now, api, calendar = setup(tmp_path, bars, cfg)
    observe(directory, api, calendar, lambda: now)
    old = snapshot(directory, now)["state"]["equity"]
    result = observe(directory, api, calendar, lambda: now + timedelta(minutes=2))
    assert result["status"] == "error"
    report = snapshot(directory, now + timedelta(minutes=2))
    assert report["state"]["equity"] == old
    assert report["stats"]["observations"] == 1
    assert report["error_count"] == 1


def test_concurrent_attempt_refuses_duplicate_execution(bars, cfg, tmp_path):
    directory, now, api, calendar = setup(tmp_path, bars, cfg)
    conn = sqlite3.connect(directory / "observations.sqlite")
    conn.execute("BEGIN IMMEDIATE")
    try:
        assert observe(directory, api, calendar, lambda: now)["status"] == "busy"
        assert not (directory / "paper.sqlite").exists()
    finally:
        conn.close()


def test_swap_collection_appends_each_rollover_once(bars, cfg, tmp_path):
    directory, now, api, calendar = setup(tmp_path, bars, cfg)
    manifest = json.loads((directory / "manifest.json").read_text(encoding="utf-8"))
    first = collect_swaps(calendar, cfg, directory, manifest, now)
    second = collect_swaps(calendar, cfg, directory, manifest, now + timedelta(days=1))
    again = collect_swaps(calendar, cfg, directory, manifest, now + timedelta(days=1))
    assert len(second) == len(first) + 1
    pd.testing.assert_frame_equal(second, again, check_dtype=False)
    pd.testing.assert_frame_equal(second.iloc[: len(first)], first, check_dtype=False)


def test_init_refuses_to_overwrite(bars, cfg, tmp_path):
    directory, now, api, calendar = setup(tmp_path, bars, cfg)
    with pytest.raises(FileExistsError):
        initialize(directory, cfg, now)


def test_readonly_loopback_dashboard(bars, cfg, tmp_path):
    directory, now, api, calendar = setup(tmp_path, bars, cfg)
    server = make_server(directory, 0)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    url = f"http://127.0.0.1:{server.server_port}"
    try:
        with urlopen(url + "/api/snapshot") as response:
            assert json.load(response)["state"] is None
        with urlopen(url) as response:
            assert "観測帳" in response.read().decode()
            assert response.headers["Cache-Control"] == "no-store"
        for request, code in [
            (Request(url, headers={"Host": "attacker.test"}), 403),
            (Request(url + "/manifest.json"), 404),
            (Request(url + "/api/snapshot", method="POST"), 501),
        ]:
            with pytest.raises(HTTPError) as exc:
                urlopen(request)
            assert exc.value.code == code
    finally:
        server.shutdown()
        server.server_close()
        thread.join()
