"""Write a static, self-refreshing HTML page of the live journal. Read-only; no server.

The page shows the readiness gates, account and risk room, orders, the reconciled equity
history and, when given, the last scheduled cycle result. Every value is escaped; nothing
is fetched by the page itself.
"""

import argparse
import html
import json
import os
import tempfile
from datetime import UTC, datetime
from pathlib import Path

from trading.live_doctor import diagnose
from trading.live_report import report
from trading.private_order_recovery import PrivateOrderRecovery

STYLE = " ".join(
    [
        ":root { color-scheme: light dark; --fg: #1b1f24; --bg: #ffffff;",
        "--muted: #5b6470; --bad: #b42318; --ok: #067647; }",
        "@media (prefers-color-scheme: dark) { :root { --fg: #e6e8eb; --bg: #14171a;",
        "--muted: #9aa4af; --bad: #f97066; --ok: #47cd89; } }",
        "body { font-family: system-ui, sans-serif; margin: 16px; color: var(--fg);",
        "background: var(--bg); }",
        "table { border-collapse: collapse; width: 100%; margin-bottom: 16px;",
        "font-variant-numeric: tabular-nums; display: block; overflow-x: auto; }",
        "th, td { border-bottom: 1px solid color-mix(in srgb, var(--fg) 15%, transparent);",
        "padding: 4px 8px; text-align: left; white-space: nowrap; }",
        ".status { font-size: 1.4em; font-weight: 600; }",
        ".ok { color: var(--ok); } .bad { color: var(--bad); } .muted { color: var(--muted); }",
    ]
)


def _e(value):
    if isinstance(value, bool):
        value = "はい" if value else "いいえ"
    return html.escape("" if value is None else str(value), quote=True)


def _rows(items, columns):
    head = "".join(f"<th>{_e(title)}</th>" for title, _ in columns)
    body = "".join(
        "<tr>" + "".join(f"<td>{_e(item.get(key))}</td>" for _, key in columns) + "</tr>"
        for item in items
    )
    return f"<table><thead><tr>{head}</tr></thead><tbody>{body}</tbody></table>"


def _sparkline(points, width=640, height=120):
    values = [float(p["equity"]) for p in points]
    if len(values) < 2:
        return "<p>評価額の履歴が2件以上になると表示します。</p>"
    low, high = min(values), max(values)
    span = (high - low) or 1.0
    step = width / (len(values) - 1)
    coords = " ".join(
        f"{i * step:.1f},{height - (v - low) / span * (height - 10) - 5:.1f}"
        for i, v in enumerate(values)
    )
    return (
        f'<svg viewBox="0 0 {width} {height}" width="100%" height="{height}" role="img" '
        f'aria-label="評価額の推移"><polyline fill="none" stroke="currentColor" '
        f'stroke-width="2" points="{coords}"/></svg>'
        f"<p>最小 {_e(f'{low:,.2f}')} / 最大 {_e(f'{high:,.2f}')}</p>"
    )


TAIL_CHUNK = 64 * 1024
MAX_TAIL_BYTES = 4 * 1024 * 1024


def tail_lines(path, limit):
    """The last `limit` complete lines, reading backwards from the end in bounded chunks.

    Cost depends on the lines shown, not on the whole history's size.
    """
    with Path(path).open("rb") as handle:
        handle.seek(0, 2)
        position, buffer = handle.tell(), b""
        while position > 0 and buffer.count(b"\n") <= limit and len(buffer) < MAX_TAIL_BYTES:
            step = min(TAIL_CHUNK, position)
            position -= step
            handle.seek(position)
            buffer = handle.read(step) + buffer
    lines = buffer.split(b"\n")
    if position > 0:
        lines = lines[1:]  # Started mid-line.
    return [line for line in lines if line.strip()][-limit:]


def read_history(path, *, limit=24):
    """The newest cycle results from a JSON Lines history.

    A damaged line is shown as its own row rather than silently dropped.
    """
    if path is None or not Path(path).is_file():
        return []
    items = []
    for line in tail_lines(path, limit):
        try:
            item = json.loads(line.decode("utf-8"))
        except ValueError:
            item = None
        if not isinstance(item, dict):
            items.append(
                {"finished_at": None, "ok": None, "action": None, "reason": "damaged_history_line"}
            )
            continue
        if isinstance(item, dict):
            decision = item.get("decision") or {}
            items.append(
                {
                    "finished_at": item.get("finished_at"),
                    "ok": item.get("ok"),
                    "action": decision.get("action"),
                    "reason": item.get("reason") or decision.get("reason"),
                }
            )
    return list(reversed(items))


def render(doctor, profit, cycle=None, *, generated_at, history=()):
    account = profit["account"] or {}
    gates = [{"gate": name, **value} for name, value in doctor["gates"].items()]
    status = "送信可能" if doctor["send_ready"] else "送信不可"
    cycle_html = "<p>サイクル結果ファイルが指定されていません。</p>"
    if cycle is not None:
        decision = cycle.get("decision") or {}
        cycle_html = _rows(
            [
                {
                    "finished_at": cycle.get("finished_at"),
                    "ok": cycle.get("ok"),
                    "reason": cycle.get("reason") or decision.get("reason"),
                    "action": decision.get("action"),
                }
            ],
            [("終了", "finished_at"), ("成功", "ok"), ("理由", "reason"), ("判断", "action")],
        )
    account_table = _rows(
        [account],
        [
            ("時刻", "observed_at"),
            ("残高", "balance"),
            ("評価額", "equity"),
            ("余力", "available_margin"),
            ("損失上限までの残り", "loss_limit_remaining"),
            ("ピークからの下落", "drawdown_from_peak"),
        ],
    )
    totals_table = _rows(
        [profit["totals"]],
        [
            ("実現損益", "realized"),
            ("手数料", "fees"),
            ("スワップ", "settled_swap"),
            ("差し引き", "net"),
        ],
    )
    trades_table = _rows(
        [profit.get("closing_orders") or {}],
        [
            ("決済回数", "count"),
            ("勝ち", "wins"),
            ("負け", "losses"),
            ("勝率", "win_rate"),
            ("PF", "profit_factor"),
        ],
    )
    orders_table = _rows(
        profit["orders"],
        [
            ("注文", "client_id"),
            ("状態", "state"),
            ("売買", "side"),
            ("新規/決済", "effect"),
            ("数量", "units"),
            ("約定", "filled_units"),
            ("平均価格", "average_price"),
            ("実現損益", "realized"),
            ("スリッページ", "slippage"),
        ],
    )
    gates_table = _rows(gates, [("項目", "gate"), ("合格", "ok"), ("理由", "reason")])
    tone = "ok" if doctor["send_ready"] else "bad"
    approval = (
        f"許可の期限 {_e(doctor['approval_expires_at'])}"
        f"（残り {_e(doctor['approval_seconds_left'])} 秒）"
        f"・損失による新規停止 {_e(doctor['entry_halted'])}"
    )
    return "\n".join(
        [
            "<!doctype html>",
            '<html lang="ja"><head><meta charset="utf-8">',
            '<meta name="viewport" content="width=device-width, initial-scale=1">',
            '<meta http-equiv="refresh" content="60">',
            "<title>Trading Lab 実発注</title>",
            f"<style>{STYLE}</style></head><body>",
            "<h1>実発注の状態</h1>",
            f'<p class="status {tone}">{_e(status)}</p>',
            f'<p class="muted">作成 {_e(generated_at)} ・ 1分ごとに再読込 ・ '
            "業者側の状態は含みません</p>",
            "<h2>送信ゲート</h2>",
            gates_table,
            f"<p>{approval}</p>",
            "<h2>口座とリスクの余裕</h2>",
            account_table,
            "<h2>合計</h2>",
            totals_table,
            "<h2>決済注文ごとの成績</h2>",
            trades_table,
            "<h2>評価額の推移</h2>",
            _sparkline(profit["equity_history"]),
            "<h2>注文</h2>",
            orders_table,
            "<h2>直近のサイクル</h2>",
            cycle_html,
            "<h2>サイクルの履歴</h2>",
            _rows(
                list(history),
                [("終了", "finished_at"), ("成功", "ok"), ("判断", "action"), ("理由", "reason")],
            )
            if history
            else "<p>履歴ファイルが指定されていないか、まだ記録がありません。</p>",
            "</body></html>",
            "",
        ]
    )


def write_page(page, path):
    path = Path(path)
    handle, temporary = tempfile.mkstemp(dir=path.parent, prefix=".dashboard-", suffix=".tmp")
    try:
        with os.fdopen(handle, "wb") as output:
            output.write(page.encode("utf-8"))
        os.replace(temporary, path)
    except BaseException:
        Path(temporary).unlink(missing_ok=True)
        raise


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--directory", type=Path, required=True)
    parser.add_argument("--read-control-directory", type=Path, required=True)
    parser.add_argument("--scope", required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--cycle-result", type=Path)
    parser.add_argument("--history", type=Path)
    args = parser.parse_args(argv)
    try:
        journal = PrivateOrderRecovery(
            args.directory, args.read_control_directory, args.scope
        ).journal
        now = datetime.now(UTC)
        cycle = None
        if args.cycle_result is not None:
            try:
                cycle = json.loads(args.cycle_result.read_text(encoding="utf-8"))
            except (OSError, ValueError):
                cycle = {}
        profit = report(journal)
        doctor = diagnose(journal, now, cycle=cycle if args.cycle_result is not None else None)
        page = render(
            doctor,
            profit,
            cycle,
            generated_at=now.isoformat(),
            history=read_history(args.history),
        )
        write_page(page, args.output)
    except Exception as error:
        parser.exit(2, f"live_dashboard_failed: {type(error).__name__}\n")
    print(json.dumps({"output": str(args.output), "network_used": False}))


if __name__ == "__main__":
    main()
