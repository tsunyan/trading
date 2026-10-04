# 実発注の状態ページ

2026-10-04。実発注台帳の状態を1枚の静的HTMLに書き出すCLIを追加しました。サーバーは起動せず、
資格情報・通信・台帳の変更もありません。ページは1分ごとに自分自身を再読込します。

```powershell
uv run python -m trading.live_dashboard --directory runs/live-orders --read-control-directory runs/account-read-control --scope <scope> --output runs/live-orders/live.html --cycle-result runs/live-orders/cycle.json
```

表示する内容は、[送信前の状態診断](live-doctor.md)の各ゲートと送信可否、許可の期限、
[損益レポート](live-report.md)の口座・リスクの余裕・合計・注文、照合済みの評価額の推移（折れ線）、
直近の[運用サイクル](live-cycle.md)の結果です。文字列はすべてHTMLとしてエスケープし、
ページは外部の資源を読み込みません。明暗どちらの表示設定でも読めます。

定期実行では`live_cycle --dashboard-output`（`install-live-cycle.ps1 -DashboardOutput`）を指定すると、
毎時のサイクルの後にページを更新します。ページの作成に失敗してもサイクルの結果は変わりません。

業者側の状態（業者画面の注文・建玉）は表示しません。照合済みの台帳の記録だけを表示します。

## 検証

`tests/test_live_dashboard.py`で、送信ゲート・注文の表示とサイクル結果の文字列のエスケープ、口座証拠や
履歴がない台帳の表示、折れ線の作成、置き換え書込みを検証します。運用サイクルからのページ作成は
`tests/test_live_cycle.py`で検証します。
