# 送信直前までの運用サイクル

2026-10-04。[手順書](live-setup.md)の読取・提案・準備の各段階を1回のコマンドで行う`live_cycle`を
追加しました。注文のPOSTは含みません。送信は、出力された実行内容のSHA-256を確認してから
[`order_runtime submit`](order-runtime.md)で行います。

```powershell
uv run python -m trading.live_cycle --config configs/fx.toml --directory runs/live-orders --read-control-directory runs/account-read-control --scope <scope> --credential-reference <read_only_reference> --units 1000 --max-slippage 0.02 --quote-output runs/live-orders/quote.json --intent-output runs/live-orders/intent.json --ledger runs/ledger.sqlite --hypothesis H001 --prepare --confirm complete-account --confirm account-identity --confirm external-writers-paused --confirm complete-history
uv run python -m trading.order_runtime submit --directory runs/live-orders --read-control-directory runs/account-read-control --scope <scope> --client-id <client_id> --quote runs/live-orders/quote.json --expected-sha256 <checkpoint_sha256> --credential-reference <order_reference> --order-permission-confirmed
```

## 1回のサイクルで行うこと

0. 公開APIの[サービス状態](live-quote.md)を読みます。`MAINTENANCE`なら非公開GETを一切行わず、
   `hold`（`broker_maintenance`）で終わります。メンテナンス中は口座GETがすべて失敗し、毎時の失敗通知に
   なるためです。`CLOSE`（週末）はそのまま進み、提案の段階で市場閉鎖として見送ります。
1. 台帳で受付済み（`RECONCILING`・`WORKING`・`PARTIAL`・`CANCEL_PENDING`）の注文を、
   [受付済み注文のGET照合](live-order-sync.md)で順に照合します。
2. 公開tickerから[気配](live-quote.md)を1回取得し、`--quote-output`へ書き出します。
3. [口座証拠を更新](live-account.md)します。
4. [戦略の提案](live-signal.md)を作ります。提案があれば`--intent-output`へ書き出します。
   `--intent-output`のファイルはサイクルの最初に削除するため、提案のない回の後に古い注文意図は残りません。
5. `--prepare`を指定した場合だけ、提案を台帳に準備し、手順2の気配で実行内容を作って
   `checkpoint_sha256`・要求の本文・リスク評価を出力します。

手順5で実行内容の作成が拒否された場合（リスク検査、同期・監視の不健全など）は、同じ実行で準備した
未送信の注文を破棄（ABANDONED）し、`prepared_order_abandoned:<理由>`で失敗します。準備済みのまま残すと、
以後の提案が`unsettled_local_order`で見送られ続けるためです。

どの段階でも失敗すればそこで止まり、以降の段階を行いません。照合済みの注文や更新済みの
口座証拠は台帳に残ります。読取専用キーは各段階で必要な時だけ読みます。

確認項目は口座証拠の3項目と`complete-history`の4つです。足りない場合は、何も読まずに拒否します。
各段階が要求する確認を、運用者がサイクル単位でまとめて与える形です。

`--ledger`と`--hypothesis`で[実運用に昇格した凍結候補](promotion.md)を指定します。`--flatten`以外では
必須で、指定しないと`promoted_candidate_required`で何も読まずに拒否します。
`--units auto`で新規の数量を[口座証拠から決めます](live-signal.md)。
`--flatten`を付けると、手順4で戦略の代わりに全建玉の決済を提案します（[手仕舞い](live-signal.md)）。
建玉がある間は`--valuation-tolerance`で[評価額の許容幅](live-account.md)を指定します。

`--result-output`で結果をファイルに置き換え書込みし、`--notify`で提案・失敗をWindows通知します。
毎時の定期実行は[運用サイクルの定期実行](live-tasks.md)を使います。

## 送信までの時間

実行内容には手順2の気配が含まれます。送信時の口座リスク検査は気配と口座証拠の鮮度
（既定は各60秒）を要求するため、サイクルの出力から送信までに時間がかかると拒否されます。
気配だけが古い場合は、台帳の準備済み注文を残したまま`order_runtime context --fetch-quote <気配ファイル>`で
新しい気配を取って実行内容を作り直し、そのSHA-256と同じ気配ファイルで送信します。
口座証拠も古い場合は、[口座証拠の更新](live-account.md)（またはサイクルの再実行）から行います。
確認に時間をかける手動運用では、台帳の作成時に方針の`max_snapshot_age_seconds`と`max_quote_age_seconds`を
長め（例: 300秒）に設定できます。長くするほど、確認時と送信時の口座・相場の差が大きくなりうる点に注意してください。

同じ足・同じ向きの提案は顧客注文IDが同じになります。以前の注文が取消・失効で終わった後に
同じ足で別の価格保護の提案を準備しようとすると、`signal_client_id_already_used`で拒否します。

## 出力とエラー

`--prepare`で準備した場合は、確認後にそのまま使える`submit_command`も出力します。発注用キーの参照
（`<order_reference>`）だけを置き換えます。送信に使った気配は台帳の送信記録に残ります。

出力は照合した注文の状態、口座証拠の時刻・建玉数・有効注文数・損失による新規停止、
提案の内容、準備の有無と実行内容です。`orders_sent`は常に`false`です。
失敗時は`live_cycle_failed:`の後に、各段階の固定理由コード、それ以外は例外の型名だけを表示します。

## 検証

`tests/test_live_cycle.py`で、提案だけのサイクル、有効化後の準備と出力したSHA-256による1回だけの送信、
次のサイクルでの受付済み注文の`FILLED`化・建玉ありの口座証拠・保有済みでの見送り、
確認不足でキーを読まないこと、CLIの失敗表示を検証します。`tests/test_live_flow.py`は
同じ流れを各部品の呼出しで検証します。合成のGET・POST・気配・足だけを使い、実通信は行いません。
追加7試験（サイクル6・結合1）が合格しました。Ruffの検査・整形確認、差分チェックも合格しました。
