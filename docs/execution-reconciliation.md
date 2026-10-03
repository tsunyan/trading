# 約定通知と既知注文のREST照合

2026-10-01。`AccountSyncMonitor.resync` と `JournaledEventCapture.resync` に、
既知注文を再取得する任意の `collect_orders` コールバックを追加しました。
約定通知の数量・価格・費用とRESTの観測値を比較し、合致した約定の金額内訳を出します。
口座・APIキーなしの合成デモで利用できます。

## 使用方法

```python
# intents_by_order_id は、呼出側が保持している既知注文IDと元の注文意図。
# 通知の内容から注文意図を自動生成して本人性の証拠にするものではありません。
result = monitor.resync(
    session,
    reader.collect_account,
    collect_orders=lambda order_ids: tuple(
        reader.collect_order(intents_by_order_id[order_id], order_id) for order_id in order_ids
    ),
)
```

コールバックには、現在の接続世代で受け取った約定通知の注文IDを、重複を除いて昇順で渡します。
口座取得後に呼び出し、注文ごとに既存Readerの `/orders` と `/executions` の2回取得を使います。
注文意図が不明なら呼出側の例外により収集が失敗します。未取得の注文・約定は照合済みにしません。
通知がなければコールバックは呼びません。省略した場合は従来どおり約定を未検証として残します。

## 照合内容

- 親注文ID、注文ID、顧客注文ID、銘柄、方向、OPEN/CLOSE、注文種別、注文総量、指値。
- 約定ID、建玉ID、数量、約定価格、約定日時、手数料、決済損益、決済スワップ。
- 通知の受渡金額が `決済損益 − 正規化済み手数料 + 決済スワップ` と一致すること。
  RESTの受渡金額がある場合はReaderも同じ式を検査します。
- 通知の累積約定数量が、その約定数量以上、RESTにある注文全体の約定数量以下であること。
  後続の約定がRESTに現れることは許容し、通知にない約定IDは `rest_only_execution_ids` に残します。

同一約定の再配信は一度だけ集計します。同じIDの内容が変われば接続を無効化し、
別注文での同一約定ID、重複した注文レポート、要求外の注文レポートも拒否します。
照合結果には元のREST観測（要求・時刻・応答ハッシュ）を含めます。
未知の追加フィールドの同一性や注文作成時刻の一致は、この正規化項目の照合には含みません。

`execution_reconciliation` の `matched_loss_gain`、`matched_fee_debit`、
`matched_settled_swap`、`matched_cash_amount` は、**現在の接続世代の一致した通知集合**の合計です。
口座残高に加算する差分ではありません。部分約定・決済を含め、繰り返し照合しても同じ集合は同じ合計になります。
不一致があれば再取得要求が残り、一致した部分だけの集計と未検証IDを診断結果に出します。

## 競合・期限・永続化

口座と約定の取得はモニター／Captureのロックを保持せず行います。
取得中の通知、切断、再接続、期限超過、古い応答、要求と異なる応答は採用しません。
口座取得中に変化した場合は、後続の注文取得に進みません。
全取得を既定30秒以内で検査します。通信タイムアウトは注入するTransport側の責任です。
注文は最大1,000件、純粋照合関数の通知・REST約定は各最大100,000件で打ち切ります。
通常のモニターは従来どおり1接続世代1,000フレームが既定上限です。

後の通知・期限切れ・再照合開始により、過去の照合済み表示は取り消されます。
Captureのジャーナルには通知を保存しますが、照合済み状態やREST結果は永続化しません。
再起動や再生後は新たなREST取得が必要です。旧版は新しい再生入力の `order_reads` を読めません。

## 合成デモ

```powershell
uv run python -m trading.account_sync_lab demo --executions --directory runs/execution-reconciliation-demo
uv run python -m trading.account_sync_lab replay --input runs/execution-reconciliation-demo/transcript.json
```

接続 → 部分約定通知 → REST一致 → 同じ通知の再配信 → 合計が変わらないことを確認 →
REST手数料の不一致 → 修正されたRESTとの一致、の7ステップです。
入力と結果をJSON保存し、同じ入力から同じ結果を再現します。既存出力は上書きしません。

## 残る制約

`complete=false`、`live_enabled=false`、`accounting_applied=false` を維持します。
`execution_events_not_reconciled` が解消しても、`execution_accounting_not_applied` と
履歴完全性・口座本人性・原子的な同期の未確認条件は残ります。
残高変化は引き続き `balance_change_unverified` として扱います。
入出金・外部操作・失った履歴・初期残高との会計照合と実口座受入検証が必要です。
[WebSocket受信とトークン管理](private-stream.md)は追加済みです。
[個別約定の永続現金会計](execution-cash-book.md)も2026-10-02に追加しました。
明示入力した個別約定を別DBへ一度だけ計上します。
[同期モニターからの現金計上](execution-cash-sync.md)では、捕捉アダプターへ台帳を明示して
現在有効な個別証拠を渡せます。照合結果の合計値だけでは計上せず、全口座会計の完了や
会計反映済み状態への変換もしません。

フィールドは[GMO FXの約定REST仕様](https://api.coin.z.com/fxdocs/#executions)と
[約定通知仕様](https://api.coin.z.com/fxdocs/#ws-execution-events)を2026-10-01に確認しました。
テストは合成応答によるもので、実APIとの疎通や履歴の完全性の証明ではありません。
