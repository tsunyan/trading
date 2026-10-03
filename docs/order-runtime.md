# 発注用キーの保管と確認済み送信

2026-10-04。発注用APIキーを読取専用キーと別のWindows資格情報の名前空間に保存し、
[同期・独立監視を登録した実発注台帳](live-operations.md)へ紐付けます。送信は、事前に表示した
実行内容のSHA-256を明示した場合だけ行い、キーの読込前後とPOST枠の取得後に同じ内容かを再検査します。
気配は[公開tickerから取得](live-quote.md)できます。実キー・実口座での受入、完全な口座証拠の供給は別の残工程です。

## 発注用キーを保存する

読取専用キーは`TradingLab/GMOFX/ReadOnly/v1/`、発注用キーは`TradingLab/GMOFX/Orders/v1/`に保存します。
発注用の保管庫は読取専用の名前空間を受け付けず、読取専用の保管庫は発注用の記録を読みません。
保存できるのは、[同期・監視を登録した](live-operations.md)専用の実発注台帳だけです。

```powershell
uv run python -m trading.order_credentials binding --directory runs/live-orders --read-control-directory runs/private-reads --scope <scope>
uv run python -m trading.order_credentials save --directory runs/live-orders --read-control-directory runs/private-reads --scope <scope> --order-permission-confirmed
```

`save`は対話端末でだけ動き、キーとシークレットを`getpass`で入力します。保存する内容は、発注用の
宣言、参照ID、台帳・POST制御・GET制御の識別子、スコープ、口座IDと登録内容・保存場所のSHA-256です。
口座IDの平文、残高、注文本文はキーと一緒に保存しません。出力は参照IDと紐付けだけで、キーは表示しません。
`--order-permission-confirmed`は利用者の宣言です。業者側の権限や口座本人性の証明にはしません。

キーの差替えは新しい参照IDで保存します。保存は停止・許可・口座証拠・注文を変更しません。
別の台帳・GET制御・POST制御・同期・監視への付け替えは、読込時の紐付け不一致で拒否します。

## 実行内容を確認してから送信する

まず`context`で、送信する注文の本文・リスク評価・台帳とPOST制御の状態をまとめたSHA-256を表示します。
新規・決済には、口座証拠と同じかより新しい気配のJSON（`AccountQuote`、4096バイトまで）が必要です。
`context`はネットワーク・資格情報・claimを使いません。

```powershell
uv run python -m trading.order_runtime context --directory runs/live-orders --read-control-directory runs/private-reads --scope <scope> --client-id <client_id> --quote quote.json
uv run python -m trading.order_runtime submit --directory runs/live-orders --read-control-directory runs/private-reads --scope <scope> --client-id <client_id> --quote quote.json --expected-sha256 <checkpoint_sha256> --credential-reference <reference> --order-permission-confirmed
```

取消は`cancel-context`と`cancel`を使います。気配は受け付けず、[対象限定の取消許可](live-cancel.md)を
使う場合は`--authorization-sha256`を両方に渡します。

送信では次の順に検査します。どこかで内容が変わると、それ以降へ進みません。

1. 表示時と同じ実行内容か（キーを読む前）
2. キーの記録の用途・参照ID・紐付けが現在の台帳と一致するか
3. キー読込後にも同じ実行内容か（読込中の停止・口座更新を拒否）
4. 送信クライアントの準備時と、POST枠の取得後・SUBMITTING保存前に同じ内容か

実行内容には、注文本文とSHA-256、リスク評価、口座ID、台帳の設定・実装・注文行・リスクゲートの
ハッシュ、最後のイベントID、POST制御のrevision、停止状態、気配、取消許可のSHA-256を含めます。
別の注文の準備、口座証拠や気配の更新、停止、同期・監視の不健全化、コード変更のいずれでも値が変わります。

キーを読む前の拒否では資格情報ストアに触れず、HTTPもclaimも使いません。POST枠の取得後の拒否では
POST枠だけを正常完了し、注文をPREPAREDに残します。SUBMITTING保存後の失敗は従来どおり結果不明・停止とし、
このコマンドから再送しません。受付後の出力は`reconciliation_required: true`で、口座の完全性を主張しません。
受付後は[読取同期](live-order-catalog.md)のGET照合で約定と状態を確認します。

## 残る制約

- 気配JSONは[発注確認用の気配取得](live-quote.md)で公開tickerから作れます。鮮度は`context`と送信時の
  口座リスク検査で判定し、取得時には判定しません。
- 確認後のプロセス終了、業者側の状態変更、別PCからの操作とHTTP送信は原子的ではありません。
- 保管庫の`backend`を差し替える呼出しは信頼できるテスト用です。実運用のCLIは常にWindowsストアを使います。

## 検証

`tests/test_order_runtime.py`で、名前空間の分離、保存の宣言と登録済み台帳の要求、実行内容の安定性、
キー読込前後とPOST待機後の変化の拒否、改変・欠落したキー記録、取消許可付きの取消、CLIの拒否を検証します。
合成した資格情報ストアと模擬HTTPだけを使い、実キー・実通信・Windows資格情報APIは使いません。
追加25試験を含む全2257テストが456.34秒で合格しました。Ruffの検査・整形確認、差分チェックも合格しました。
