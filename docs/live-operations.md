# 発注直前の同期・独立監視検査

2026-10-04。専用の実発注台帳へ元のPrivate同期環境と独立監視を登録し、登録後の通常送信に
稼働状態の検査を必須にします。準備した注文・POST枠の待機後・HTTP直前で同じ検査を行います。
発注用キーの読込と送信CLIは[発注用キーの保管と確認済み送信](order-runtime.md)で接続しました。
完全な口座証拠の供給、実口座受入は別の残工程です。

## 最初の有効化より前に登録する

同じGET・POST制御に専用の実発注台帳とPrivate同期環境を紐付け、独立監視DBを初期化します。
監視の`live_binding`が元の台帳を指すことを確認して、まだDISABLEDの台帳へ登録します。
台帳を作る前に監視DBを初期化した場合は、先に`private_operations check`で元の台帳を登録します。

```powershell
uv run python -m trading.private_operations init --directory runs/private-sync
uv run python -m trading.private_operations status --directory runs/private-sync
uv run python -m trading.private_order_operations context --directory runs/live-orders --read-control-directory runs/private-reads --scope <scope>
uv run python -m trading.private_order_operations bind --directory runs/live-orders --read-control-directory runs/private-reads --scope <scope> --sync-directory runs/private-sync --expected-revision <revision> --expected-plan-sha256 <plan_sha256> --expected-monitor-instance <monitor_instance> --max-sync-age-seconds 120 --max-watchdog-age-seconds 120 --confirm-operations
```

同期の正規化した保存場所・識別子・固定設定のハッシュ、監視の識別子、成功照合と監視の鮮度上限を
台帳に保存し、`LIVE_OPERATIONS_BOUND`イベントと照合します。設定SHA-256に登録内容を含めるため、
登録後のcontextで取得した値を使って、別途の受入と最初の有効化を行います。
登録で残高・口座証拠・発注許可・停止を変更しません。

既定の鮮度上限は両方120秒です。1秒から独立監視の停滞閾値まで指定でき、登録後は変更できません。
別の同期・監視への変更、登録の除去、再登録を拒否します。登録イベントの欠落・改変・重複も拒否します。
すでに有効化した台帳や停止した台帳の登録・移行は、このコマンドの対象にしていません。
既存の無登録台帳を読取りで自動登録・再初期化しません。実運用の新規台帳では最初の有効化前に登録します。

## 同期と監視の開始後

元の同期環境を明示的に開始し、独立した`private_operations watchdog`を定期実行します。
監視は新しいRUNNING世代を初めて観測した時点の成功回数を保存します。その後に照合が一度以上
成功し、`watchdog`がその成功を確認するまでは送信を拒否します。通常の`check`はこの保護記録を更新しません。
起動時と同期再開時には、照合成功と次の監視周期まで待つ必要があります。

監視出力の`watchdog_checkpoint`には対象台帳の識別子、保護確認時刻、同期世代、初回観測時の成功回数、確認した
成功回数とcontrol revisionを保存します。口座残高・注文本文・資格情報は保存しません。
通知保存の容量不足や監視保存失敗では新しい保護記録を確定せず、過去の時刻を更新しません。

通常の新規・建玉指定決済・通常取消は、元の永久紐付け、同期のRUNNINGと実OS所有者、
同じ世代の保護記録、最後の成功照合時刻と監視時刻、GET・現金台帳・受信記録・監視条件を検査します。
以前の世代の成功や診断だけの新しい観測で、送信可能にはしません。
元のmanifest・DB・監視ロックが欠落・破損・差替えられていれば、再作成せず送信を拒否します。
監視・同期の診断を口座完全性へ変換せず、従来の許可・口座証拠・気配・リスク検査も要求します。

許可失効と損失による新規停止は全体の監視障害にはしません。許可失効は従来の有効化検査で拒否し、
損失停止後の条件を満たす決済は、同期・監視が健全なら既存のリスク検査に従って送信できます。
同期・監視が不健全な場合も、[対象限定の取消許可](live-cancel.md)は既存の独立した確認で利用できます。
対象・履歴末尾・期限・コード・GET・POST所有権・消費済み取消の検査を維持し、新規や決済を許可しません。

送信準備での拒否はHTTPもclaimも消費しません。POST枠の待機後、SUBMITTING保存前の既知の拒否では
POST枠だけを正常完了し、注文をPREPAREDに残します。SUBMITTING保存後の検査失敗では、従来どおり
結果不明と停止・未解決claimを残し、再送しません。元のGET証拠と既存の復旧手続きを使います。

検査が終わった直後のプロセス終了・外部操作や業者側の状態変更とHTTP送信は原子的ではありません。
短い検査後の窓と別PCからの操作は残り、独立監視の停止も継続します。

## 検証

`tests/test_live_operations.py`で、有効な同期・監視からの署名付き模擬POST、プロセス終了、
同期・監視の期限切れ、新しい世代、診断と保護確認の区別、保存物の欠落・登録履歴の改変、
POST待機後とHTTP直前の中断、損失停止後の決済と対象限定取消を検証します。
実キー・実HTTP・Windowsタスクの登録や新しい実通知は使いません。
追加47試験を含む全2232テストが414.39秒で合格しました。Ruffの検査・整形確認、差分チェックも合格しました。
