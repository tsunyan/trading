# 読み取り通信の運用者確認付き復旧

2026-09-30。永続停止したGET制御に、2段階の手動復旧を追加しました。
**この機能はネットワーク接続・キー読込・実注文送信を行いません。**
テスト用の一時DBだけで検証し、既存の運用DBの停止は解除していません。

## 復旧できる状態

対象は `client_stop`、`operator_stop`、`clock_invalid`、`interrupted` で停止し、
未完了claimが残っていない読み取り制御です。
`claim_mismatch`、DB不整合、通信中／クラッシュ後の未完了claimは拒否します。
新規version 3のクラッシュ後claimには、先に[OSロックで確認する解消手順](read-orphan.md)を使えます。
この手順だけでは停止は解除されず、本ページの復旧確認が別途必要です。旧版claimは対象外です。
時計逆行後は、時計が同期され、最後に記録した時刻以上になるまで拒否します。
強制解除、古いclaimの自動解放、損失による注文停止の解除は追加していません。

## 2段階の確認

1. `prepare-recovery` で期限付きの確認情報を作成する。この段階では停止したまま。
2. 原因対応などを確認し、`approve-recovery` で明示承認する。

確認情報にはランダムな提案IDと、DB識別子・scope・状態・最後の監査イベントを結びつけた
SHA-256のrevisionを含みます。有効期限は5分未満、使用は1回だけです。
新しい停止指示、別の復旧提案、対象状態の変更で無効になります。
すでに停止中でも、`stop` をもう一度実行すれば未承認の提案を無効化できます。

承認時には同じSQLiteトランザクションで状態・revision・期限を再検査し、
確認イベント、停止解除、復旧イベントを確定します。承認が競合しても成功は1件だけです。
失敗や期限切れで停止を解除することはありません。

## 必須の運用者確認

- `cause`: 停止原因を調査して対応済み
- `permissions`: 使用するAPIキーが読み取り専用の権限であることを確認済み
- `wait`: 業者からの待機指示がある場合、その時間を経過済み
- `clock`: OS時計を同期・確認済み
- `workers-paused`: 関連するワーカーや自動再起動を停止済み

これらは運用者による申告です。ソフトウェアが業者権限や原因解消を証明するものではありません。
第三者承認・二人承認・本人認証システムではなく、ローカルDBへのアクセス権が前提です。
HTTP 401/403/429は既存クライアントで同じ停止コードになるため、
運用者が業者側設定・状況を確認する必要があります。待機時間を自動推測しません。

## CLI

自動実行しないでください。既存の対象ディレクトリ・scopeを使います。

```powershell
uv run python -m trading.read_control status --directory runs/account-read-control --scope operator-account
uv run python -m trading.read_control prepare-recovery --directory runs/account-read-control --scope operator-account
```

出力された `proposal` と `revision` を確認し、5項目を満たした場合にのみ、
5分以内に以下を対話端末で実行します。

```powershell
uv run python -m trading.read_control approve-recovery --directory runs/account-read-control --scope operator-account --proposal <proposal> --revision <revision> --confirm-cause --confirm-permissions --confirm-wait --confirm-clock --confirm-workers-paused
```

さらに `RESUME READS` と入力する確認があります。
パイプ等の非対話CLIは拒否し、確認項目の不足や別の文字列でも停止したままです。
Python APIは `prepare_recovery()` と `approve_recovery(..., confirmations=...)`。
信頼済みの運用コードとテスト向けで、CLIの対話確認をセキュリティ境界とは扱いません。

## 復旧後は作り直しが必要

復旧後も、以前に作った `PersistentReadLimiter` とそれを使うHTTPクライアントは拒否されます。
新しい同一DBの制御オブジェクトとクライアントを、明示的に作成してください。
状態出力の `reopen_required=true` はそのオブジェクトが古いことを示します。
新しく開いたCLIの状態は `reopen_required=false` になり得ますが、
すでに動いている古いプロセスまで有効になるわけではありません。

DB識別子・scopeは維持するので、保存済み資格情報との紐付けを変えません。
ただし古い制御オブジェクトからの資格情報読込も拒否します。
初回復旧時にDBのversionを1から2へ更新します。復旧制御に未対応の旧コードは
version 2を拒否します。新規DBはOSロック対応のversion 3で、復旧時も3のままです。
DBの版を手作業で戻さないでください。

## 残る限界

- version 3のクラッシュ後claimは[専用手順](read-orphan.md)で解消できます。
  version 1/2、ロックファイル欠損・差替え、不整合状態の強制解除・移行は提供しません。
- 古いバックアップへの差し戻し、監査ログの書換え、別DBによる迂回を防ぐものではありません。
- 復旧はGETの再接続を可能にするだけです。口座本人性・完全性・戦略の承認とは無関係です。
- 実Windows資格情報ストアの受入確認、口座本人性、変更イベントの実受信・履歴再同期、
  実口座の会計検証、実注文処理は残っています。
  [通知とRESTの構造照合・再取得](account-sync.md)はオフライン基盤として追加済みです。

検証: `uv run pytest tests/test_read_recovery.py -q`
別プロセスの古い制御、承認競合、期限切れ、再停止、模擬HTTP、資格情報との紐付けを含みます。
