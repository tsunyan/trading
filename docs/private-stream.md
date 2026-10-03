# Private WebSocket受信とトークン管理

2026-10-02。`PrivateTokenClient` と `PrivateStreamReceiver` を追加しました。
明示的に渡された資格情報でトークンを取得し、3種類の通知を購読して、
既存の `JournaledEventCapture` へ保存後に渡します。実口座接続は未検証です。
診断結果は常に `complete=false`、`live_enabled=false` で、全口座会計や発注許可には変換しません。
[同期モニターからの現金計上](execution-cash-sync.md)を追加しました。
`resync(..., collect_orders=..., cash_book=book)` で明示指定した台帳に個別の一致約定を渡せます。
計上経路の失敗では受信・トークンを終了します。台帳なしの照合は従来どおり診断です。
2026-10-03、台帳を明示した取得は既定で完全計上済み注文の再取得を省きます。
`incremental_cash=False`で全注文取得も選べます。部分約定・新しい通知は取得を継続し、
省略した過去の計上証拠を新しいREST照合や完全な口座証明として扱いません。
[同期経路からの建玉拘束照合](position-reservation-sync.md)も追加しました。
`collect_reservations` と `reservation_book` を明示すると現在の取得に対して拘束数量を比較します。
この比較経路の失敗でも受信・トークンを終了し、発注許可には変換しません。

## 通信と期限

[GMO FXのトークン取得](https://api.coin.z.com/fxdocs/#ws-auth-post)・
[延長](https://api.coin.z.com/fxdocs/#ws-auth-put)・
[削除](https://api.coin.z.com/fxdocs/#ws-auth-delete)の公開仕様を確認しました。
トークンの取得・延長後の有効期限は60分です。署名はPOSTのみ本文を含み、
PUT・DELETEは時刻・メソッド・パスだけを使います。

- HTTPは固定の `/private/v1/ws-auth` のみ。POST・PUT・DELETEの各専用処理以外はありません。
  資格情報は `SecretStr` で明示的に渡し、環境変数や資格情報ストアから自動取得しません。
- TLSを検証し、プロキシ・HTTPリダイレクト・Cookie再利用を無効にします。
  応答は4KB、通信の各待機は5秒、応答の経過時間は10秒までです。
  重複JSONキー、不正な形式、古い応答、認証・APIエラーは拒否します。
- 期限はリクエスト開始から59分30秒とし、50分経過時に延長します。
  単調時計と壁時計の両方で判定し、時計逆行や旧期限後の延長完了を拒否します。
  `clock_skew_ms` は0〜1,000ms、既定0msで、業者応答時刻の比較にだけ使います。
- 取得失敗の自動再試行・再発行はありません。結果不明の取得でトークンを増やしたり、
  期限切れを延長して受信を再開したりしません。
- `PrivateStreamLimiter` を同一口座・送信元IPのクライアント間で共有します。
  トークン操作と購読を直列化し、完了から次の開始まで1.1秒空けます。
  [購読の同一IP毎秒1回の制限](https://api.coin.z.com/fxdocs/#restrictions-private-ws-api)を踏まえた設定です。
  既存のGET制御DBとは独立したプロセス内制御で、別プロセス・別PC・他アプリは制御できません。

トークン文字列はURLの1要素として安全な英数字・`_`・`-`だけを受理します。
公開仕様が文字種を保証しているわけではないため、この制限に合わない実トークンは拒否します。
トークンや接続URLは保存せず、返すURLも `SecretStr` です。
例外・状態表示は固定コードで、WebSocketライブラリ専用のログ出力は無効にします。

## 受信と保存

受信ソケットには `websockets` 15の同期クライアントを使います。
固定WSSホスト、TLS検証、プロキシなし、圧縮なし、接続・終了の待機5秒、
1メッセージ16KB、ライブラリの受信バッファ上限4フレームを設定します。
断片化はライブラリが結合し、満杯時はバックプレッシャーを掛けます。
アプリ側に通知を捨てるキューはありません。

購読対象は `executionEvents`、`orderEvents`、`positionEvents` です。
購読要求の送信完了を、業者の購読完了応答や通知の網羅性の証拠には扱いません。
未対応通知や不正データは既存パーサーが拒否し、その世代を終了します。

完全なデータメッセージをライブラリから取り出した時点で、1から連続するローカル受信番号を付けます。
保存・モニターへの受渡し・ACKの順序は[既存の通知保存](event-journal.md)と同じです。
番号と保存時刻はアプリの受信境界を示し、ソケット到着時刻や業者の連続番号ではありません。
ライブラリへ届く前の通知欠落や購読開始前の変化は証明できません。

[GMOのサーバーping](https://api.coin.z.com/fxdocs/#private-ws-api)にはライブラリがpongを返します。
受信ループも30秒間隔でpingを送り、対応するpongを確認したときだけ死活確認を記録します。
15秒以内に確認できないpongは拒否します。送信したpingや受信待機のタイムアウトだけでは
死活確認を更新しません。制御フレームはローカル通知番号を消費しません。

`step()` は最長1秒の受信待機中も、トークン期限・保存先の世代・モニターの期限を検査します。
保存先の検査は、ジャーナル末尾のハッシュが自分の最後の書込み・全件検証の時点から
変わっていなければ、全件検証を省きます。別プロセスの世代開始など、末尾が変わったときだけ全件を検証します。
全件の検証は、通知・死活確認の記録とACKのたびに行います。
`resync()` のREST取得中は受信ロックを保持しません。並行する通知・切断・保存先の世代切替で
古い結果を無効にし、トークンの有効性もREST取得前後で確認します。
Captureの最終確認の後、受信ロックを取り直すまでの間に通知が届いた場合も、
`capture_changed_during_collection` で結果を返しません。台帳への計上は済んでいることがあり、
再取得で同じ約定を再確認すれば二重には計上されません。
REST再取得は受信ループとは別の呼出側で実行してください。

死活確認1回につき、ジャーナルには死活確認とACKの2件が増えます。既定の上限10,000記録では、
通知がなくても約41時間（最大の20,000記録では約83時間）で上限に達し、受信は終了します。
上限で処理結果不明の記録は残りません。長く動かす場合は、上限の前に受信を止め、
新しいジャーナルを作って新しい世代から始めてください。ジャーナルの自動切替は未実装です。

## 明示的な開始と終了

ライブラリAPIとして提供します。生成や `status()` は通信を開始しません。
`start(expected_head=...)` は、ジャーナルの世代開始が成功してからトークンを取得します。
前の受信処理を止め、未開始のCaptureとTokenClientを渡してください。
頭ハッシュが古ければ、トークン取得前に拒否します。

```python
from trading.private_stream import PrivateStreamReceiver
from trading.private_stream_token import PrivateStreamLimiter, PrivateTokenClient

# api_key / secret は明示取得した SecretStr、capture は未開始の JournaledEventCapture。
# expected_head は運用側で確認したジャーナルの末尾ハッシュ。
limiter = PrivateStreamLimiter()
tokens = PrivateTokenClient(api_key, secret, limiter=limiter)
receiver = PrivateStreamReceiver(tokens, capture)
try:
    receiver.start(expected_head=expected_head)
    receiver.run(stop_event)  # 呼出側が所有する threading.Event で停止する。
finally:
    receiver.close()
```

`run()` は呼出側のスレッドで動きます。アプリのバックグラウンド起動・再接続・自動再開はありません。
通信・保存・期限の失敗で世代を終了し、ソケットを閉じ、所有トークンの削除を試みます。
口座観測はネットワークの後片付けより先に無効化します。
失敗後の再接続には新しいReceiver・Capture・TokenClient、末尾確認と新しいREST観測が必要です。
履歴欠損の未解決条件は維持します。

HTTP操作が結果不明だった場合や停止済みリミッターにより削除を送れない場合は、
`token_cleanup_unknown=true` を残します。削除成功とは表示せず、再試行もしません。
このフラグはプロセス内の診断で、再起動後のトークン所有を復元する台帳はありません。
リモートに残ったトークンの扱い・長期運用・実権限の受入確認は残件です。

## 口座なしのデモ

```powershell
uv run python -m trading.private_stream_lab demo --directory runs/private-stream-demo
```

未使用ディレクトリに `event-journal.sqlite` と `report.json` を作ります。
合成HTTP・ソケット・時計で、購読→REST観測→通知保存→無通知時の延長→期限切れによる終了を再現します。
ネットワーク、実トークン、実時間の待機は使わず、既存出力を上書きしません。

検証は `uv run pytest tests/test_private_stream.py -q` で実行できます。
署名、入力制限、停止、後片付け、死活監視、世代競合、REST取得中の通知を模擬通信で確認します。
実業者のTLS・購読応答・反映遅延・再接続挙動、会計反映、履歴完全性は未検証です。
