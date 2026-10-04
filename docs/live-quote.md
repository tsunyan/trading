# 発注確認用の気配取得

2026-10-04。GMO FXの公開tickerからUSD_JPYの気配を1回取得し、[確認済み送信](order-runtime.md)の
`--quote`に渡すJSONを書き出します。資格情報・注文API・台帳には触れません。

```powershell
uv run python -m trading.live_quote --output runs/live-orders/quote.json
uv run python -m trading.order_runtime context --directory runs/live-orders --read-control-directory runs/private-reads --scope <scope> --client-id <client_id> --quote runs/live-orders/quote.json
```

## 取得と検査

固定した公開ホストの`/public/v1/ticker`へ認証ヘッダーなしのGETを1回だけ送ります。
リダイレクト・環境変数のプロキシ設定は使わず、5秒の期限と16384バイトの上限を設けます。
HTTP 200と`application/json`以外、重複キー、USD_JPYの行が0件または複数件、文字列でない価格、
指数表記、逆転した気配、`OPEN`/`CLOSE`以外の状態、時差のない時刻を拒否します。
ticker本文はエラーに表示しません。

価格は文字列から`Decimal`へ変換し、浮動小数点を経由しません。研究用の`GmoPublic.quote`は
浮動小数点のため、発注確認には使いません。`CLOSE`は`market_open=false`として書き出し、
口座リスク検査が`market_closed`で拒否します。気配時刻が受信時刻より2秒を超えて未来なら拒否します。
古さは書き出し時に判定せず、`context`と送信時の口座リスク検査（`max_quote_age_seconds`）に任せます。

出力ファイルは一時ファイルを経由して置き換え、途中まで書かれた気配を読ませません。
内容は`AccountQuote`の項目だけで、`order_runtime`がそのまま検証できます。

## 運用上の注意

`context`と送信には同じ気配ファイルを使います。気配を取り直すと実行内容のSHA-256が変わるため、
`context`から確認し直します。口座証拠の気配より古い気配も拒否します。
気配の取得は口座本人性・注文可否・約定価格を保証しません。

## 検証

`tests/test_live_quote.py`で、模擬HTTPから取得した正確な小数、認証ヘッダーなしの単一GET、
市場閉鎖の保持、不正な本文・状態・時刻、HTTPエラー・大きすぎる本文・期限超過、
置き換え書込みと`order_runtime`での読込、CLIの失敗表示を検証します。実ネットワークは使いません。
追加21試験と関連する公開APIの試験が合格しました。Ruffの検査・整形確認、差分チェックも合格しました。
2026-10-04（日曜）に公開APIの実データで取得を確認しました。市場閉鎖中の状態`CLOSE`を`market_open=false`として読み、閉鎖中のスプレッドは0.1円でした。

## サービス状態

`fetch_status()`は`GET /public/v1/status`を1回読み、`OPEN`・`CLOSE`・`MAINTENANCE`のいずれかを返します。
tickerの`status`は`OPEN`・`CLOSE`だけで、定期メンテナンスを区別できないため別に読みます。
それ以外の値・重複キー・`status`が0でない応答は`invalid_public_service_status`で拒否します。
[運用サイクル](live-cycle.md)は`MAINTENANCE`の時に非公開GETを行わず見送ります。
2026-10-04（日）の実データでは`CLOSE`でした。
