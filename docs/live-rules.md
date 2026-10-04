# 注文上限と業者の取引ルールの照合

2026-10-04。実発注台帳の注文上限（`OrderLimits`）が、GMO FXの公開APIが示すUSD_JPYの取引ルールと
食い違っていないかを確認するCLIを追加しました。資格情報・注文APIは使いません。

```powershell
uv run python -m trading.live_rules check --config configs/live-orders.local.json --output evidence/broker-rules.json
uv run python -m trading.live_rules check --directory runs/live-orders --read-control-directory runs/account-read-control --scope <scope>
```

台帳を作る前は設定ファイル、作った後は台帳に固定された上限を照合します。
公開APIの`/public/v1/symbols`へ認証なしのGETを1回送り、USD_JPYの最小新規注文数量・最大注文数量・
数量の刻み・呼値を、浮動小数点を経由せずに読みます。行の欠落・重複・不正な値は拒否します。

## 照合する内容

| 理由 | 内容 |
| --- | --- |
| `min_units_below_broker_minimum` | 台帳の最小数量が業者の最小新規注文数量より小さい |
| `max_units_above_broker_maximum` | 台帳の最大数量が業者の最大注文数量より大きい |
| `unit_step_not_multiple_of_broker_step` | 台帳の数量単位が業者の刻みの倍数でない |
| `min_units_not_multiple_of_broker_step` | 台帳の最小数量が業者の刻みの倍数でない |
| `price_tick_not_multiple_of_broker_tick` | 台帳の呼値が業者の呼値の倍数でない |

食い違いがなければ終了コード0、あれば1です。台帳の上限は作成後に変更できないため、
食い違いがある場合は台帳を作り直します（[手順書](live-setup.md)）。

`--output`は取得したルールと照合結果を新しいファイルに保存します（上書きしません）。
これを[受入証拠](live-acceptance.md)の`rules`として`file-evidence`で指紋化できます。
公開APIのルールは業者の告知や約款の代わりではありません。決済注文の扱いや取引時間などは別途確認します。

## 検証

`tests/test_live_rules.py`で、単一GETからの正確な値の読取、不正な応答の拒否、各食い違いの検出、
設定ファイルの照合と証拠ファイルの保存・上書きの拒否・入力不足のCLI表示を検証します。実通信は行いません。
追加11試験が合格しました。Ruffの検査・整形確認、差分チェックも合格しました。
