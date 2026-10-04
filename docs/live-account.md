# 読取結果から発注台帳の口座証拠を更新する

2026-10-04。読取専用キーで口座を2回走査した結果を、運用者の明示確認つきで
`AccountSnapshot(complete=True)`に変換し、専用の実発注台帳の口座照合へ渡します。
これまで台帳の`update_account`へ実口座の値を供給する経路はなく、合成の口座値だけを使っていました。
POST・発注用キー・注文の状態は使いません。

```powershell
uv run python -m trading.live_quote --output runs/live-orders/quote.json
uv run python -m trading.live_account --directory runs/live-orders --read-control-directory runs/private-reads --scope <scope> --credential-reference <read_only_reference> --quote runs/live-orders/quote.json --confirm complete-account --confirm account-identity --confirm external-writers-paused --valuation-tolerance 0.05
```

`--quote`を省くと、公開tickerから[気配](live-quote.md)を1回取得します。

## 完全性の根拠

[口座読取](account-reader.md)の報告は、建玉・有効注文の全ページ走査と2回の走査結果の一致を示しますが、
口座本人性・取得の原子性・履歴の完全性は示しません。このため自動では完全な口座証拠へ昇格させません。
次の3つの確認がすべて揃った場合だけ`complete=True`とします。

- `complete-account`: 走査した口座全体を、この台帳の口座証拠として扱う
- `account-identity`: 読取専用キーが台帳の口座IDと同じ口座のものだと確認済み
- `external-writers-paused`: 手動発注や別アプリなど、台帳の外から口座を変更する操作を止めている

口座IDはRESTの応答に含まれないため、台帳の方針（`AccountPolicy.account_id`）の値を使います。
最初の有効化の承認では、本人性・読取受入・口座の基準値・履歴の受入証拠を別に要求します。

## 変換規則

- 時刻: 全GET応答のうち最も早い応答時刻。鮮度の判定が甘くならない側を使います。
- 残高・時価評価総額・拘束証拠金・余力: 最後の資産取得の`balance`・`equity`・`margin`・`availableAmount`
- 未決済スワップ: 資産の`totalSwap`。建玉ごとの`totalSwap`の合計と一致しなければ拒否します。
- 建玉: 建玉ID・売買・数量・建値（`price`）
- 有効注文: 台帳でWORKING/PARTIAL/CANCEL_PENDINGかつ受付証拠がある注文だけを認めます。親注文ID・注文ID・
  銘柄・売買・新規/決済・指値・数量・価格が台帳の注文意図と受付証拠に一致しなければ拒否します。
  有効注文の数量は注文全体の数量で未約定数量ではないため、残数量は台帳の約定証拠から求めます。

送信済みでまだGET照合していない自分の注文（SUBMITTING・UNKNOWN・RECONCILING）が有効注文に
あれば、台帳を停止せず`local_order_reconciliation_required`で拒否します。先に
[受付済み注文のGET照合](live-order-sync.md)（結果不明なら[GET照合](order-recovery.md)）を行います。
台帳が説明できない有効注文（手動注文、別アプリの注文、内容の違う注文）を見つけた場合は、
口座照合へ渡さずに台帳を停止します。外部操作を止めていなかったか、台帳の状態が壊れています。

## 照合と停止

変換した口座と気配は、既存の口座照合（`reconcile_account`）で台帳の約定証拠から再構成した
残高・建玉・有効注文と比較します。残高・建玉・有効注文の不一致は台帳に記録して停止します。
古い口座・古い気配・未解決の注文などの再試行可能な理由では停止せず、記録だけします。

評価額だけが一致しない場合は台帳に渡さず、停止もしません（`valuation_time_mismatch`）。業者の時価評価の
時刻とtickerの時刻が違うため、建玉がある間は値動きだけで差が出ます。既定では、手元の気配で評価額を
作り直して業者の値の代わりに使うことはしません。

## 評価額の許容幅（建玉がある場合）

建玉がある口座で照合を通すため、`--valuation-tolerance`で1通貨あたりの許容幅（円、0より大きく1以下）を
明示できます。指定した場合だけ、次のとおり評価し直します。

- 手元の評価額 = 残高 + 未決済スワップ + 各建玉の評価損益（買いは買気配、売りは売気配で評価）
- 業者の評価額との差が「許容幅 × 保有数量 + 方針の`tolerance_jpy`」を超えれば
  `valuation_outside_tolerance`で拒否し、台帳を変更しません。
- 範囲内なら、口座証拠の評価額を手元の評価額に置き換えます。余力は業者の値と
  「手元の評価額 − 拘束証拠金」の小さい方にし、業者の値より増やしません。

損失上限・ドローダウン・余力の検査は、送信時に使う気配と同じ気配で評価した値で行うことになります。
残高・建玉・有効注文の照合は変わりません。出力の`broker_equity`に業者の評価額、
`valuation_adjusted`に置き換えの有無を表示します。値動きの速い時間帯に許容幅を広げると、
業者と手元の評価の差を見逃しやすくなります。実口座の受入で業者の評価方法を確認するまでは、
小さい値（例: 0.05円）から使ってください（[口座読取](account-reader.md)の
`broker_margin_fee_rounding_not_verified`）。

照合に成功すると、台帳の口座証拠・peak・損失による新規停止が更新され、
[確認済み送信](order-runtime.md)の`context`で新しい口座証拠を使えます。

## 前提と制約

- 同期・監視を登録した台帳と、その台帳に紐付くGET制御・読取専用キーを使います。
  GET制御が停止中なら読取専用キーを読みません。
- 読取はGET制御の間隔制限を通り、POSTは使いません。読取の失敗や途中変化は`account_collection_failed`で
  拒否し、台帳を変更しません。
- 建玉がある状態での評価額の照合、手数料・スワップの丸め、口座本人性は実口座での確認が必要です。

## 検証

`tests/test_live_account.py`で、報告から口座証拠への変換、スワップ合計の不一致、台帳と違う有効注文、
GETだけの照合成功とその後の送信確認、確認不足・GET停止時にキーを読まないこと、外部建玉・残高不一致での停止、
台帳にない有効注文での停止、評価額だけの差を停止しないこと、受付済み注文の照合、CLIの失敗表示を検証します。
合成したGET応答・資格情報ストアだけを使い、実通信は行いません。
追加21試験を含む全2299テストが468.40秒で合格しました。Ruffの検査・整形確認、差分チェックも合格しました。
評価額の許容幅について追加9試験を含む全2369テストが529.20秒で合格しました。
