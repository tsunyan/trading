# 台帳の評価損益・保有証拠金の診断

2026-10-02。`ExecutionCashBook.compare_valuation` は開始建玉を宣言した台帳の
取得価格・数量・現金残高を使い、明示したローカル計算条件とREST観測値との差を表示します。
業者の計算仕様を確定した機能ではありません。通信・注文送信・台帳への書込みは行いません。

## 計算条件

`ValuationPolicy` で証拠金率、円の丸め単位、丸め方向、丸める集計範囲、
観測スワップを加算するか、見込み手数料を引くか、比較の許容差を明示します。
率は0より大きく1以下、丸め単位は0より大きく1円以下、金額は小数8桁までです。
`CEILING`、`FLOOR`、`HALF_EVEN` を選べます。建玉ごとに丸めて合算する `POSITION` と、
合算後に丸める `ACCOUNT` を区別します。数量の相殺や決済注文による証拠金軽減はありません。

| 項目 | ローカルモデル `declared-held-gross-ask-v1` |
| --- | --- |
| 買いの評価損益 | `(BID − 台帳の平均取得価格) × 数量` |
| 売りの評価損益 | `(台帳の平均取得価格 − ASK) × 数量` |
| 保有額 | 全建玉について `ASK × 数量` を合算 |
| 保有証拠金 | 保有額に宣言した率を掛け、宣言した単位・方法・範囲で丸める |
| 時価評価額 | 台帳現金＋計算評価損益＋選択した観測スワップ−選択した見込み手数料 |
| 余力 | 時価評価額−保有証拠金。負値も保持 |

これはGMOの確定式ではありません。[読み取り基盤の制約](account-reader.md)は継続します。
スワップと見込み手数料は符号付きの観測入力で、台帳から再構築した値ではありません。
スワップを使用する場合は、REST建玉別の合計と資産欄の合計も比較します。
`transferable_amount` の計算式・比較は追加していません。

評価額は分数で計算し、Decimalの精度設定に依存しません。`exact` の分子・分母が
比較の根拠です。`display_jpy` は8桁のhalf-even表示に限り、比較には使いません。
分子・分母は4,096ビットまでで、計算上限を超える場合は診断を拒否します。

## 入力と結果

```python
from trading.account_valuation import ValuationPolicy, ValuationQuote

policy = ValuationPolicy(
    margin_rate="0.04",
    margin_quantum_jpy="1",
    margin_rounding="CEILING",
    margin_rounding_scope="ACCOUNT",
    include_reported_swap=False,
    subtract_reported_estimated_fee=False,
    tolerance_jpy="0",
)
quote = ValuationQuote(bid="149.9", ask="150.1", observed_at=observed_at)
result = book.compare_valuation(report, quote, policy, evaluated_at=evaluated_at)
```

数字は合成例です。`report` は `AccountReadReport`、時刻はタイムゾーン付きdatetimeを渡します。
気配・観測の鮮度上限はそれぞれ既定60秒、設定範囲1〜300秒です。評価時刻より未来の
受信・気配を拒否し、全REST応答と気配の時刻差も気配鮮度上限内に限定します。
`clock_skew_ms` は業者応答時刻だけの許容幅で、既定0、最大1,000msです。
REST観測と気配のどちらも、台帳の最新約定・入出金と開始境界より前なら拒否します。

入力は再検証し、2MB、建玉・有効注文は各1,000件、観測は最大10,000件に限定します。
建玉ID・方向・数量・取得価格も比較します。取得価格の許容差は既定0、最大0.01円です。
金額比較の許容差は0〜1円です。
結果には台帳の `head`、気配、宣言条件、評価時刻、建玉別の計算値、項目別の差額が含まれます。
差額の向きは `観測 − モデル` です。残高も比較し、残高差を評価損益に吸収しません。

有効注文がある場合は注文総数量から未約定数量を推測せず、
`active_order_margin_not_modeled` と未計算注文IDを表示します。
この場合 `diagnostics_match=false` です。注文がなくても履歴完全性・本人性・同時点性・
業者式は未証明で、`complete=false`、`live_enabled=false` が常に残ります。
台帳停止中も停止状態と未解決条件を返します。診断一致による停止解除はありません。

## 口座なしのデモ

```powershell
uv run python -m trading.account_valuation_lab demo --directory runs/account-valuation-demo
```

未使用ディレクトリを指定してください。合成REST応答と宣言した買い400通貨から、
評価損益−40円、保有証拠金2,402円、一致する観測・再起動・評価額の不一致を示します。
台帳と `report.json` を作成します。実気配の認証・有効注文の証拠金・業者式の受入確認・
同期モニター経由の評価診断は残件です。発注ゲートへの接続はありません。
