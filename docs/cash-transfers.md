# 明細証拠による入出金会計

2026-10-02。宣言した2つの資料源から正規化した入出金記録が一致した場合に、
`ExecutionCashBook.apply_transfers` で現金と支払手数料を一度だけ保存できるようにしました。
約定・建玉会計と同じDBを使います。実際の送金や明細取得APIは含みません。

## 資料と開始境界の宣言

新規作成時に `OpeningCash.transfer_policy` を指定するとversion 3になります。
`position_basis` も指定すれば、約定時の建玉・決済損益検証を併用できます。
既存のversion 1／2は変更せず、入出金入力を拒否します。自動移行はありません。

```python
from datetime import UTC, datetime
from pathlib import Path

from trading.cash_transfers import CashTransferPolicy
from trading.execution_cash_book import ExecutionCashBook, OpeningCash
from trading.execution_positions import PositionBasis

book = ExecutionCashBook.create(
    Path("runs/declared-transfer-book"),
    "declared-account-scope",
    OpeningCash(
        balance="1000000",
        cutoff=datetime(2026, 10, 1, tzinfo=UTC),
        position_basis=PositionBasis(positions=()),
        transfer_policy=CashTransferPolicy(
            primary_source="broker-statement", confirmation_source="bank-statement"
        ),
    ),
)
```

資料源は異なる名前を宣言します。名前が本人の口座や真正な資料に対応することは検証しません。
開始残高・開始建玉・期間境界も運用者の宣言で、資料の範囲や履歴の完全性は未証明です。

## 正規化した記録の入力

呼出側で資料を読み、次の記録を作ります。サンプルの資料ハッシュは合成値です。
実資料では元の資料のSHA-256を指定してください。資料本文はこのAPIへ渡しません。

```python
from trading.cash_transfers import CashTransferEvidence, CashTransferMatch, CashTransferRecord

occurred = datetime(2026, 10, 2, tzinfo=UTC)
record = CashTransferRecord(
    transfer_id="Deposit-001",
    kind="DEPOSIT",
    amount="10000",
    fee_debit="0",
    occurred_at=occurred,
)
matched = CashTransferMatch(
    primary=CashTransferEvidence(
        source="broker-statement",
        reference="Broker-123",
        document_sha256="1" * 64,
        observed_at=occurred,
        record=record,
    ),
    confirmation=CashTransferEvidence(
        source="bank-statement",
        reference="Bank-456",
        document_sha256="2" * 64,
        observed_at=occurred,
        record=record,
    ),
)
result = book.apply_transfers((matched,))
```

両資料の記録は、共通の入出金ID・入出金種別・総額・手数料・発生日時・通貨・状態が
一致する必要があります。資料ごとの `reference` は元資料の安定した取引参照です。
共通IDと参照の対応付け、総額や日時の正規化は呼出側の責任です。

- 通貨はJPY、状態は `SETTLED`、種別は `DEPOSIT` または `WITHDRAWAL` に限定します。
- 総額は正、支払手数料は非負。総額は手数料を含めず、手数料は別途現金から引きます。
- 発生日時は `cutoff` より後、両観測日時は発生日時以降で、タイムゾーンが必要です。
- 宣言した資料源の組合せと、異なる資料ハッシュを要求します。
- 資料源はASCII英数字・`_`・`-`で64文字まで。ID・参照は先頭が英数字で、
  ASCII英数字・`.`・`_`・`:`・`-`を使った128文字までです。

入力は1〜1,000件のtuple、正規化本文合計2MBまでです。バッチ全件を検証してから保存するため、
不一致を含むバッチから一部だけ計上しません。資料ハッシュの形式と一致条件の検査は、
原本の真正性・口座本人性・二重の独立した証明を保証するものではありません。

## 仕訳と重複防止

正は借方、負は貸方です。3つの仕訳を証拠本文・ID・連番・末尾ハッシュと同時にcommitします。

| 勘定 | 入金10,000円、手数料0円 | 出金2,500円、手数料3円 |
| --- | ---: | ---: |
| `cash` | 10,000 | -2,503 |
| `external_capital` | -10,000 | 2,500 |
| `fee_expense` | 0 | 3 |
| 合計 | 0 | 0 |

入出金の元本を取引損益へ加算しません。`snapshot()` の `external_capital_amount` は
入金総額から出金総額を引いた値、`external_cash_amount` はさらに入出金手数料を引いた値です。
`transfer_fee_debit` は入出金手数料の合計で、約定の `fee_debit` とは別です。
金額は小数8桁まで、各金額・会計合計の絶対値は10^18円までで、超過や端数を丸めず拒否します。

共通IDと経済的内容・両資料の参照が同じなら、資料ハッシュや観測日時が更新されても再計上しません。
`applied_transfer_ids`、`already_applied_transfer_ids`、今回の `cash_delta` を返します。
同時入力・再起動・commit後の結果消失でも、同じDBへの再入力は一度だけ反映されます。

一致済みの内容や参照が変わった場合、または同じ資料源の参照を別IDに使った場合は、
`cash_book_transfer_identity_conflict` を永続記録します。最後の残高と末尾ハッシュを保持し、
再起動後も約定・入出金の追加を拒否します。自動解除はありません。

## 検査と残る条件

version 3の件数上限は約定と入出金で共有し、既定5,000件です。本文合計32MBも共有します。
毎操作で両方の証拠・仕訳・ハッシュ連鎖を再検査し、欠損や破損時には再作成しません。
公開する `head` は約定と入出金の両末尾を含むハッシュです。どちらの更新でも変わります。
ハッシュは不整合の検出用で、署名・悪意あるDB編集・古いコピーへの復元の防止には使えません。

REST残高・建玉の比較では、最新の約定または入出金より古い観測を拒否します。
残高差から入金を推定する処理や補正仕訳はありません。同期捕捉アダプターは約定だけを取り込み、
入出金はこのAPIへ明示的に入力します。両処理は同じDBのトランザクションで順に実行されます。

一致しても `complete=false`、`live_enabled=false` です。実資料の認証、資料からの取得・正規化、
初期残高・開始建玉・失った履歴・全入出金・外部操作の照合、実口座での受入確認が残ります。

## 合成デモ

```powershell
uv run python -m trading.cash_transfer_lab demo --directory runs/cash-transfers-demo
uv run pytest tests/test_cash_transfers.py -q
```

ネットワークを使わず、100万円から入金10,000円、出金2,500円、入出金手数料3円、
新規約定手数料2円を反映して1,007,495円にします。重複・再起動・REST残高と建玉の一致、
説明不能な100円差を保存します。未使用ディレクトリにDBと `report.json` を作り、上書きしません。
検証では破損・不正入力・保存途中の失敗・commit前後のプロセス終了・同時入力・共有上限も確認します。
電源断・ディスク障害の実機試験、実資料・実業者との疎通は未実施です。
