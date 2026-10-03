# 同期経路からの評価損益・保有証拠金診断

2026-10-02。[台帳の評価診断](account-valuation.md)を同期モニター・通知捕捉・
Private WebSocket受信アダプターへ接続しました。
取得回ごとに口座資料・気配・計算条件を保持し、現在の世代とrevisionだけで比較します。
評価診断は台帳へ書き込まず、`complete=false`、`live_enabled=false` を維持します。

## 使用方法

通知ジャーナルと同じローカルscope、開始建玉を宣言したversion 2／3の台帳を指定します。
scopeの一致は実口座本人性を証明しません。

```python
from trading.account_valuation import ValuationQuote


def collect_quote():
    # 呼出時に取得した気配とその観測時刻を渡す。
    return ValuationQuote(bid=bid, ask=ask, observed_at=observed_at)


assessment = capture.resync(
    reader.collect_account,
    collect_quote=collect_quote,
    valuation_policy=policy,
    valuation_book=book,
)
comparison = assessment.account_valuation
```

`policy` は [ValuationPolicy](account-valuation.md) です。
コールバックは引数なしで `ValuationQuote` を返します。
実気配の通信・認証アダプターは追加していません。呼出側が取得手段を明示します。
`receiver.resync` でも同じ引数を使えます。

気配コールバックとpolicyは対で指定し、片方だけなら取得開始前に拒否します。
評価台帳を指定した場合はその対が必須です。policyは再検証・複製してから取得を始めます。
scopeが違う台帳や、併用する会計・拘束照合台帳とパスが違う場合も取得前に拒否します。
細かな金額範囲・精度の検査は台帳の評価診断にも適用します。

取得順は「拘束照合用の個別注文資料 → 口座資料 → 約定照合用の個別注文資料 → 気配」です。
個別注文資料は対応するオプションを指定した場合だけ収集します。
約定照合用資料は約定通知がある場合だけ収集します。
気配の観測時刻は今回の取得開始以降、取得終了以前であることを要求します。
鮮度内でも前回の気配を使い回せません。業者応答時刻の許容幅 `clock_skew_ms` を
気配の未来時刻許可へ流用しません。

## 取得と診断を分ける場合

```python
assessment = capture.resync(
    reader.collect_account,
    collect_quote=collect_quote,
    valuation_policy=policy,
)
comparison = capture.compare_account_valuation(book, expected_revision=assessment.revision)
```

台帳を省略した取得では `account_valuation=None` です。
モニター内部には受け入れた資料を一時保持し、返却モデルや辞書の変更を取り込みません。
次の取得・通知到着・切断・時計異常で資料を破棄します。取得時に気配収集を省略すると、
以前の評価資料は使えません。保存済みジャーナルから資料を復元する処理もありません。

モニター単独では `resync(session, ..., collect_quote=..., valuation_policy=...)` の後に
`compare_account_valuation(session, book, expected_revision=...)` を使えます。
ジャーナルの検査が必要な経路では捕捉アダプターを使ってください。

## 鮮度・世代・失敗

取得ごとにrevisionを更新し、通知の再配信も変更として扱います。
収集全体・内部資料保持の期限は既定30秒です。取得中は捕捉ロックを保持せず、通知を
保存・配信できます。その到着によって進行中の取得結果を拒否します。
構造不一致があっても評価資料の保持期限を検査します。

評価時刻はモニターの現在時刻を使います。気配・口座資料についてpolicyの鮮度上限を適用し、
比較後も現在のrevision・保持期限・policyの鮮度を確認します。
比較中に期限を超えた結果は返しません。台帳の最新仕訳より古い資料も従来どおり拒否します。

比較中は捕捉・モニターのロックとジャーナルのSQLite書込み予約を保持します。
別プロセスの世代引継ぎや、現在／前世代の処理結果不明の記録があれば比較を拒否します。
比較例外は `account_valuation_comparison_failed` に統一し、捕捉を停止します。
中断例外は停止後に再送出します。評価台帳を指定した受信アダプターでの失敗は、
ソケットとトークンの終了処理にも接続します。自動再接続はありません。

約定の現金計上・拘束照合と併用した場合は、計上 → 拘束照合 → 評価診断の順に実行し、
各結果の台帳 `head` が同じことを要求します。途中で別プロセスが約定・入出金を計上して
ハッシュが変わった場合は結果を拒否します。複数DBにまたがる原子的な確定ではありません。
評価診断が失敗しても、先に完了した現金計上を戻しません。
新しい捕捉世代で通知・口座・注文・気配を再取得すれば、既存約定IDの冪等性により二重計上を防げます。

結果には `epoch`、`revision`、受信番号、台帳 `head`、気配・policy・評価時刻を含めます。
イベント／RESTの `structural_match` と評価診断の `diagnostics_match` は別です。
気配本人性、同一時点の完全性、業者の確定式、有効注文の証拠金、実口座受入は未確認です。
発注ゲートへの接続はありません。

## 口座なしのデモ

```powershell
uv run python -m trading.account_valuation_sync_lab demo --directory runs/account-valuation-sync-demo
```

未使用のディレクトリを指定してください。合成入力で一致・評価額差・期限切れ・
新しい世代での再取得を示し、台帳・通知ジャーナル・`report.json` を保存します。
