# 同期モニターからの約定現金計上

2026-10-02。通知ジャーナル・REST照合・[永続現金台帳](execution-cash-book.md)を接続しました。
`JournaledEventCapture.resync` と `PrivateStreamReceiver.resync` に `cash_book` を明示すると、
その取得で一致した個別の約定通知と注文REST証拠を台帳へ渡します。
台帳を省略した取得と、保存済み通知の履歴再生は従来どおり診断だけです。

## 明示指定での計上

ジャーナルと現金台帳は同じ宣言済み `scope` を使います。
台帳の開始残高・対象期間は、既存の `ExecutionCashBook.create` で別途指定してください。
このローカル名の一致は、実口座本人性や開始残高の境界を証明しません。

```python
def collect_orders(order_ids):
    # known_intents は運用者が明示した元の注文意図。現在の有効注文一覧で代用しない。
    return tuple(reader.collect_order(known_intents[order_id], order_id) for order_id in order_ids)


assessment = capture.resync(
    reader.collect_account,
    collect_orders=collect_orders,
    cash_book=book,
)
posting = assessment.execution_cash  # 計上対象がなければNone
# receiver.resyncでも同じ引数を使用できます。
```

既知注文のコレクターは必須です。約定通知がなければ計上処理を呼びません。
複数の通知の一部だけが一致する場合、個別約定の費用が異なる場合、建玉・注文の構造が
一致しない場合には計上しません。REST側だけに存在する約定を計上対象に追加することもありません。
診断集計の合計値ではなく、検証済みの通知と元のREST証拠を渡し、台帳内で再照合します。

残高差だけの `balance_change_unverified` は残したまま、個別に一致した約定を計上できます。
新しい約定による残高変化まで計上拒否すると、初回以外の会計反映が進まないためです。
その結果も `structural_match=false` のままで、全口座同期の成功には変換しません。
入出金や外部操作による差は、`book.compare_balance(assessment.report)` で診断し、補正しません。
[入出金会計](cash-transfers.md)を明示したversion 3では、別途入力した資料一致記録も残高へ反映します。

## 世代と鮮度の確認

取得開始ごとに `revision` を進めます。取得中の通知・同一通知の再配信・切断・接続切替・
期限切れがあれば、その取得結果から計上できません。
受け入れた個別証拠はモニター内部で保持し、後続通知・再取得開始・時計異常で無効化します。
返却された診断モデルを書き換えても、計上に使う証拠は置き換わりません。

取得と計上を分ける場合も、同じ捕捉オブジェクトの現在の結果を使います。

```python
assessment = capture.resync(reader.collect_account, collect_orders=collect_orders)
posting = capture.apply_execution_cash(book, expected_revision=assessment.revision)
```

古いrevisionや現在有効な個別照合がない呼出しは拒否します。
通常のREST観測と同じく、既定30秒の観測期限とストリームの生存期限を確認します。
残高差のため全体の観測が不一致でも、保持した個別照合の期限は別途検査します。

REST取得中は受信処理を止めません。計上中だけローカル捕捉・モニターのロックと
ジャーナルのSQLite書込み予約を保持し、通知の配信と別プロセスの世代引継ぎを止めます。
現在のジャーナル世代・整合性・処理結果不明の記録も検査します。
過去の世代に処理結果不明が残っていれば、新しい通知が一致しても、この接続経路では計上しません。
既存台帳の上限とSQLiteの待機上限を使います。高頻度・長期運用での遅延測定は残っています。

## 停止・再実行と結果の意味

計上の失敗・中断・計上中の観測期限切れで捕捉オブジェクトを停止します。
受信アダプター経由の失敗では、ソケットとトークンも終了します。
秘密情報を含む内部例外は `execution_cash_posting_failed` に置き換えます。

ジャーナルと現金台帳は別DBで、2つのDBと呼出し元への結果返却は原子的ではありません。
現金台帳のcommit後にプロセスが停止すると、呼出しは失敗しても約定が既に計上されていることがあります。
仕訳を取り消さず、台帳を開き直し、新しい捕捉世代と新しいREST照合で同じ約定を再確認します。
既存の約定IDと経済内容が一致すれば再計上せず、矛盾があれば台帳の永続停止を維持します。
自動再起動・自動再取得・自動再生計上はありません。

`execution_cash` は対象台帳の末尾ハッシュ、今回と既計上の約定ID、現金差額、計算残高に加え、
捕捉の `epoch`・`revision`・受信番号を返します。後続通知で古くなる診断値です。
`execution_cash.accounting_applied=true` は指定した個別約定の現金仕訳だけを表します。
`ExecutionReconciliation.accounting_applied=false` と全口座の `execution_accounting_not_applied` は維持します。
全口座の建玉・入出金・全履歴・口座本人性の確認は残り、`complete=false`、`live_enabled=false` です。
[開始建玉を宣言した建玉会計](execution-positions.md)も追加済みです。この台帳を明示すると
計上時に数量・取得価格・決済損益も検証します。初期履歴・外部操作・全口座の確認は残ります。

## 合成デモと検証

```powershell
uv run python -m trading.execution_cash_sync_lab demo --directory runs/execution-cash-sync-demo
uv run pytest tests/test_execution_cash_sync.py -q
```

デモは未使用ディレクトリへジャーナル・現金台帳・`report.json` を作成します。
100万円から手数料2円を一度だけ計上し、同じ取得の繰返し・再起動後の重複・説明できない100円差を
再現します。認証情報・ネットワーク・実口座を使わず、既存出力を上書きしません。
テストでは取得中の配信、古いrevision、期限切れ、別接続の世代引継ぎ、結果不明の通知、
commit前後の失敗、commit直後のプロセス終了、受信アダプターの終了も確認します。
実購読の受入確認、初期履歴・全入出金・初期建玉の実資料との照合は次の工程です。
