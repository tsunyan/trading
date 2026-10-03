# 履歴を保存したPrivate通知ジャーナルの切替

2026-10-03。`SegmentedEventJournal`と明示的な接続終了APIを追加しました。
同じSQLite内に区間ごとの通知・ACK・末尾ハッシュと区間間のハッシュ連鎖を保存します。
現在の区間だけを通常の通知処理で検査し、過去の区間は起動時・明示監査・切替前に全件検査します。
実口座での受入確認と、自動で切替時期を判断する長時間運用ランナーは未実装です。

## 切替の条件と手順

新しいディレクトリへversion 2のジャーナルを作成します。既定の1区間上限は1,024記録です。
既存version 1のジャーナルは従来どおり動き、version 2への自動移行はありません。
既存DBの削除・再作成で切り替えないでください。

```python
from trading.segmented_journal import SegmentedEventJournal

journal = SegmentedEventJournal.create(directory, scope, max_records=1024)
# 通常どおり新しいCapture・TokenClient・Receiverを構築し、明示開始・REST照合する。
# 約定通知がある場合は明示したcash_bookへ照合済み約定を計上する。

head = receiver.close_for_rollover(cash_book=book)
journal = journal.rotate(expected_head=head)
# 新しいCapture・TokenClient・Receiverを構築する。トークンと購読のlimiterは引き続き共有する。
# receiver.start(expected_head=journal.head()) の後に、新しいREST口座観測を取得する。
```

`close_for_rollover`は進行中のREST取得、未計上の受信約定、停止中の現金台帳、
ACK不明の記録を拒否します。部分約定注文は、受信した約定をすべて計上済みなら境界を越えられます。
新しい接続でその注文を再取得し、後続約定も照合します。約定通知がなければ台帳の指定は省略できます。
確認後に別のジャーナル書込みがあれば、末尾ハッシュの比較で終了を拒否します。
口座観測を無効化してからソケットと所有トークンを終了し、終了失敗・トークン削除不明なら
切替成功の末尾を返しません。失敗時の新接続・再試行は行いません。

`rotate`は末尾が一致し、最後が正常なENDで、全世代のACKが既知の場合だけ受理します。
旧区間の記録を保管テーブルへコピーし、新しい区間の識別子・連鎖情報と現在区間のリセットを
同じトランザクションで保存します。途中終了後は、旧区間が残る状態か、保存済み旧区間と
空の新しい区間がある状態のいずれかです。commit後の結果不明時はDBを開き直して監査してください。
旧ジャーナルオブジェクト・旧セッションの追記、ACK、現金計上時のガード、再切替は拒否します。
新しい区間を開始しても、過去の区間終了より古い壁時計は受理しません。

区間の切替前に、ENDの1記録と、受信通知・ACK用の余裕を確保してください。
上限到達後に正常終了できるとは限りません。受信イベントも1接続につき既定1,000件までです。
容量・時間・イベント数による切替判断、受信を続けながらの定期REST取得、
永続的な停止と確認付き再開をまとめる運用ランナーは次の工程です。

## 監査と履歴再生

```python
view = journal.inspect()  # 現在区間、archived_segments、容量とACK不明記録
audit = journal.audit_history()  # 全区間・全保存本文・連鎖・孤立記録を検査
history = journal.replay_archive(1)  # 指定区間を診断目的で再生
```

1区間の本文合計は32MB、保存区間数は10,000までです。過去区間を自動削除しません。
同じDBが継続して大きくなるため、ディスク残量監視と外部バックアップは別途必要です。
全履歴の監査は通知受信ループの外で行ってください。旧本文の破損は全監査時に検出します。
現在区間の通常操作は、旧本文を毎回読み直さず最後の保存区間の識別子とハッシュを検査します。

ローカルの履歴保存は業者側通知の連続性を証明しません。新接続の状態は`NEEDS_RESYNC`となり、
新しいREST取得後も`journal_rollover_gap_not_repaired`を残します。接続間の見逃した約定・外部操作の
修復は別工程です。履歴再生も、口座証明・会計の自動再適用・発注許可へ変換しません。
すべての診断は`complete=false`、`live_enabled=false`です。
DBは暗号化しておらず、ハッシュ連鎖は署名や外部の改ざん防止ではありません。

検証:

```powershell
uv run pytest tests/test_segmented_journal.py tests/test_private_rollover.py -q
```

41ケースで30区間の保存、旧書込みの拒否、未計上約定・ACK不明の拒否、部分約定の継続、
破損、同時切替、commit前後のプロセス終了、3接続の購読・削除と新しいREST観測を合成検証します。
