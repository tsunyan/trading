# 送信前の状態診断

2026-10-04。次の送信を止める台帳側の条件を、まとめて表示する読取専用のCLIを追加しました。
送信は最初に見つかった拒否理由だけで止まるため、複数の条件が重なっていると一つずつしか分かりません。
この診断は各条件を別々に評価し、すべての理由を並べます。資格情報・HTTP・claim・状態の変更はありません。

```powershell
uv run python -m trading.live_doctor --directory runs/live-orders --read-control-directory runs/account-read-control --scope <scope>
```

送信可能なら終了コード0、そうでなければ1です。

## 表示する条件

| 項目 | 理由の例 |
| --- | --- |
| `disk_space` | `disk_space_low:<空きMiB>`、`disk_space_unavailable`、`live_operations_unavailable`（実発注台帳・POST・GETと、登録済みの同期・監視・現金台帳などの保存先を個別に検査。空きが1GiB未満、取得不能、計画の差替えや保存先欠落で拒否） |
| `read_control` | `read_control_blocked`（GET制御の停止・未完了claim） |
| `post_control` | `post_stopped:<停止理由>`、`post_claim_in_flight` |
| `approval` | `phase_disabled`、`phase_stopped`、`journal_halted`、`implementation_changed`、`approval_expired` |
| `sync_and_watchdog` | `operations_not_registered`、`live_sync_owner_missing`、`live_sync_unhealthy`、`live_watchdog_unhealthy` など |
| `account_proof` | `account_proof_missing`、`account_proof_stale` |
| `order_queue` | `orders_awaiting_reconciliation:<件数>`（準備済み以外で終わっていない注文） |

`--ledger`・`--hypothesis`・`--config`を指定すると`strategy_promotion`も表示します。設定が実運用に
昇格した凍結候補と同一でなければ不合格です（[昇格管理](promotion.md)）。この項目は新規の提案だけに
関係し、決済・手仕舞いには影響しませんが、送信可否の判定には含めます。

`--cycle-result`で定期サイクルの結果ファイルを指定すると`scheduled_cycle`も表示します。結果が2時間より
古い、ファイルがない・読めない場合は不合格です。毎時のタスクが止まったことに気づくための項目で、
状態ページ（`live_dashboard --cycle-result`、サイクルの`--dashboard-output`）にも表示します。

あわせて、許可の期限と残り秒数、損失による新規停止、注文ごとの状態を表示します。

## 判定の範囲

2026-10-05、容量検査を送信経路へ接続しました。キー読込前後、POST待機後、HTTP直前で新規・決済・取消を
拒否し、停止中の対象限定取消にも適用します。実発注台帳・POST・GETに加え、登録済み同期のルート・
イベント記録・制御・注文カタログ・監視・現金台帳の保存先が別ドライブでも検査します。
元の計画の指紋と保存先を確認し、容量検査で履歴を走査したり同期を開始したりしません。
空き容量を予約する機能ではなく、検査後に
容量が減る可能性は残るため、書込み失敗時の既存の停止・結果不明処理も維持します。

気配の鮮度・口座リスク（注文額・余力・損失上限など）は注文と気配ごとに決まるため、この診断では
判定しません。[確認済み送信](order-runtime.md)の`context`で確認します。準備済みの注文は送信対象なので
`order_queue`を止めません。業者側の状態は読まないため、業者の停止やメンテナンスは表示されません。

## 検証

`tests/test_live_doctor.py`で、健全な台帳での送信可能判定と状態が変わらないこと、POST停止と同期の
所有者不在を同時に表示すること、時間経過での許可失効と口座証拠の期限切れ、台帳停止とGET停止、
未登録・無効の台帳、CLIの終了コードを検証します。
追加5試験が合格しました。Ruffの検査・整形確認、差分チェックも合格しました。

容量不足・取得不能の診断は`tests/test_live_doctor.py`、別保存先の不足と待機中・HTTP直前の減少は
`tests/test_storage_capacity.py`で検証します。新規・決済・通常取消・停止中の対象限定取消について、
HTTPを送らず、未送信のclaimを結果不明にしないことを確認します。`tests/test_order_runtime.py`で
キー読込前後の拒否も確認します。実資格情報・業者通信は使いません。

`tests/test_live_storage_capacity.py`では登録済みの全保存先について、診断と送信境界の拒否、
計画差替え時の拒否、検査が同期の開始や履歴走査を行わないことを確認します。

`--notify-state <ファイル>`を指定すると、送信を止めている条件の組が変わった時だけWindows通知します（[定期実行](live-tasks.md)）。
