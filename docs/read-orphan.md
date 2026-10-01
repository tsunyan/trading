# 異常終了したGETの未完了記録を解消する

2026-09-30。新規作成する読み取り制御（version 3）に、OS排他ロックと
運用者確認付きの未完了claim解消を追加しました。
**実口座への接続・資格情報の読込・発注・自動復旧は行いません。**

## 保証する範囲

同一PC・同一ローカルディレクトリ・同一scopeを使う、このプロトコルに従うプロセスが対象です。
新しい `slot()` は、claim取得前から待機・GET実行・完了記録まで `read-owner.lock` の
OS排他ロックを保持します。Windowsでは `msvcrt.locking`、POSIXでは `fcntl.flock` を使います。
Windowsでは、ファイルを閉じたときの解除が遅れることがあるため、閉じる前に同じ先頭1バイトを明示的に解除します。
競合時は待ち続けず拒否します。プロセスが死ぬとOSロックは解放されますが、DBのclaimは残ります。

解消の提案時と承認時にも同じOSロックを取得します。
実行中の所有者がいれば拒否し、ロック取得からDBの確定まで排他を維持します。
PIDや経過時間、タイムアウトだけを根拠とする引継ぎはありません。
これはローカルのGET所有者がいないことの確認であり、業者側の処理終了を証明しません。
**注文送信・取消や `order_journal` の未確定注文には使用できません。**

## 操作の順序

1. 関連ワーカーと自動再起動を止め、異常終了の原因を調査する。
2. 同じ制御DBを `stop` で停止させる。実行中の通信を強制中断する操作ではありません。
3. `prepare-orphan` で、claim・DB識別子・scope・状態・監査末尾に結びついた提案を作る。
4. 5分未満に4項目を確認し、対話端末で `approve-orphan` を承認する。
5. claimだけが解消される。**停止は維持され、GETはまだ再開できません。**
6. 再開する場合は別途[読み取り復旧](read-recovery.md)の全確認を行い、制御とクライアントを作り直す。

```powershell
uv run python -m trading.read_control status --directory runs/account-read-control --scope operator-account
uv run python -m trading.read_control stop --directory runs/account-read-control --scope operator-account
uv run python -m trading.read_control prepare-orphan --directory runs/account-read-control --scope operator-account
```

提案の対象・`claim`・期限を確認後、出力された `proposal` と `revision` を指定します。

```powershell
uv run python -m trading.read_control approve-orphan --directory runs/account-read-control --scope operator-account --proposal <proposal> --revision <revision> --confirm-cause --confirm-clock --confirm-workers-paused --confirm-get-only
```

さらに `CLEAR GET CLAIM` と入力します。非対話CLI、確認不足、別の入力では解消しません。

- `cause`: 異常終了の原因を調査・対応済み。
- `clock`: OS時計を同期・確認済み。
- `workers-paused`: 関連ワーカー・自動再起動を停止済み。
- `get-only`: 対象がこのGET専用制御であり、注文の未確定記録ではないことを確認済み。

確認項目は運用者の申告であり、権限・本人性の証明や二人承認ではありません。
Python APIは `prepare_orphan_resolution()` と
`approve_orphan_resolution(proposal, revision, confirmations=...)`。信頼済み運用コード用です。
通信には公開の `slot()` を使い、内部の `_claim()` 等を直接呼ばないでください。

## 拒否・監査・互換性

- 未停止、claimなし、claim監査との不一致、`claim_mismatch` 停止は拒否します。
- 別提案、新しい停止、状態変更、時計逆行、期限切れで承認を拒否します。
- 提案は単回使用です。並行承認でも解消するのは1件だけです。
- `ORPHAN_CHECKS_CONFIRMED` と、元のclaimを参照する `ORPHAN_RESOLVED` を記録します。
  解消はGET成功の証明ではなく、`COMPLETED` として扱いません。
- DB更新・監査追記は同じトランザクションです。保存に失敗したら解消を確定しません。
- ロックファイルのdevice/inodeをDBに保存し、パスと開いたファイルの同一性、サイズ、
  DB識別子を検査します。欠損・差替えを検出したら拒否し、自動再作成しません。
- 新規DBはversion 3です。旧コードはこの版を拒否します。復旧してもversion 3を維持します。
- version 1/2は従来のGETと、未完了claimのない停止からの復旧に限って対応します。
  **旧版claimはOSロックを保持していた証拠がないため解消不可**。自動移行もありません。
- `status` の `orphan_resolution_supported` は形式への対応を示すだけで、今の状態で
  解消できるという意味ではありません。`in_flight` も所有者の生死を示しません。

## 運用上の限界

ローカルファイルの安定した識別子、正常なOSロックとSQLite、協調するプロセスが前提です。
ネットワーク共有、別PC、別DB、他アプリ、プロセスfork後のクライアント再利用は対象外です。
DBやロックファイルをコピー・削除・差し戻し・手編集して回避しないでください。
両方を編集できる利用者による改ざんや古いバックアップへの差し戻しは防げません。
ロックファイルを失ったDBの復旧・旧版からの移行は別途手順が必要で、今回は提供しません。

検証: `uv run pytest tests/test_read_orphan.py tests/test_read_control.py tests/test_read_recovery.py -q`

Windows上の一時ディレクトリで、実OSロック、別プロセスの強制終了・`os._exit`、
実行中所有者の拒否、承認競合、保存失敗、旧版、ファイル差替え、模擬HTTPでの再開制限を検証しました。
POSIX分岐、実業者API、実Windows資格情報ストア、電源断・ディスク障害の実機検証は未実施です。
既存の運用DBは変更していません。
