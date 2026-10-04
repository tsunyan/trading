# 停止後の発注許可とPOST制御の明示再開

2026-10-04。`LiveOrderJournal.restart`と`private_order_restart` CLIを追加しました。
最新の完全な口座照合、現在コードでの新しい発注許可、停止原因の確認記録を条件に、
専用実口座台帳と元のPOST制御を再開します。実口座と資格情報は未準備で、検証は一時DB・
模擬HTTP・実プロセスの途中終了で行っています。実口座の有効化は実施していません。

## 再開条件

台帳が`STOPPED`で全体停止を保持し、元のPOST制御に未解決claimがないことを要求します。
結果不明claimは先に[終端結果の解消手続き](order-resolution.md)で解消します。
トークンclaimと、claimなしの`token_failed`はこの手続きで解除できません。
[トークン限定復旧](post-control.md)の失効待ちと確認を別に実施します。
GET制御が停止中・claim未完了の場合も再開を拒否します。

保存済みの注文・約定・受付・準備と送信履歴を検査します。未送信・未送信のまま放棄した注文は、
送信履歴がないことを確認します。送信済み注文は、完全な終端証拠または完全に照合した
`WORKING`/`PARTIAL`だけを許可します。`UNKNOWN`、`SUBMITTING`、`RECONCILING`、`CANCEL_PENDING`や
履歴不完全の証拠では拒否します。完全に照合した残注文があれば、再開後に対象を指定して取消できます。
別の注文が残っている間の新規送信は、既存の注文ゲートが引き続き拒否します。

全口座証拠と気配の鮮度、最新の受入履歴、注文revision、残高・建玉・未約定注文・評価額の一致を
再検証します。口座観測が最後のPOST更新より前なら拒否します。
読取診断の`complete=false`や`executions_complete=false`を完全性の証明へ変換しません。
完全な口座証拠・履歴と本人性は、信頼する供給側・運用者が別に確認する責任です。

再開で全体停止を明示解除しますが、損失による新規停止`entry_halted`、peak、口座開始条件・
上限・注文・約定・受付・取消試行・送信済みID・停止履歴・永久紐付けは保持します。
損失停止中の新規発注は拒否し、条件を満たす建玉指定決済と残注文取消は可能です。
時計異常では追加の`clock-repaired`確認を要求します。時刻の修正や保存済み時刻の巻戻しは行いません。
現在時刻と口座観測が元の時刻検査を満たす必要があります。

## 確認ファイルと実行

停止原因を確認し、完全な注文・口座証拠を別に保存した後、ローカルcontextを取得します。
このCLIは既存の保存物を開くだけで、新しい台帳・制御を初期化しません。

```powershell
uv run python -m trading.private_order_restart context --directory runs/account-live-orders --read-control-directory runs/account-read-control --scope operator-account
```

contextのSHA-256は旧許可・停止状態・POSTの世代と理由・注文・口座ゲート・履歴末尾・現在コードを
含みます。`confirmations`には今回必要な確認事項を表示します。
新しい`LiveApproval`の確認日時は、口座観測日時以降かつ以前の発注許可の確認日時より後を要求します。
有効期間は通常許可と同じ最大7日です。口座や気配の鮮度期限は延長しません。

```python
from pathlib import Path
from trading.live_journal import LiveApproval, LiveRestartApproval

context = journal.restart_context()
approval = LiveApproval(
    account_id=context["account_id"],
    configuration_sha256=context["configuration_sha256"],
    implementation_sha256=context["implementation_sha256"],
    accepted_at=accepted_at,
    expires_at=expires_at,
    evidence=verified_live_evidence,
)
acceptance = LiveRestartApproval(
    checkpoint_sha256=context["checkpoint_sha256"],
    approval=approval,
    stop_review_reference=verified_stop_review_reference,
    stop_review_sha256=verified_stop_review_sha256,
)
Path("restart-approval.json").write_text(acceptance.model_dump_json(), encoding="utf-8")
```

`verified_live_evidence`には本人性・業者条件・読取受入・口座開始条件・完全履歴の参照とSHA-256を
各一件指定します。停止原因の確認も参照とSHA-256で記録し、秘密鍵や生の応答は含めません。
以下を運用者が明示実行します。承認ファイルは最大64,000 bytesで、失敗時は本文を表示しません。
`clock_invalid`の場合は`--confirm clock-repaired`も指定します。

```powershell
uv run python -m trading.private_order_restart restart --directory runs/account-live-orders --read-control-directory runs/account-read-control --scope operator-account --approval restart-approval.json --confirm live-orders --confirm account-identity --confirm broker-rules --confirm read-acceptance --confirm complete-account --confirm external-writers-paused --confirm restart-orders --confirm stop-cause-reviewed --confirm old-clients-closed --confirm preserve-loss-stop
```

ライブラリでは`journal.restart(acceptance, confirmations=...)`を使います。
この操作はローカルの発注許可を有効にしますが、資格情報の読込、HTTP、発注、再接続、Windowsタスク変更は
行いません。同期側に保存した停止の解除と、実口座での受入は別の工程です。

## 保存境界と旧クライアント

元のPOSTのOS所有権を取得し、台帳へ`LIVE_RESTART_PREPARED`を先にcommitします。
次に同じcheckpoint・保存済み承認・現在コード・口座と気配の鮮度・GET制御を再検査します。
台帳transactionを保持し、POST側に再開前後の状態と確認記録ID・SHA-256を`EXECUTION_RESTARTED`と
同時commitした後、台帳側の新しい発注許可・全体停止の解除・`LIVE_RESTARTED`をcommitします。
最後の台帳commit直前にも失効・コード・鮮度・GET停止を検査します。

確認記録だけの保存やPOST commit前で終了すると、元のPOST状態と実口座台帳の停止を保持します。
POST commit後、台帳commit前で終了した場合は、POSTが`READY`でも実口座台帳は`STOPPED`を維持し、
注文送信を拒否します。保存済みの許可を自動使用せず、口座証拠を更新し、新しいcontextと確認で再開します。
台帳commit後は両記録が揃って残ります。履歴・参照・チェックサム・再開前後の状態を再読込時にも検査します。
最新の再開完了記録が欠落または重複した有効化状態は拒否します。

再開commitはPOST世代を進めます。実行したオブジェクトを含む旧クライアントを使用できないため、
元の制御・台帳を開き直し、新しいクライアントを作ります。
再開後の各送信は、従来の1.1秒待機・所有権・一度だけのclaim・口座/気配リスク検査・最終許可検査を通します。
送信済みIDを戻したり、注文・取消を再送したりする手続きではありません。

完全履歴と口座証拠の供給、非終端の結果不明・ID不明・未受付の判定、資格情報・読取同期・
既知注文カタログ・独立監視との接続、戦略昇格と実口座受入は残工程です。

## 検証

追加53試験で、台帳だけの停止・運用者停止・終端claim解消後・時計異常・後片付け失敗からの
再開、現在コードでの再承認、損失停止とpeakの保持、旧クライアントと送信済みIDの再使用拒否、
再開後の新規・建玉指定決済・既知残注文の取消を確認しました。
未解決claim・トークン失敗・GET停止・欠落/不完全/古い証拠、確認項目や期限の不備は拒否します。
各保存境界で実プロセスを終了させ、台帳の停止保持または両記録付きの再開を確認しました。
POST commit後の失効・コード変更・GET停止、再開履歴と参照の欠落/改変、完了記録の欠落/重複、
CLIのファイル容量上限と失敗時の本文非表示も検証しました。
全2092テストとRuffの検査・整形確認、差分チェックが合格しました。
実口座への通信、実資格情報の読込、Windowsタスク変更は行っていません。

2026-10-04追記: 資格情報からの送信は[確認済み送信](order-runtime.md)、口座証拠と注文状態の更新は[口座証拠の更新](live-account.md)・[受付済み注文のGET照合](live-order-sync.md)、有効なまま残る新規・決済のclaim解消とID発見は[claim解消](order-resolution.md)・[ID発見](order-discovery.md)、停止中の無登録台帳は[明示移行](live-operations.md)、戦略との接続は[提案](live-signal.md)と[昇格管理](promotion.md)で追加しました。現在の残工程は[ロードマップ](roadmap.md)を参照してください。
