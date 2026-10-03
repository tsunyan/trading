# 終端結果を確認した注文claimの解消

2026-10-04。新規・建玉指定決済・取消の結果不明claimについて、保存済みの終端注文証拠と
全口座照合、運用者の確認を条件に、ローカルのclaimを解消する手続きを追加しました。
解消後も実口座台帳とPOST制御は停止したままです。送信、再送、再接続、取引再開は行いません。
実口座と資格情報は未準備で、検証には一時DB・模擬HTTP・実プロセスの途中終了を使っています。

## 解消できる条件

元のGET・POST制御と専用実口座台帳の永久紐付けを検査します。POSTの対象が`order`、
`close_order`、`cancel`のいずれかで、保存した要求ハッシュ・送信履歴・業者IDが一致することが必要です。
対象注文が`FILLED`、`CANCELED`、`EXPIRED`で、保存証拠の`executions_complete=true`と最新の
照合履歴が一致することを要求します。全量約定では数量一致も必要です。
取消の場合は、取消claimを消費した時点のGET証拠を履歴から検証します。

完全な正規化口座証拠と気配を再検証し、現在の注文台帳から残高・建玉・未約定注文・評価額を
再計算して比較します。保存した口座証拠の注文revision、受入履歴、鮮度、本人性を検査し、
口座観測時刻が最後のPOST更新より前なら拒否します。GET制御が停止中・claim未完了でも拒否します。

[GET調査CLI](order-recovery.md)の一致結果は`executions_complete=false`のままです。
この手続きで完全性へ変換しません。完全履歴と全口座証拠は、信頼する供給側・運用者が別途
業者の証拠から確立する必要があります。参照とSHA-256は、その確認を記録するためのものです。
完全性の供給側と実口座での受入は未完了です。
空のGET、履歴の欠落、受付だけ、ID不明、時間経過ではclaimを解消できません。

## 業者側で有効なまま残っている新規・決済注文

2026-10-04追加。新規・建玉指定決済の結果不明claimで、業者側に注文が受け付けられて有効なまま
（`WAITING`・`ORDERED`・`MODIFYING`）残っている場合も解消できます。条件は終端の場合と同じく、
保存証拠の`executions_complete=true`、最新の照合履歴との一致、完全な口座証拠と気配の再検証です。
台帳の状態は約定がなければ`WORKING`、一部約定なら`PARTIAL`で、全量約定済みの有効注文は拒否します。
口座証拠には、この注文が未約定数量つきの有効注文として含まれている必要があります。

contextは`terminal_state`の代わりに`active_state`を持ちます。確認項目は`terminal-order`の代わりに
`active-order`を指定し、ほかの6項目は同じです。終端用の確認項目では有効注文を、有効注文用の
確認項目では終端注文を解消しません。解消後も注文は有効なまま台帳に残り、
[明示再開](order-restart.md)の後に通常の取消や約定照合の対象になります。

取消の結果不明claimは、注文が有効なまま残っていても解消できません。取消が業者に届いていないのか、
処理待ちなのかを有効注文の証拠だけでは区別できないためです。終端の証拠を待ちます。
明示停止・時計異常がある場合も、claimを解消してその停止理由を保持します。

## 確認と実行

独立して確認した終端注文証拠を`journal.reconcile`へ、完全な口座証拠と気配を
`journal.update_account`へ保存した後、次のローカルcontextを取得します。

```powershell
uv run python -m trading.private_order_recovery resolution-context --directory runs/account-live-orders --read-control-directory runs/account-read-control --scope operator-account --client-id Buy001
```

contextのSHA-256は、台帳・停止・旧許可・対象注文・POSTのclaim/世代/理由・履歴末尾・口座ゲート・
現在コードを含みます。確認後の変更は古い承認を無効にします。
`OrderResolutionApproval`には口座ID、contextのSHA-256、timezone付きの確認・失効日時、
本人性・業者条件・読取受入・口座開始条件・完全履歴の参照とSHA-256を各一件指定します。
有効期間は最大10分で、口座・気配の鮮度期限を延長しません。

```python
from pathlib import Path
from trading.live_journal import OrderResolutionApproval

context = journal.order_resolution_context(client_id)
approval = OrderResolutionApproval(
    account_id=context["account_id"],
    checkpoint_sha256=context["checkpoint_sha256"],
    accepted_at=accepted_at,
    expires_at=expires_at,
    evidence=verified_resolution_evidence,
)
Path("resolution-approval.json").write_text(approval.model_dump_json(), encoding="utf-8")
```

実際の証拠を確認した承認ファイルを指定し、以下を明示実行します。資格情報の読込とHTTPはありません。
承認ファイルは最大64,000 bytes、失敗時は固定理由だけを表示します。

```powershell
uv run python -m trading.private_order_recovery resolve --directory runs/account-live-orders --read-control-directory runs/account-read-control --scope operator-account --client-id Buy001 --approval resolution-approval.json --confirm terminal-order --confirm complete-history --confirm complete-account --confirm account-identity --confirm external-writers-paused --confirm old-clients-closed --confirm preserve-stops
```

ライブラリでは`journal.resolve_order_claim(client_id, approval, confirmations=...)`を使えます。
停止、旧発注許可、損失停止、口座証拠、注文・約定・取消受付、消費済み送信を保持します。
結果の`post_claim_resolved=true`はローカルclaimの解消を表します。実運用全体の完了ではありません。

## 保存境界と再起動

POSTの実OS所有者が不在であることを確認し、取得した所有権を最後まで保持します。
先に実口座台帳へ`ORDER_RESOLUTION_PREPARED`をcommitします。次に、台帳のtransactionを保持して
同じcheckpoint・承認・鮮度・コード・GET制御を再検査し、POST側へ`TRADE_RESOLVED`と解消前後の
状態、台帳側の確認記録ID・SHA-256を同時commitします。POSTのphaseは常に`STOPPED`です。
両保存物の参照、チェックサム、解消前後の状態遷移を再読込時にも検査します。

確認記録の保存後、POST commit前に終了すると元のclaimが残ります。新しいcontextで証拠を確認し、
改めて承認します。古い承認の自動再使用はできません。
POST commit後に終了した場合は、解消記録と停止が揃って残り、同じclaimを再度解消しません。
DB・履歴・所有権ファイルの置換やバックアップ巻戻しは復旧手続きに含みません。

解消commitはPOST世代を進めます。解消を実行したオブジェクトも、それ以前のクライアントも使用を拒否します。
元の制御と台帳を開き直して確認します。開き直してもPOST停止と台帳停止は解除されません。
claim解消後の再開は、別の[明示再開手続き](order-restart.md)で完全な注文・口座照合と
停止原因の確認、新しい発注許可を要求します。claimなしの後片付け失敗も、終端または完全に
照合した残注文があれば対象にできます。
取消の非終端・ID不明・未受付の確定は残工程です。

## 検証

追加56試験で、新規・建玉指定決済・取消、取消/失効/全量約定、部分約定の残高・建玉保持、
停止理由・旧許可・損失停止の保持、現在コードでの承認、旧世代の書込み拒否を確認しました。
準備commit後・POST commit前・POST commit後で実プロセスを終了させ、claimの保持または
確認記録付きの解消を検証しました。送信プロセスが実際に終了して残ったIN_FLIGHT claimも、
別に確立した完全な終端・口座証拠と明示確認でSTOPPEDへ解消しました。
中間と最後のcommitでの失効・コード変更・GET停止、参照履歴の欠落・改変、CLIの承認ファイルの
容量上限と失敗時の本文非表示も検査しています。
全体回帰2038件が合格し、最後のCLIエラー処理修正は関連89件と保存証拠欠損の追加1件で検証しました。
Ruffの検査・整形確認も合格しました。実口座への通信、資格情報の読込、Windowsタスク変更は行っていません。
有効注文の解消は`tests/test_order_resolution_active.py`で、未約定・一部約定の解消、確認項目の取り違え、
不完全な履歴・古い口座・状態と証拠の不一致、取消claimの拒否を検証します。
追加7試験を含む全2313テストが469.89秒で合格しました。Ruffの検査・整形確認、差分チェックも合格しました。
