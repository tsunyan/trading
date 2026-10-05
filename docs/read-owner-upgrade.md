# 停止済みで未完了GETのない旧制御を現形式へ移す

2026-10-05。version 1/2のGET制御を、元のディレクトリ・識別子・scope・POST/同期紐付けを保ったまま、
OS所有権を使うversion 3へ明示移行できます。停止と停止理由を保持し、GETの再開は別の
[運用者確認付き復旧](read-recovery.md)で行います。資格情報・HTTP・実注文・タスクの操作はありません。

## 対象

元のGET制御が停止済みで、`in_flight`が空であることが必要です。claimの履歴がある場合は、
`CLAIMED`と同じtokenの完了・失敗が交互に記録され、未完了がないことを検査します。
`claim_mismatch`、未完了claim、所有権ファイルや`owner_file`表が既にある旧制御は拒否します。
version 3で所有権ファイルが失われた場合にも使用できません。欠損・差替えを新しいロックで迂回しません。

旧GETはOSロックを保持していなかったため、旧claimの所有者不在をこの移行で証明できません。
未完了の旧claimは引き続き別の復旧工程です。運用者は関連ワーカーと自動再起動を止め、
旧クライアントをすべて終了してください。異なるPC・ディレクトリ・他アプリは制御対象外です。

## 提案と承認

```powershell
uv run python -m trading.read_owner_upgrade status --directory runs/account-read-control --scope operator-account
uv run python -m trading.read_owner_upgrade prepare --directory runs/account-read-control --scope operator-account
```

`status`は元のDBを変更しません。現在のコードが通常の起動時に後付けする索引も、
移行の工程では作りません。調査用に採取したSHA-256と`status`後のDBは一致します。
`prepare`は所有権ファイルを作りません。元の制御状態・監査末尾・保存されたPOST/同期紐付けを含む
期限付き提案をDBへ記録します。出力された`proposal`・`revision`を確認し、5分未満に対話端末で承認します。
状態、紐付け、提案が変わった場合や時計異常・期限切れは拒否し、停止を維持します。
提案と保存済み判断はUTF-8で64KB以下に限ります。保存済み判断はSQLでサイズ・型を確認してから読み、
巨大データや形式不整合があればGET・復旧を拒否します。

```powershell
uv run python -m trading.read_owner_upgrade approve --directory runs/account-read-control --scope operator-account --proposal <proposal> --revision <revision> --confirm-cause --confirm-clock --confirm-workers-paused --confirm-old-clients-closed --confirm-get-only --confirm-preserve-stops
```

端末で`UPGRADE IDLE GET OWNER`と入力します。確認項目は、原因調査・時計確認・ワーカー停止・
旧クライアント終了・GETだけの移行・停止維持です。これは運用者の申告で、本人性や業者側の処理終了の証明ではありません。

移行では先に`OWNER_UPGRADE_STARTED`と元の状態をDBへcommitします。この時点から現在のコードの
GET・停止解除を拒否します。次に元のディレクトリで`read-owner.lock`を排他的に新規作成し、fsyncします。
状態と履歴を再検査した上で、ファイルのdevice/inode・version 3・`OWNER_UPGRADED`を同時commitします。
旧オブジェクトは移行開始と完了の両方で世代が変わるため、使用を拒否します。

制御を開き直しても停止は残ります。以後のGETと現形式のclaim解消は同じOSロックを使います。
POST制御・同期制御・口座・注文は変更しません。同期の旧紐付けに`STREAM_BOUND`がない場合も、
移行だけでは補完せず、元の[紐付け確認](private-sync.md)を別途行います。
このコード変更は実発注側の実装SHA-256も変えるため、実発注の再開には現在のコードでの承認が必要です。

## 途中終了

移行開始前の終了では元の旧制御と停止が残ります。提案を確認し直してください。
開始commit後、完了commit前は`owner_upgrade_incomplete=true`でGET・通常復旧を拒否します。
緊急の`read_control stop`は引き続き使えます。停止済みの制御への停止は`STOP_*`の記録だけを追加し、
保存済みの判断の完了を妨げません。停止は完了後も保持されます。状態・停止理由・紐付けが変わった場合や、
停止以外の監査記録が入った場合は、元の承認では完了しません。

所有権ファイルがまだ存在しない場合に限り、保存済みの移行判断を明示的に完了できます。
`status`の`intent_sha256`を確認し、次を対話端末で実行して`FINISH RECORDED GET UPGRADE`と入力します。

```powershell
uv run python -m trading.read_owner_upgrade complete --directory runs/account-read-control --scope operator-account --intent-sha256 <saved_intent_sha256> --confirm-finish-recorded-upgrade --confirm-old-clients-closed --confirm-get-only --confirm-preserve-stops
```

commit済みの判断の完了なので、元の提案が失効していても扱えます。新しいGET承認や停止解除ではありません。
開始後に停止以外の監査記録が入った場合や、`claim_mismatch`など停止理由が変わった場合は拒否します。
ファイル作成後に終了した場合は、未記録のファイルを自動採用・削除・再作成しません。
元の保存物を保全し、別の調査・復旧が必要です。この境界の修復と、旧形式の未完了claim解消は未提供です。
完了commit後の終了ではversion 3と停止が揃って残り、元の制御を開き直して確認します。

## 検証

一時DBと実OSファイル・別プロセスを使い、旧版・停止理由の保持、別途GET復旧、所有権の競合、
POST/同期紐付けの保持、未完了・履歴不整合・既存所有物の拒否、期限・時計・状態変更、
保存失敗・三つの途中終了境界・保存済み判断の完了・監査改変を検証します。
索引のない旧DBで`status`が全バイトを変えないこと、開始後の停止が完了を妨げず停止を保持することも検証します。
既存の運用DBの移行や実口座への接続は行っていません。
