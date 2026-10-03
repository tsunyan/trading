# 結果不明注文のGET照合

2026-10-04。`private_order_recovery`は、専用の実口座台帳で送信claimを消費した注文を、
業者の注文IDを指定してGETで調べるCLIです。別手続きの[終端結果のclaim解消](order-resolution.md)も
追加しました。GET調査はPOST制御が停止していても、元のGET制御が
使用可能なら実行できます。資格情報はWindowsストアの既存の読取専用参照を明示します。
実口座・資格情報は未準備のため、検証は一時DB・模擬HTTP・実プロセスの途中終了で行いました。

## 業者仕様と確認できる範囲

[GMO公式の注文情報取得](https://api.coin.z.com/fxdocs/#orders)は`orderId`または`rootOrderId`を
要求します。`clientOrderId`で検索するパラメーターはありません。顧客注文IDは設定時に応答へ返ります。
[有効注文一覧](https://api.coin.z.com/fxdocs/#active-orders)の空結果から、受付されなかったと
判定することもできません。注文IDは受付記録や業者画面などから取得し、運用者が指定します。
IDが分からない場合の自動発見と、不在・拒否の証明は未実装です。

注文と約定を2回ずつ取得し、対象ID・顧客注文ID・銘柄・売買・新規/決済・注文タイプ・数量・
価格・決済建玉・約定内容・応答鮮度と、2回の内容一致を既存のReaderで検査します。
以前に保存した受付や約定との矛盾も拒否します。HTTP・APIエラー、取得漏れ、欠落約定、
取得中の変化は成功記録を残しません。
結果不明の取消も調べられます。元の注文IDを指定し、取消claimの要求ハッシュと、
取消を開始した時点の注文証拠を履歴から検証します。後続のGETで約定が増えても調査を継続できます。
contextの`post_operation`が`cancel`となり、取消の再送やclaim解放は行いません。

一致しても`executions_complete=false`を維持します。部分約定、全量約定、取消、失効の
いずれでも、今回のGETだけで履歴完全性や全口座会計を証明したとは扱いません。
注文台帳は`RECONCILING`に保持し、POST claim・要求SHA-256・停止理由・送信済み履歴・
許可・リスク停止を保持します。送信、取消、再接続、claim解放、停止解除、再承認は実行しません。
`private_sync run`の停止時拒否も継続します。結果の`recovery_required=true`は残工程を示します。

## 手順

既存の実口座台帳、そこに永久登録されたPOST制御とGET制御を使用します。
保存先の新規作成・別制御への差替えは行いません。次は構文例で、実保存先の設置ではありません。

```powershell
uv run python -m trading.private_order_recovery context `
  --directory D:\Trading\live `
  --read-control-directory D:\Trading\reads --scope my-account `
  --client-id Buy001
```

このローカル確認は資格情報を読み込まず、通信しません。消費済みSUBMITTING、準備履歴、
注文内容とPOST操作・要求SHA-256の一致を検査し、`checkpoint_sha256`を返します。
ハッシュは台帳の識別子・保存許可・revision・注文状態・末尾・停止状態と、POSTの識別子・
revision・claim・要求・停止理由を含みます。

```powershell
uv run python -m trading.private_order_recovery reconcile `
  --directory D:\Trading\live `
  --read-control-directory D:\Trading\reads --scope my-account `
  --client-id Buy001 --order-id 201 `
  --expected-sha256 $checkpointSha256 `
  --credential-reference $credentialReference --read-only-confirmed
```

`$checkpointSha256`にcontextで確認したSHA-256、`$credentialReference`に既存の読取専用参照IDを
設定して実行します。POSTのOS所有者が不在であることを確認し、取得から保存まで同じOSロックを保持します。
他の送信・通常の台帳変更は競合を拒否します。緊急停止は引き続き保存でき、取得中に確認対象が
変わると今回の証拠を反映しません。GET制御が停止中・claim未完了なら資格情報の読込前に拒否します。
取得は全体30秒と各HTTPの期限を検査します。応答しない通信は、戻るまで所有権を保持します。

成功時は停止、正規化した注文・約定、GETの要求と応答時刻・SHA-256を同じSQLite transactionで
保存します。生の応答やキーは保存しません。送信プロセスが停止処理前に終了していた場合も、
この保存で実口座台帳を停止します。既存POST DBは変更しません。
保存前の終了では全変更がrollbackされ、保存後の終了では証拠と停止が揃って残ります。
再調査は新しいcontextを確認して行い、同じ約定IDを増やしません。現金計上はこのCLIの対象外です。
コード更新後も照合できますが、既存の発注許可を更新しません。

## 残工程

注文ID不明の場合の調査、業者の拒否応答を使う確定判定、完全履歴の確認、非終端結果のclaim解放、
同期カタログ・独立監視への接続、実口座受入が残ります。
[停止後の明示再開](order-restart.md)は別手続きで、未解決claimがないことと完全な注文・口座照合、
停止原因の確認、新しい発注許可を要求します。
[取消HTTPと停止を保持した対象限定の取消許可](live-cancel.md)はライブラリで使用できます。
DBのチェックサムは悪意ある編集やバックアップ巻戻しの防御ではありません。

## 検証

33件の追加試験で、業者状態5種類、部分約定の保持、対象不在・内容矛盾・認証失敗・期限超過・
後片付け失敗の拒否、チェックポイント変更、OS所有権、コード更新後の照合、保存前後の
実プロセス終了を確認しました。全1882テストが合格した後、コード更新と過去約定欠落の
追加2件を含む照合33件も合格しました。実口座への通信とネイティブ資格情報アクセスは使っていません。
