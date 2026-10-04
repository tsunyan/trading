# 明示開始する実口座読取CLI

2026-10-03。`python -m trading.private_sync`は、Windows資格情報ストア、永続GET制御、
口座Reader、Private受信・継続同期を組み合わせます。起動・正常終了・認証失敗・通知と約定計上・
応答しないREST処理・復旧を合成通信で検証しています。実キー・実口座での受入は未実施です。

`init`・`status`・`recover`・`init-orders`・`register-order`・`register-live-orders`はキーを読み込まず、業者へ通信しません。
`run`は明示指定したWindows資格情報を読み、GET口座取得とWebSocket用のトークン操作・購読を行います。
`reconcile-stopped`は保存済み約定について既知注文のGET照合・計上を行います。
注文送信や戦略判断はありません。[独立監視とWindows障害通知](private-operations.md)を利用できます。
Windowsへの監視タスク登録手順も同文書に記載しています。
[永続POST制御](post-control.md)をGET制御に紐付けた環境では、トークン操作を別プロセスとも共有します。
`status`の`posts`で確認でき、停止・未完了の制御ではキーを読む前に新しい実行を拒否します。
`post_owner_present`は処理中のPOSTの実OS所有権を検査します。専用の実発注台帳が紐付いていれば、
`live`に元の紐付けと停止・許可有効性・損失停止の小さな診断を返します。残高・証跡参照は返しません。
独立した`private_operations watchdog`は同期やPOSTの中断を再確認して、紐付いた実発注台帳を停止します。
`status`や診断用`check`だけでは台帳を停止しません。停止解除や口座完全性への昇格も行いません。

## 事前に用意するもの

- 同じローカルscopeの[永続GET制御](read-control.md)。既存DBを指定し、停止・claim不明を先に確認します。
- [Windows資格情報](credential-store.md)の参照ID。キーを引数や設定へ記載しません。
- 開始残高・cutoffを明示して作った[現金台帳](execution-cash-book.md)。実口座の値を別途確認します。
  開始建玉や入出金の条件を使う場合も、この台帳に明示します。
- 既知注文の業者IDと`OrderIntent`の対応。資料と照合した内容を指定し、通知から推測しません。

現金台帳・GET制御のディレクトリを再作成して停止を回避しないでください。
`scope`と資格情報の紐付けはローカル整合性で、業者側の口座本人性・権限を証明しません。

## 設定と初期化

外部のJSON設定例です。`read_control_instance`はGET制御の`status`が返す`instance_id`、
`credential_reference`は資格情報を保存した際の参照IDに置き換えます。
この2項目の例示値は実行用ではありません。相対パスは設定ファイルの場所から解決します。

```json
{
  "version": 1,
  "scope": "operator-account",
  "read_control_directory": "../runs/account-read-control",
  "read_control_instance": "00000000000000000000000000000000",
  "cash_directory": "../runs/account-cash",
  "credential_reference": "00000000000000000000000000000000",
  "known_orders": [],
  "max_records": 1024,
  "read_timeout_seconds": 5,
  "collection_limit_seconds": 30,
  "supervisor": {
    "sync_interval_seconds": 15,
    "sync_timeout_seconds": 35
  }
}
```

既知注文を指定する場合の`known_orders`の1要素は次の形です。`intent`は読取・比較用の宣言で、
登録しても発注しません。ID・client_idの重複は拒否します。

```json
{
  "order_id": 201,
  "intent": {
    "client_id": "PreviouslyConfirmed",
    "symbol": "USD_JPY",
    "side": "BUY",
    "effect": "OPEN",
    "units": 1000,
    "kind": "LIMIT",
    "price": "150"
  }
}
```

```powershell
uv run python -m trading.private_sync init --directory runs/private-sync --plan configs/private-sync.local.json
uv run python -m trading.private_sync status --directory runs/private-sync
```

未使用ディレクトリへ`sync-plan.json`、`journal/`、`control/`、`catalog/known-orders.sqlite`を作ります。
解決後の設定とハッシュを保存し、現金台帳・GET制御の識別子と同期制御を紐付けます。
既存ディレクトリへの再初期化は拒否します。途中で初期化が失敗した場合は、部分的な保存物を
検査のため残すことがあります。既存DBや不明ファイルを自動削除しません。
ハッシュは偶発的な変更の検査であり、署名や外部の改ざん防止ではありません。
保存した設定は固定し、後から確認した注文は別の追記専用カタログへ登録します。
GET制御DBには同期制御の識別子を一度だけ登録します。同じGET制御から別ディレクトリに同期環境を
作る操作は、正常終了後も拒否します。停止・旧所有者を新しいディレクトリで迂回できません。
紐付けの自動解除・差替えはありません。別GET制御・別PC・他アプリの通信を制御するものではありません。

旧形式のGET紐付けに`STREAM_BOUND`履歴がない場合は、通常の`status`や`run`を拒否します。
元の保存済み同期環境を照合してから、`confirm-read-binding`で同じ紐付けの履歴だけを補完します。
通常の`status`が使えない場合も、ローカルの`PrivateSyncWorkspace(directory)`を開き、
`plan_sha256`、`control.snapshot()`のrevisionとinstance、`journal.head()`を確認できます。
同期設定・現金台帳・通知ジャーナル・同期制御の紐付けはこの開き直しでも検証します。

```powershell
uv run python -m trading.private_sync confirm-read-binding --directory runs/private-usdjpy --expected-plan-sha256 <saved_plan_sha256> --expected-revision <saved_revision> --expected-head <journal_head> --legacy-binding-confirmed
```

同期とGETのOS所有権、元の設定SHA-256・revision・ジャーナル末尾・GET制御の識別子を確認します。
RUNNINGの残存記録、未完了GET claim、紐付けの欠落・別識別子を拒否します。
元の表と同じ識別子に履歴を追加するだけで、設定・注文・停止・復旧世代を変えません。
STOPPEDの場合も停止は残り、補完だけでは同期を再開できません。資格情報・HTTPは使いません。

## 既知注文の追記登録

`status`の`plan_sha256`と`catalog.head`を確認し、上の1注文分のJSONを別ファイルへ保存します。
業者の資料と注文意図を照合した後で、次のコマンドを実行します。`source-ref`には確認資料の
参照名を指定し、キーや資料本文を記載しません。

```powershell
uv run python -m trading.private_sync register-order --directory runs/private-sync --order-file configs/known-order.local.json --expected-plan-sha256 <plan_sha256> --expected-catalog-head <catalog.head> --source-ref broker-export-reviewed --intent-confirmed
```

登録は稼働中・停止中のどちらでも可能です。次の注文取得から参照し、固定設定と同期の停止状態は
変更しません。同じ業者注文IDに異なる意図を登録する操作や、別注文へのclient_idの再使用は拒否します。
同一内容の再登録は件数を増やしません。末尾が変わった場合は`status`で確認し直してください。
上限は10,000注文、1宣言4,096バイト、宣言本文合計32MBです。

カタログはscope・同期制御・固定設定と紐付け、読込時に全宣言の連鎖を検査します。
欠落・破損を自動再作成しません。ハッシュは悪意ある書換えやバックアップ巻戻しを防ぐ仕組みではありません。
登録はローカルの宣言で、業者側の本人性・注文の正しさを証明せず、計上・発注・復旧も実行しません。

旧形式の環境では、同期プロセスを終了し、`status`のrevisionと末尾を確認して明示的に移行します。

```powershell
uv run python -m trading.private_sync init-orders --directory runs/private-sync --expected-plan-sha256 <plan_sha256> --expected-revision <revision> --expected-head <head>
```

OS所有権が使用中なら拒否します。元の既知注文だけを種として保存し、固定設定のハッシュと停止状態は
維持します。カタログ作成後、manifest保存前に中断した場合は同じカタログを検査して移行を完了します。
新形式で紐付け済みのカタログが欠落した場合、旧環境として作り直すことはありません。

同じGET・POST制御に専用発注台帳が紐付いている場合は、[発注台帳からの注文登録](live-order-catalog.md)を
同期開始前と各注文取得前に自動使用します。保存済み受付・照合から業者IDと元の注文意図を検査し、
稼働中に追加された注文も追跡します。`register-live-orders`で通信なしの登録もできます。
取得後に受付・過去約定との一致も検査し、矛盾や同期失敗では紐付いた発注台帳を停止します。
発注台帳の状態・口座リスク証拠は自動更新せず、停止解除や発注許可へ変換しません。

## 実行と終了

`status`の`plan_sha256`、`control.revision`、`journal.head`を確認して指定します。
下の山括弧付き項目は実際の値に置き換えてください。

```powershell
uv run python -m trading.private_sync run --directory runs/private-sync --expected-plan-sha256 <plan_sha256> --expected-revision <revision> --expected-head <head> --duration-seconds 3600 --read-only-confirmed
```

開始前の状態・末尾が変わっている場合や、停止した制御DBでは、キーの読込前に拒否します。
取得はGET専用クライアントを通り、トークン操作は固定ws-authだけです。
口座取得とそれに続く既知注文の取得には合計30秒の予算を設け、各GETの前後で確認します。
個々の通信待機は5秒です。Supervisorも35秒の期限を検査し、遅い結果を受理しません。

開始建玉を宣言した台帳では、空の開始建玉も含めて、現在の注文拘束を毎回照合します。
予備の口座取得で稼働中の注文IDを列挙し、全IDの登録済み意図を確認してから個々の注文と約定を取得し、
最後に口座を新しく取得します。予備取得は注文を探すために使い、最終口座報告の代わりにはしません。
これらと通知約定の注文取得は同じ30秒予算を共有し、GET制御の待機時間も含みます。
拘束用の注文取得は最大1,000件です。未知注文は個別注文GETの前に拒否します。
予備取得後に現れた注文、拘束数量の不一致、未計上のREST約定も永続停止します。
部分約定は今回の通知照合で計上した後に残りの拘束を比較し、RESTにあるだけの約定を自動計上しません。
開始建玉の宣言がない旧台帳では、この取得を省略し、建玉の未確認条件を残します。
凍結した設定の項目・ハッシュは変更しません。業者の評価・証拠金式は実確認が必要なため、
CLIが推測した式で評価診断を有効化することはありません。

正常終了（READY）からの続行は`continue`でも行えます。現在の固定設定のハッシュ・制御のrevision・
受信記録の末尾を保存済みの状態から読み、`run`と同じ検査で開始します。READY以外の状態や、
期待値の引数を指定した場合は拒否します。STOPPEDや強制終了後のRUNNINGは引き続き明示復旧が必要です。

```powershell
uv run python -m trading.private_sync continue --directory runs/private-sync --duration-seconds 86400 --read-only-confirmed
```

`--duration-seconds`は1〜604,800秒で、購読開始後からの実行上限です。
時間到達またはCtrl+C/SIGTERMで終了処理に入ります。受信約定・ACK・後片付けが解決している正常終了は
READYへ戻ります。問題が残る場合はSTOPPEDを保存します。OSによる強制終了はRUNNINGを残し、
次の開始で明示復旧を要求します。終わらないRESTワーカーの所有権は保持し、強制終了・置換しません。
HTTPクライアントも処理終了後に閉じ、終了待機中のワーカーを閉じるためにCLIの呼出側をブロックしません。
非daemonワーカーが残る場合、プロセス終了自体はその終了を待ちます。

不明な注文IDは注文HTTP取得の前に拒否し、推測した意図で計上しません。
後から確認した注文は追記登録できます。未知注文で停止した後は、次の照合・計上手順を
使います。登録だけでは停止を解除しません。
CLIの引数・設定・通信例外の生の内容はエラーへ再表示しません。同期・制御・Supervisorの固定理由コード
（例: `sync_checkpoint_changed`）だけを`reason=`として表示し、停止原因を区別できるようにします。

## 停止中の約定照合・計上

同期プロセスを終了し、`status`の設定ハッシュ・revision・末尾・停止理由を確認します。
未知注文があれば資料と照合して先に登録します。GET制御が停止・claim不明の場合は、
[GET復旧](read-recovery.md)・[claim解消](read-orphan.md)を別に確認してください。

```powershell
uv run python -m trading.private_sync reconcile-stopped --directory runs/private-sync --expected-plan-sha256 <plan_sha256> --expected-revision <revision> --expected-head <head> --expected-reason <reason> --read-only-confirmed
```

STOPPEDまたは旧所有者不在のRUNNINGだけを受理します。OS所有権をネットワーク操作と計上が終わるまで
保持し、稼働中・応答しない旧処理が所有していれば拒否します。現在区間に保存した約定通知を取得し、
各注文の`orders`・`executions`を2回ずつGETします。通常の取得と同じ通信待機・全体取得予算を使います。
RESTから見つけただけの約定は追加計上しません。重複通知は受信時刻が異なっていても各内容を照合し、
一致したexecution IDを一度だけ計上します。上限は重複を含む2,000通知・1,000注文です。

設定・制御状態・ジャーナル末尾が取得中に変わった場合、計上を拒否します。計上中はジャーナル変更を
トランザクションで防ぎます。現金台帳とは別DBのため両者のcommitは一体ではありませんが、計上commit直後に
終了した場合も保存済み証拠を使って再実行でき、二重計上しません。
ACK不明・FAULT/REJECTEDで終了した区間・停止した現金台帳は自動解消しません。
約定通知がなければキーの読込・GET・計上は行いません。

成功後も同期状態・ジャーナルは維持し、`recovery_required=true`を返します。
接続間に失った履歴や口座全体の一致を証明する処理ではありません。

## 復旧

```powershell
uv run python -m trading.private_sync recover --directory runs/private-sync --expected-plan-sha256 <plan_sha256> --expected-revision <revision> --expected-head <head> --expected-reason <reason> --acknowledge-token-uncertainty
```

業者の定期メンテナンスなどで接続が切れ、同期がSTOPPEDになった場合も、`continue`や監視タスクは
自動で再開しません。停止理由を`status`で確認し、この手順で復旧します。実発注の送信は、新しい世代で
照合が成功し監視が確認するまで止まります（[送信前の検査](live-operations.md)）。

[StreamControlの復旧条件](stream-control.md)を満たす場合だけ、世代を進めます。
トークン不明の扱いを確認した場合にのみ最後のフラグを使います。
GET制御の停止・claim不明はこのコマンドでは解除しません。[GET復旧](read-recovery.md)・
[claim解消](read-orphan.md)を別に確認し、その後に新しい読取クライアントで開始します。
未計上約定は上の手順で一致を確認して計上した後に復旧します。
ACK不明・未計上約定・FAULT区間の強制解除はありません。

口座の全履歴、外部操作、業者の丸め・証拠金式、接続間の履歴欠損は引き続き未確認です。
CLI成功も`complete=false`、`live_enabled=false`で、実注文や戦略の昇格へ変換しません。
実Windowsストア・実REST/WebSocketの受入、全口座照合、実口座用の監視設置、実発注は残工程です。

検証: `uv run pytest tests/test_private_sync.py tests/test_known_orders.py tests/test_stopped_reconciliation.py -q`。ネイティブ資格情報APIと実ソケットを禁止し、
境界だけを合成通信へ置き換えて、実際のGETクライアント・Reader・TokenClient・Supervisorを通します。
停止中の照合は23件で、重複通知・内容矛盾・末尾競合・応答しないGET・計上commit前後の
実プロセス終了と、再実行による二重計上防止を検証しています。
