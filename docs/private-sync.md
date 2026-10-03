# 明示開始する実口座読取CLI

2026-10-03。`python -m trading.private_sync`は、Windows資格情報ストア、永続GET制御、
口座Reader、Private受信・継続同期を組み合わせます。起動・正常終了・認証失敗・通知と約定計上・
応答しないREST処理・復旧を合成通信で検証しています。実キー・実口座での受入は未実施です。

`init`・`status`・`recover`・`init-orders`・`register-order`はキーを読み込まず、業者へ通信しません。`run`だけが明示指定した
Windows資格情報を読み、GET口座取得とWebSocket用のトークン操作・購読を行います。
注文送信や戦略判断はありません。Windowsのタスク登録・障害通知への接続は次の工程です。

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

`--duration-seconds`は1〜604,800秒で、購読開始後からの実行上限です。
時間到達またはCtrl+C/SIGTERMで終了処理に入ります。受信約定・ACK・後片付けが解決している正常終了は
READYへ戻ります。問題が残る場合はSTOPPEDを保存します。OSによる強制終了はRUNNINGを残し、
次の開始で明示復旧を要求します。終わらないRESTワーカーの所有権は保持し、強制終了・置換しません。
HTTPクライアントも処理終了後に閉じ、終了待機中のワーカーを閉じるためにCLIの呼出側をブロックしません。
非daemonワーカーが残る場合、プロセス終了自体はその終了を待ちます。

不明な注文IDは注文HTTP取得の前に拒否し、推測した意図で計上しません。
後から確認した注文は追記登録できます。未知注文で停止した後、保存済みの未計上約定を
REST証拠と照合して計上する運用手順は残っています。登録だけでは停止を解除しません。
CLIの引数・設定・通信例外の生の内容はエラーへ再表示しません。

## 復旧

```powershell
uv run python -m trading.private_sync recover --directory runs/private-sync --expected-plan-sha256 <plan_sha256> --expected-revision <revision> --expected-head <head> --expected-reason <reason> --acknowledge-token-uncertainty
```

[StreamControlの復旧条件](stream-control.md)を満たす場合だけ、世代を進めます。
トークン不明の扱いを確認した場合にのみ最後のフラグを使います。
GET制御の停止・claim不明はこのコマンドでは解除しません。[GET復旧](read-recovery.md)・
[claim解消](read-orphan.md)を別に確認し、その後に新しい読取クライアントで開始します。
ACK不明・未計上約定・FAULT区間の強制解除はありません。

口座の全履歴、外部操作、業者の丸め・証拠金式、接続間の履歴欠損は引き続き未確認です。
CLI成功も`complete=false`、`live_enabled=false`で、実注文や戦略の昇格へ変換しません。
実Windowsストア・実REST/WebSocketの受入、全口座照合、Windows通知、実発注は残工程です。

検証: `uv run pytest tests/test_private_sync.py tests/test_known_orders.py -q`。ネイティブ資格情報APIと実ソケットを禁止し、
境界だけを合成通信へ置き換えて、実際のGETクライアント・Reader・TokenClient・Supervisorを通します。
