# Private POSTの永続制御

2026-10-03。`PersistentPostLimiter`は、同じローカル口座のトークン操作と発注送信で共有する
通信枠です。[GMO公式仕様](https://api.coin.z.com/fxdocs/)のPrivate REST POST上限は口座ごとに毎秒1回です。
実口座への発注・発注許可・口座本人性の確認は、この制御では行いません。

## 初期化と利用

同じscopeの既存[GET制御](read-control.md)を指定し、未使用のディレクトリに一度だけ作ります。

```powershell
uv run python -m trading.post_control init --directory runs/account-post-control --read-control-directory runs/account-read-control --scope operator-account
uv run python -m trading.post_control status --directory runs/account-post-control --read-control-directory runs/account-read-control --scope operator-account
uv run python -m trading.post_control stop --directory runs/account-post-control --read-control-directory runs/account-read-control --scope operator-account
```

これらはキーを読み込まず、通信しません。GET制御にはPOST制御の識別子と絶対パスを一度だけ登録します。
停止・削除・正常終了後も別ディレクトリへの差替えを拒否します。紐付け済みの保存物が欠落・破損した場合、
再作成や旧制御への自動切替はありません。初期化中の中断で未登録の保存物が残った場合も自動採用しません。
同じトランザクションで登録内容のハッシュをGET履歴へ保存し、登録表の欠落や書換えを拒否します。

[読取CLI](private-sync.md)は、この紐付けがあると共通の制御でトークン取得・更新・削除・購読操作を行います。
凍結したSyncPlanの項目やハッシュは変更しません。以前の未紐付け環境は従来のプロセス内制御を使います。
永続制御が停止・未完了なら、新しい読取実行はキー読込前に拒否します。
`private_sync status`の`posts`に状態を表示します。

Pythonでは、同じGET制御とPOST保存先で`PersistentPostLimiter`を再度開きます。
`PrivateTokenClient`へ既存の`limiter`引数として渡せます。[専用注文送信API](live-orders.md)では
`operation("order"|"close_order"|"cancel", request_sha256=...)`で要求の識別子も保存します。
ハッシュには秘密情報や認証ヘッダーを含めない運用です。新規・建玉指定決済のHTTPに接続済みです。
一つのPOST制御へ実口座台帳を一度だけ登録し、checksummed登録表とEXECUTION_BOUND履歴を保存します。
削除・別保存先による差替えや同じオブジェクトを使った別スレッドの所有権代用を拒否します。
この制御の`live_enabled=false`は通信枠自体が発注許可を与えないという意味です。
発注許可は専用台帳の受入・有効化・口座リスクで別途確認します。

## 処理と停止

| 状態 | 動作 |
| --- | --- |
| READY | 安定したOSロックを取得し、IN_FLIGHTとclaimをcommitしてから通信枠へ入る |
| IN_FLIGHT | 同じ所有者による処理を除き、追加の枠取得を拒否 |
| STOPPED | 新しい枠取得を拒否。例外や値戻りで解除しない |

所有権取得後に、単調時計で必ず1.1秒待ちます。プロセス再起動や壁時計の前進でも間隔を飛ばせません。
OSロックは待機・処理・完了commitの全体で保持します。ネットワーク中にSQLiteのトランザクションは保持しないため、
別の監視処理や明示停止は保存できます。正常完了のcommit後だけclaimを解放します。
途中終了・例外・割込み・時計異常・待機異常・保存失敗では、claimを自動的に解放しません。
OS所有者が消えても未完了記録は残り、時間経過だけでは復旧しません。
処理中の明示停止は正常完了でも保持します。
注文HTTPクライアントの終了処理が失敗した場合は`order_cleanup_failed`を記録します。
先に保存した結果不明・明示停止・時計異常などの停止理由とclaimを上書きせず、受付済みの記録も保持します。
この停止はトークン限定復旧の対象にはなりません。
送信しない`check()`は、別プロセスがOSロックを保持している処理中には継続できます。
所有者不在の未完了記録は拒否し、検査中に完了した場合は保存状態を再確認します。
新しい注文枠の取得は同時所有を拒否します。トークン操作は最大3秒だけ同じOSロックを待ちます。
待機中の停止・時計異常・所有者不在の未完了claimは送信を拒否します。取得できた場合も1.1秒の間隔を守ります。
更新が枠を取得できなければ、トークンを失効させずに次の受信周期へ回します。旧期限を過ぎれば受信を終了します。
取得・削除の枠競合はその操作を失敗にしますが、他の所有者の注文をoperator_stopとして停止しません。

保存先をコピーしたり、ロックファイルを作り直して所有権を代用することはできません。
DBは既存ファイルとして開き、保存状態のチェックサム・最終遷移・ロックファイルの実体・GETとの紐付けを検査します。
これは同じローカル保存先を使うクライアントの制御です。別PC、別GET制御、手動操作、他アプリの通信は制御しません。
チェックサムは署名やバックアップ巻戻しの防止ではありません。

## 検証と残工程

33件の合成試験で、再起動・壁時計前進時の間隔、タイマーがわずかに早く戻った場合の追加待機、同時所有権の拒否、処理中の停止、
時計・待機異常、保存物の欠落・置換・破損、完了保存失敗、初期化競合、トークンHTTP失敗を確認しました。
実プロセスの終了をclaim後の待機前、完了commit前、処理中で試験し、未完了記録の保持を確認しました。
別プロセスの処理中検査と、所有者が消えた後の拒否、検査中に完了した場合の再検査も確認しました。
実際のTokenClient・Reader・Supervisorを通す合成運転と凍結設定の保持も確認しました。

追加の29試験と専用注文台帳との接続2試験で、トークン枠競合、待機中の失効、503・タイムアウト、
3種類の操作、実プロセスのclaim後・復旧commit前の終了、旧クライアントの拒否を確認しました。

## トークンだけの明示復旧

新しいトークン操作は`token_acquire`、`token_renew`、`token_delete`を別々に保存します。
その結果不明と、処理後の`token_failed`だけを`recover-token`で解消できます。自動再試行はしません。
旧形式の`private_stream`は購読等と区別できないため、この復旧では解消しません。
注文・決済・取消のclaim、運用者の停止、時計異常も対象外です。

1. 旧クライアントを終了し、最後の処理からトークンが失効するまで待ちます。
2. `status`のrevision・reason・claimを控えます。保存した最終時刻から3660秒以上の待機を要求します。
   60分の業者期限に、待機・通信・時計差の上限分を加えています。
3. 以下を明示実行します。これはローカル記録の確認だけで、資格情報の読込・HTTPはありません。

```powershell
uv run python -m trading.post_control recover-token --directory runs/account-post-control --read-control-directory runs/account-read-control --scope operator-account --expected-revision 3 --expected-reason operation_unknown --expected-claim REPLACE_WITH_SAVED_CLAIM --confirm token-only --confirm old-clients-closed --confirm expiry-waited
```

例のrevisionとclaimは`status`の実値へ置き換えます。claimがnullの`token_failed`では`--expected-claim`を省きます。
OS所有者が存在すれば拒否し、状態が変わっていれば再確認を要求します。解除とTOKEN_RECOVERED履歴は同時commitです。
復旧前の全POSTオブジェクトは使用を拒否します。制御を再度開き、新しいクライアントを作ります。
GET・同期・実口座台帳の紐付け、注文状態、許可期限、別途保存した停止は変更しません。
同期制御が停止していれば、その復旧を別に実施する必要があります。ACK不明・FAULT区間は[確認の記録](private-sync.md#受渡し結果不明fault区間の確認)と計上の後に復旧します。
待機の確認項目とscopeは運用者の申告です。壁時計の前進や別PCの操作を検証する仕組みではありません。

[終端結果を確認した注文claimの解消](order-resolution.md)を追加しました。専用実口座台帳の
完全な終端証拠・全口座照合・運用者確認を条件に、解消前後の状態と台帳側の確認記録を保存します。
POSTを`STOPPED`に保持し、旧クライアントを世代で拒否します。トークン限定復旧とは別経路です。
[停止後の明示再開](order-restart.md)は、未解決claimがない状態で完全な注文・口座照合と
停止原因の確認、新しい発注許可を要求します。再開前後の状態と台帳の準備参照を保存し、
POST世代を進めて旧クライアントを拒否します。POSTだけ再開して途中終了しても台帳の停止は維持します。
claimなしの後片付け失敗も検証済み注文なら対象にできます。`token_failed`は先にトークン限定復旧が必要です。
非終端の結果不明・ID不明・未受付の判定、旧形式の未完了処理、実口座受入は残工程です。

2026-10-04追記: 資格情報からの送信は[確認済み送信](order-runtime.md)、口座証拠と注文状態の更新は[口座証拠の更新](live-account.md)・[受付済み注文のGET照合](live-order-sync.md)、有効なまま残る新規・決済のclaim解消とID発見は[claim解消](order-resolution.md)・[ID発見](order-discovery.md)、停止中の無登録台帳は[明示移行](live-operations.md)、戦略との接続は[提案](live-signal.md)と[昇格管理](promotion.md)で追加しました。現在の残工程は[ロードマップ](roadmap.md)を参照してください。
