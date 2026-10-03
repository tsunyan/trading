# Private POSTの永続制御

2026-10-03。`PersistentPostLimiter`は、同じローカル口座のトークン操作と将来の発注送信で共有する
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
`PrivateTokenClient`へ既存の`limiter`引数として渡せます。将来の注文送信では
`operation("order"|"close_order"|"cancel", request_sha256=...)`で要求の識別子も保存します。
ハッシュには秘密情報や認証ヘッダーを含めない運用です。現段階では注文HTTPクライアントは未接続です。

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
送信しない`check()`は、別プロセスがOSロックを保持している処理中には継続できます。
所有者不在の未完了記録は拒否し、検査中に完了した場合は保存状態を再確認します。
新しい枠取得は同時所有を拒否します。枠競合の待機・調停は今後の発注経路との接続で扱います。

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

結果不明の注文・取消・トークン操作は種類ごとに必要な証拠が違います。GET用の孤立claim解消を流用せず、
送信結果を照合した後に行う明示復旧を追加する工程が残っています。
現段階では復旧・強制解除コマンドはありません。実口座の受入も未実施です。
