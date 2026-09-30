# 読み取り通信の永続停止・プロセス間制御

2026-09-30。`PersistentReadLimiter` を追加しました。
同じローカルDBを使うプロセス間でGETを直列化し、停止を再起動後も保持します。
**ネットワーク接続、認証情報の読込、実注文、自動復旧は行いません。**

## 実装した安全動作

- 専用ディレクトリ内の `read-control.sqlite` を使用。ペーパーDB／注文DBは変更しません。
- SQLiteの排他トランザクションでclaimを取得し、通信を始める前に確定します。
  通信中はDBトランザクションを保持しないため、別プロセスから停止を記録できます。
- 新規version 3では、claim前から通信終了の記録まで同一ファイルのOS排他ロックも保持します。
  ロックファイルの欠損・差替えを検出すると拒否し、自動再作成しません。
- 同時にclaimできるのは1件だけ。他プロセスの実行中は待機して横取りせず、
  `read_claim_unresolved` として収集全体を拒否します。
- claim後に毎回250ms以上、単調増加時計で待機します。
  別プロセスへの切替や壁時計の早送りで通信間隔が短縮されない構造です。
- 通信終了を記録して初めてclaimを解放します。正常終了・通常の例外を区別して監査します。
- 強制終了や電源断で終了処理が走らなければclaimが残り、再起動後も拒否します。
  PID確認、経過時間、リース期限だけでclaimを自動解放する機能はありません。
- HTTP 401/403/429による既存クライアントの停止、手動停止、時計逆行、
  KeyboardInterrupt/SystemExitによる中断を永続化します。
- DB欠損・破損、スコープ不一致、実行中のDB識別子変更、claim不一致は安全側に拒否します。
  既存DBを開く処理は `mode=rw` のため、見つからないDBを勝手に再作成しません。
- DBにはローカルスコープ名、識別子、時刻、固定イベント種別、claim識別子、
  version 3ではロックファイルのdevice/inodeも保存します。
  APIキー・秘密鍵・生エラー・口座残高・注文内容は保存しません。

## 操作（すべてローカルのみ）

初期化は未使用ディレクトリに一度だけ行います。
`scope` は運用者が管理するローカルラベルで、実口座の本人性を証明するものではありません。

```powershell
uv run python -m trading.read_control init --directory runs/read-control-demo --scope synthetic-demo
uv run python -m trading.read_control status --directory runs/read-control-demo --scope synthetic-demo
uv run python -m trading.read_control stop --directory runs/read-control-demo --scope synthetic-demo
```

`stop` は永続的に停止します。無条件の`reset`／`resume` コマンドはありません。
未完了claimのない一部の停止に限り、[運用者確認付き復旧](read-recovery.md)を追加しました。
version 3のクラッシュ後claimには、停止を維持する[OSロック付き手動解消](read-orphan.md)もあります。
停止を回避する目的でディレクトリやscopeを変えて初期化し直さないでください。
処理中のDBをコピー・削除・差し戻し・復元してはいけません。

ステータスの `stopped=true` は永続停止、`in_flight=true` は未完了claimです。
どちらかがtrueなら `blocked=true` になります。クラッシュ後は
`stopped=false, in_flight=true` の場合もありますが、再通信は許可されません。

## GET専用クライアントへの接続

実口座へのGETを行うアプリケーションを将来作る際には、
既存のメモリ内 `AccountReadLimiter` の代わりにこれを渡します。
すべてのキー・プロセスで同じローカルディレクトリとscopeを使ってください。

```python
from pathlib import Path
from trading.read_control import PersistentReadLimiter


limiter = PersistentReadLimiter(Path("runs/account-read-control"), "operator-account-label")
# PrivateReadClient(api_key, secret, limiter=limiter) に明示的に渡す。
```

これは参照用コードで、今回のCLIやペーパー自動実行へ実通信を接続していません。
GETクライアントとの結合はダミーキーと模擬HTTPで検証しています。

## 限界と次の工程

- 停止は新しいclaimを防ぐもので、claim済みの通信を強制中断しません。
  待機終了時にも停止を再検査しますが、その検査後に別プロセスが停止した場合、
  claim済みの処理はまだ未送信でもGETを送信し得ます。原子的なネットワーク取消ではありません。
- 通常の通信例外は `FAILED` と記録してclaimを解放します。自動再試行はありません。
  認証エラー等で停止した場合は、終了処理が成功しても停止フラグを解除しません。
- 同じローカルファイルシステム上の同じDBを使う協調プロセスだけを制御します。
  別PC、別DB、他アプリ、業者側の手動操作は対象外。ネットワーク共有にDBを置かないでください。
- SQLiteのロック・正常なストレージ動作が前提です。ファイルを編集できる相手への改ざん防止や、
  古いバックアップへの差し戻し検出を保証しません。
- [運用者確認付き復旧](read-recovery.md)と、version 3向けの[未完了claimの手動解消](read-orphan.md)は実装済みです。
  旧版claimの解消・移行、ロックファイル欠損からの復旧は未対応です。
  復旧提案は期限・状態に紐付き、承認後も古いクライアントは拒否します。自動復旧はありません。
  [Windowsキー保管](credential-store.md)は模擬検証まで追加しました。
  [変更通知との構造照合・再取得](account-sync.md)はオフラインで追加しましたが、
  実ストア受入確認、口座本人性、変更イベントの実受信・履歴再同期、実口座検証は残っています。

別プロセスでの同時実行と `os._exit` によるクラッシュを含めてテストしています。
この制御は注文発注の許可でも、口座スナップショットの完全性証明でもありません。
