# Windows資格情報マネージャーによる明示的なキー保管

2026-09-30。`trading.credential_store` を追加しました。
**今回、実際のWindows資格情報の読込・書込・列挙・削除や、実口座通信は行っていません。**
Windows API境界は模擬実装で検証しています。実Windowsストアでの保存・復元確認は別途必要です。

## 設計

- Windowsの汎用資格情報を使用します。保存先は現在のWindowsユーザー・同じPCに限定する設定です。
  ソースコード、環境変数、SQLite、transcript、平文ファイルへのキー保存はありません。
  他OSやWindowsストアへのアクセス失敗時に、平文保存へ切り替える機能はありません。
- 保存するたびにランダムな参照IDを生成し、専用の
  `TradingLab/GMOFX/ReadOnly/v1/<参照ID>` に格納します。
  指定された参照IDだけを読み、資格情報の列挙や「最新キー」の自動選択はしません。
- 更新時も新しい参照IDになります。旧キーは上書き・削除せず、既存クライアントも変更しません。
  新しい参照IDを明示して新しいクライアントを作る必要があります。
  旧キーの業者側失効とWindows側の削除は別作業です。
- 保存データにはAPIキー・秘密鍵のほか、参照ID、ローカルscope、通信制御DBの識別子、
  読み取り専用権限を設定したという運用者の申告を含めます。
  **この申告は業者側権限や口座本人性の検証ではありません。**
- 元の通信制御DBに紐付けます。別のDBへの差替えや、停止・未完了claimがある状態では読み込みません。
  停止中でも新しいキーの保管自体はできますが、使用は拒否され、停止状態は解除されません。
- キー・認証ヘッダー・OSエラー本文は表示しません。
  秘密を含む可能性がある不正CLI引数も、エラー時に再表示しません。

Windows実装は `CREDENTIALW`、`CredReadW`、`CredWriteW`、`CredFree` を使います。
型はGENERIC、永続化はLOCAL_MACHINE、最大blobは2,560バイトです。
DLLはSystem32から読み込み、ネイティブの一時バッファは使用後にゼロ化・解放します。
公式仕様: [CREDENTIALW](https://learn.microsoft.com/en-us/windows/win32/api/wincred/ns-wincred-credentialw)、
[CredReadW](https://learn.microsoft.com/en-us/windows/win32/api/wincred/nf-wincred-credreadw)、
[CredWriteW](https://learn.microsoft.com/en-us/windows/win32/api/wincred/nf-wincred-credwritew)。

## 将来の手動操作

口座がない現在、キーを用意・入力する必要はありません。
以下は、口座開設後に読み取り専用権限のAPIキーを作成してから運用者が使う手順です。
**`save` は実際のWindowsストアへ書き込み、`check` は指定キーを実際に読み込みます。**
どちらもネットワーク接続は行いません。

既存の通信制御ディレクトリ・scopeを指定してください。停止を回避するための再初期化は禁止です。

```powershell
uv run python -m trading.credential_store save --directory runs/account-read-control --scope operator-account --read-only-confirmed
uv run python -m trading.credential_store check --directory runs/account-read-control --scope operator-account --reference <保存時に返った参照ID>
```

`save` は対話端末でAPIキーと秘密鍵を非表示入力します。
CLI引数、パイプ、チャット、設定ファイルへキーを貼り付けないでください。
端末でエコーを無効化できない場合は中止します。非対話の自動投入には対応しません。
戻り値の参照IDはキーそのものではありませんが、運用情報として管理してください。
`check` の成功は保存データを復元できた意味で、認証成功・権限確認・残高照合を意味しません。

## 明示的なGETクライアント生成

```python
from pathlib import Path
from trading.credential_store import CredentialVault
from trading.read_control import PersistentReadLimiter


def build_reader(reference):
    control = PersistentReadLimiter(Path("runs/account-read-control"), "operator-account")
    return CredentialVault().open_client(control, reference)
```

この関数呼出は指定資格情報を読みますが、生成だけではHTTP通信しません。
返されたクライアントで `get` を呼ぶと認証付きGETが送られます。使用後は必ずcloseしてください。
読み込んだ後に停止が発生した場合も、実際のGET時の永続制御で新しいclaimを拒否します。
自動実行、ペーパー運用、注文台帳にはこの経路を組み込んでいません。

## 限界と残項目

- Windowsのログオンユーザーの権限が安全性の前提です。同じユーザー権限の悪意あるプログラム、
  管理者、デバッガー、メモリダンプから秘密を完全に隔離するものではありません。
- Pythonが生成した文字列・JSON・bytes等のコピーを完全にゼロ化する保証はありません。
  ローカル変数付き例外表示や任意の通信フックを本番で有効にしないでください。
- ランダム参照IDと既存確認で通常の上書きを避けますが、WindowsのCredWrite自体には
  作成時のcompare-and-swap保証がありません。同じ対象への外部書込み・改ざんは防げません。
- 制御DBを丸ごとコピー・差し戻しする攻撃を防ぐものではなく、紐付けはローカル整合性です。
- キー保管の模擬テストは済みですが、実Windowsストアでの受入確認とキー失効手順は未完了です。
- [一部停止の手動復旧](read-recovery.md)と、version 3の[claim解消](read-orphan.md)は実装済みです。旧版claimの移行、
  口座本人性、変更イベントの実受信・履歴再同期、実口座での会計検証、
  実注文送信と戦略の実運用への昇格判断は引き続き別工程です。
  [変更通知とRESTの構造照合](account-sync.md)はオフラインで追加しています。

テスト: `uv run pytest tests/test_credential_store.py -q`
テストでは実Windows DLLアクセスとネットワーク接続を禁止し、ダミーキーだけを使っています。
