# Private同期の監視とWindows障害通知

2026-10-03。`python -m trading.private_operations`は、[読取CLI](private-sync.md)の保存状態を
独立したプロセスから監視します。Windowsのデスクトップ通知は既存のtoast処理を使います。
資格情報の読込・HTTP・購読・発注・計上・停止解除は行いません。

## 初期化と監視

```powershell
uv run python -m trading.private_operations init --directory runs/private-sync
uv run python -m trading.private_operations watchdog --directory runs/private-sync
uv run python -m trading.private_operations status --directory runs/private-sync
uv run python -m trading.private_operations alerts --directory runs/private-sync
```

既存の同期環境に`private-operations/operations.sqlite`とOSロックを作ります。
同期制御の識別子・固定設定のハッシュ・保存場所と紐付け、既存の口座・ジャーナル・設定は変更しません。
再初期化・欠落したDBやロックの再作成は拒否します。

既定は照合成功の停滞120秒、通知失敗の再試行300秒です。初期化時だけ
`--stale-seconds`と`--retry-seconds`で固定できます。停滞の閾値は同期間隔と取得期限の合計以上が
必要です。既存監視の設定変更・自動移行はありません。
`check`は監視記録だけを更新し、`watchdog`はその後に通知の送信を試みます。

監視する障害条件:

- 永続STOPPED。
- RUNNINGが残り、実際のOS所有権を取得できたことによる旧所有者不在。
- RUNNING中に成功回数が増えず、閾値を超えた状態。再試行回数が増えても解消しません。
- GET制御の停止、現金台帳の停止。
- 停止中または所有者不在で残る開いた受信区間・ACK不明・拒否記録。
- 保存状態の欠落・破損・紐付け変更などによる確認不能。

新しいRUNNING世代を初めて観測した時点から開始猶予を設けます。停滞は監視開始後の成功回数の
増加で判断し、発見までに閾値と次の監視周期がかかります。正常なREADYは待機状態として扱います。
通常のGET実行中や、稼働中の短い通知保存とACKの間は障害通知しません。
OSロックファイルの存在だけではプロセスの生存・死亡を判断しません。

保存状態は複数DBの診断用観測です。正常な同時更新では最大3回読み直します。
確認不能になった場合は以前の障害条件を保持し、復旧済みとは推測しません。
監視結果は口座の完全性や売買許可には変換しません。

## 通知と確認

```powershell
uv run python -m trading.private_operations test --directory runs/private-sync
uv run python -m trading.private_operations notify --directory runs/private-sync
uv run python -m trading.private_operations ack --directory runs/private-sync --alert-id 1
```

`test`は通信・売買をせずにテスト通知を保存し、Windowsへの送信を試みます。
障害が継続する間は同じ条件を繰り返し追加しません。条件が解消すると解消通知を追加し、
以前の未送信の障害通知は送信対象から外します。再発した場合は新しい通知を作ります。

送信試行を先に保存し、失敗しても同期状態は変更しません。Windowsが受け付けた後にプロセスが
終了すると再送する場合がありますが、同じtagで同じtoastを置き換えます。受け付けは人が見た証拠ではありません。
通知本文は固定した条件名・同期識別子・通知IDだけを使い、キー、残高、通知原文、例外本文は載せません。

既読操作は通知だけを変更し、障害条件・同期制御・GET制御を解除しません。
停止の原因を確認し、必要なら[停止中の照合と明示復旧](private-sync.md)を別に行ってください。
通知記録は最大10,000件で自動削除しません。`alerts`は未確認の最新100件を表示します。
DBは暗号化しておらず、ローカルの整合性検査は悪意ある書換えやバックアップ巻戻しを防ぐ仕組みではありません。

障害条件または通知送信失敗がある場合、監視CLIは終了コード1を返します。
通知先はログイン中の同じWindowsユーザーのデスクトップです。
実口座・長時間の実REST/WebSocket受入は未実施です。

検証: `uv run pytest tests/test_private_operations.py tests/test_paper_runner.py -q`。
実プロセス終了とOS所有権解放、停止・停滞・確認不能、同じ通知の再試行、既読と停止の分離を
合成環境で検証しています。
一時的な合成環境からのPrivateテストtoastは実Windowsでも受け付け1件・失敗0件を確認しました。
