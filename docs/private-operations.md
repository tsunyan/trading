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
uv run python -m trading.private_operations history --directory runs/private-sync --after-alert-id 0 --limit 100
uv run python -m trading.private_operations audit --directory runs/private-sync
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
通知IDは累積で増え、過去の本文と送信・既読・解消情報を自動削除しません。
`alerts`は履歴区間も含む未確認の最新100件を表示します。
`history`は既読済みも含めてID順に最大100件ずつ返します。次のページでは返された
`next_alert_id`を`--after-alert-id`に指定します。読取り・監査で通知や口座状態を変更しません。
DBは暗号化しておらず、ローカルの整合性検査は悪意ある書換えやバックアップ巻戻しを防ぐ仕組みではありません。

## 通知履歴を残した継続監視

2026-10-04、累積10,000件で監視が止まる上限を、通常処理に残す本文の上限へ変更しました。
本文が10,000件に達した時点で、移動可能な最大1,000件を検証し、同じSQLite内の履歴BLOBへ保存します。
対象は解消済みの障害通知と、既読・送信済みの解消通知またはテスト通知です。
送信待ちの通知と継続中の障害通知は移しません。継続中の障害は、既読・送信済みでも通常処理に残します。

本文・ID・元のSHA-256をそのまま保存し、区間のハッシュ連鎖と検索索引を照合します。
既読・解消・送信試行の情報は同じIDのまま保持するため、履歴へ移した後でも既読操作ができます。
本文の移動、索引追加、新しい通知、監視件数の更新は一つのトランザクションで行います。
途中終了後は移動前か移動完了後の状態となり、保存失敗で未送信通知を削除しません。
旧監視DBの読取りは元の正規化本文を保持し、履歴テーブルの追加は最初の移動時だけ行います。

起動・監視・通知時も全履歴BLOBを読み直してSHA-256を確認し、索引・通知IDの欠落や差替え、
送信情報の不正、時刻逆行を検出します。旧本文のJSON解析は繰り返しません。
`audit`は全履歴の本文も解析して、元の通知と索引の一致を検査します。
ハッシュを再計算した敵対的なSQL編集や巻き戻しを防ぐ仕組みではありません。

未処理通知だけで10,000件を占める場合は、新しい通知を作る監視試行が
`operations_alert_capacity`となり、前の監視条件と記録を維持します。
`watchdog`はこの場合も既存通知の送信を試み、`check_failed=true`と終了コード1を返します。
Windowsへの送信が回復すれば次の監視試行で移動できる通知ができ、監視を継続できます。
通知の送信失敗が続く場合は、通知先と未処理通知を確認してください。件数を減らすための自動既読は行いません。

[合成測定](../research/20261004-operations-archive-benchmark.json)では、累積50,000件・通常本文1,000件で、
再起動0.33秒、状態確認0.31秒、監視更新0.93秒、全本文の監査0.98秒、模擬通知送信0.64秒でした。
[測定スクリプト](../research/benchmark_operations_archive.py)は一時監視DBと合成READY観測だけを使い、
資格情報・HTTP・Windowsタスク・デスクトップ通知を使いません。
各サイズ一度の暖まったローカル読取りです。履歴の全量読取りと送信情報の検証は件数に比例し、
ディスク容量と大きな履歴での処理時間は引き続き監視が必要です。

## Windowsでの定期監視

監視DBを初期化した後、登録内容を確認してからスクリプトを実行します。

```powershell
uv run python -m trading.private_tasks plan --directory runs/private-sync
powershell.exe -NoProfile -ExecutionPolicy Bypass -File scripts/install-private-watchdog.ps1 -Directory runs/private-sync -PlanOnly
powershell.exe -NoProfile -ExecutionPolicy Bypass -File scripts/install-private-watchdog.ps1 -Directory runs/private-sync
```

例の実行ポリシー指定は起動したPowerShellプロセスにだけ適用し、ユーザー・システムの設定は変更しません。
リポジトリの`.venv`を先に用意してください。登録スクリプトはその環境から定義を生成し、
実体のPythonインタープリターと依存ライブラリの検索パスをアクションへ固定します。
空白・日本語を含むパスにも対応します。

登録するのは`TradingLab-Private-<同期識別子の先頭12文字>-Watchdog`の1タスクです。
`-PlanOnly`はPythonのASCII JSONをそのまま表示します。日本語・空白のパスをコードページ932とUTF-8で保持し、
登録・通信・保存状態の更新を行わないことを実PowerShellで検証しました。
ログイン時と既定60秒ごとに`private_operations watchdog`を実行します。
間隔は`-IntervalSeconds`で30秒から停滞閾値まで指定できます。
同時起動はIgnoreNew、実行上限は90秒です。ログイン中の同じユーザーで、管理者権限を要求せずに動きます。
同期の開始は[読取CLI](private-sync.md)から別に行います。タスクは監視・通知を担当します。
同じ場所の`pythonw.exe`が利用できるWindows環境では、監視タスクのコンソールを表示しません。
利用できない場合は従来のインタープリターを使います。既存タスクへの適用は再登録時です。

同じ名前のタスクを更新する際は、保存した監視識別子を含む説明とユーザーSIDを照合します。
無関係なタスクや別ユーザーのタスクは上書きしません。Windowsがユーザー名を別表記で保存しても、
同じユーザーなら更新できます。監視DBや固定設定が確認できない場合はタスク定義を生成しません。

登録した実際のタスク名を使って、実行・確認できます。

```powershell
Start-ScheduledTask -TaskName <task_name>
Get-ScheduledTask -TaskName <task_name> | Get-ScheduledTaskInfo
```

一時的な合成環境で登録・同一タスクの更新・実行を確認しました。
2026-10-03 16:38 JSTの実行は終了コード0で、監視時刻の保存・障害条件なしを確認し、
試験後にタスクを削除しました。実口座用のタスクは口座準備後に設置します。

障害条件または通知送信失敗がある場合、監視CLIは終了コード1を返します。
通知先はログイン中の同じWindowsユーザーのデスクトップです。
実口座・長時間の実REST/WebSocket受入は未実施です。

検証: `uv run pytest tests/test_private_operations.py tests/test_private_tasks.py tests/test_paper_runner.py -q`。
実プロセス終了とOS所有権解放、停止・停滞・確認不能、同じ通知の再試行、既読と停止の分離を
合成環境で検証しています。
一時的な合成環境からのPrivateテストtoastは実Windowsでも受け付け1件・失敗0件を確認しました。
履歴保存は`tests/test_operations_archive.py`で、1万件超の継続、未送信・継続中の障害の保持、
後日の既読、履歴ページ、破損、移動失敗、commit前後の実プロセス終了を検証します。
