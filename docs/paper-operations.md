# FX模擬観測の定期実行とWindows通知

更新日: 2026-10-03

`paper_runner` は初期化済みの探索的FX観測口座を運用するランナーです。
売買判断・数量・費用・スワップを扱う固定済みの観測コードを変更せず、実行管理を追加します。
研究上のcandidate、候補の凍結、forward OOSや実発注への昇格は行いません。

## 実行と記録

```powershell
uv run python -m trading.paper_runner step --directory runs/paper-sma-24-120-observation --policy configs/paper-runner.json
uv run python -m trading.paper_runner status --directory runs/paper-sma-24-120-observation
uv run python -m trading.paper_runner alerts --directory runs/paper-sma-24-120-observation
```

既定の周期は5分、1周期の観測処理は最大240秒、1ワーカーは最大180秒です。
通信タイムアウト・接続失敗・同時実行の競合だけを最大3回、5秒・10秒の待機を挟んで再試行します。
観測の設定・コード・実行環境・注文量・価格鮮度などの検証違反は再試行しません。
HTTPエラーや「業者エラーまたは予期しないschema」は原因を区別できないため自動再試行しません。
再試行はその時点の観測を新しく行い、取り逃した時刻の売買を再現しません。

`operations.sqlite` に周期の開始・終了、各試行、経過秒数、結果、通知を保存します。
開始記録を観測ワーカーの起動前にcommitするため、プロセス終了後も中断を検出できます。
OSロックで同じランナーの二重実行を拒否し、所有者が終了してから未完了周期を中断扱いにします。
`operations.lock` の存在だけを根拠に停止・復旧を判断せず、ロックファイルを削除しないでください。

ワーカーが時間切れになっても、模擬約定が保存済みの可能性は残ります。
結果を「約定なし」に補正せず、paper口座の重複防止に従って次の観測を行います。
新しい約定通知はpaperイベントの保存IDで追跡し、ワーカーの応答が失われても取り込みます。
既存口座にランナーを導入したとき、導入前の約定を新規通知として送り直しません。

Windowsの仮想環境用Python起動ファイルは、終了しても実体のPythonが残る場合があります。
ランナーとタスクは実体のPythonに現在の依存検索パスを渡して直接起動し、時間切れで実行中の
観測プロセスを終了します。Python環境を移動・更新した場合は、凍結された実行環境との整合を
確認してからタスクを再登録してください。

## 通知と停止検出

```powershell
uv run python -m trading.windows_tasks watchdog --directory runs/paper-sma-24-120-observation --policy configs/paper-runner.json
uv run python -m trading.paper_runner notify --directory runs/paper-sma-24-120-observation
```

独立したwatchdogは、15分を超える実行遅延・停止を検出し、Windowsに通知を提出します。
3周期連続の失敗、中断、新しい模擬約定、損失停止、障害からの復旧も通知対象です。
同じ連続失敗・遅延は復旧するまで1件にまとめます。通常休場・同一シグナルは通知しません。
通知機構は売買を実行せず、通知の失敗で模擬口座を初期化したり会計を修正したりしません。

Windows PowerShellの既存ショートカットのAppUserModelIDでローカルtoastを提出します。
実装は[Microsoftのデスクトップtoastの仕様](https://learn.microsoft.com/en-us/windows/win32/shell/enable-desktop-toast-with-appusermodelid)に従います。
通知の送信元表示はWindows PowerShellで、本文のタイトルはTrading Labです。
Windowsが通知を受け付けたことと、人が通知を読んだことは別です。
通知設定・集中モード・ログアウトなどでポップアップが見えないことがあります。

失敗した提出は記録を残し、5分以上空けて再試行します。提出済みのtoastは再送しません。
提出直後にプロセスが終了して再試行になった場合も、同じtagのtoastを置き換えます。
通知の確認済み処理は明示的に行います。確認済みにしても障害そのものは解消されません。

```powershell
uv run python -m trading.paper_runner ack --directory runs/paper-sma-24-120-observation --policy configs/paper-runner.json --alert-id 1
```

## Windowsタスクの登録

```powershell
uv sync --frozen --all-groups
powershell.exe -NoProfile -ExecutionPolicy Bypass -File scripts/install-paper-tasks.ps1 -Directory runs/paper-sma-24-120-observation -Policy configs/paper-runner.json -PlanOnly
powershell.exe -NoProfile -ExecutionPolicy Bypass -File scripts/install-paper-tasks.ps1 -Directory runs/paper-sma-24-120-observation -Policy configs/paper-runner.json
```

実行ポリシーの指定はこのPowerShellプロセスだけに適用し、システム設定を変更しません。
`-PlanOnly` は固定コード・設定・実行環境を検査し、登録する2タスクの定義だけを表示します。
PythonのASCII JSONをそのまま表示するため、日本語・空白を含むパスもコードページ932とUTF-8で保持します。
登録すると5分周期の観測タスクと独立したwatchdogタスクが作られ、ログオン時にも起動します。
現在のユーザーの対話セッションで、管理者権限・保存パスワードなしで動作します。
作業ディレクトリとPythonの絶対パスを記録し、同じタスクの多重起動は無視します。
同じ場所に`pythonw.exe`があるWindows環境では、タスクをコンソールなしで起動します。
観測ワーカーは終了検出と出力取得のため`python.exe`を使い、既存の非表示起動を維持します。
`pythonw.exe`がない環境では従来のインタープリターを使います。既存タスクへの適用は再登録時です。
別用途の同名タスクは上書きしません。既存のCodex等による観測スケジュールから切り替える場合は、
登録済みタスクの正常終了を確認した後に旧スケジュールを停止してください。

観測タスクだけが停止した場合はwatchdogが検出します。PC停止、ログアウト、Task Scheduler全体の
停止、両タスクの停止はこのPC内のwatchdogでは通知できません。外部の死活監視は別工程です。

## 未完了の範囲

API取得・判断・口座保存の個別処理時間は、既存観測本体を凍結しているため追加していません。
周期全体と各ワーカーの経過時間を記録します。研究と模擬運用の約定条件の一致、
候補昇格・forward OOSの合否条件、実口座接続の受入検証は別途必要です。
