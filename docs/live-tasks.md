# 運用サイクルの定期実行と通知

2026-10-04。[運用サイクル](live-cycle.md)を毎時の足確定後に自動で実行し、注文の提案や失敗を
Windows通知で知らせるタスク計画と登録スクリプトを追加しました。タスクは注文を準備も送信もしません。
通知を見た運用者が、サイクルを`--prepare`付きで実行し、表示されたSHA-256で送信します。

```powershell
.\scripts\install-live-cycle.ps1 -Directory runs\live-orders -ReadControlDirectory runs\account-read-control -Scope <scope> -CredentialReference <read_only_reference> -Config configs\fx.toml -Units 1000 -MaxSlippage 0.02 -QuoteOutput runs\live-orders\quote.json -ResultOutput runs\live-orders\cycle.json -ValuationTolerance 0.05 -Ledger runs\ledger.sqlite -Hypothesis H001 -Attestation runs\live-orders\attestation.json -PlanOnly
```

`-PlanOnly`を外すと、現在のユーザーのタスクとして登録します。登録はこのコマンドを運用者が
実行した場合だけで、ほかのコマンドがタスクを作ることはありません。

## 確認の宣言ファイル

```powershell
uv run python -m trading.live_attestation attest --directory runs/live-orders --read-control-directory runs/account-read-control --scope <scope> --hours 24 --confirm complete-account --confirm account-identity --confirm external-writers-paused --confirm complete-history --output runs/live-orders/attestation.json
uv run python -m trading.live_attestation status --directory runs/live-orders --read-control-directory runs/account-read-control --scope <scope> --output runs/live-orders/attestation.json
```

口座全体の取得・約定履歴の完全性・口座本人性・外部操作の停止は、ソフトウェアが観測できない
業者口座の状態です。以前は計画の作成時の`--confirm`をタスクの引数に永久に含めていましたが、
登録時の宣言が以後のすべての実行に効き続けるため、期限付きのファイルに改めました（2026-10-04のレビュー対応）。

- 宣言は台帳（`live_instance`）に結び付き、別の台帳では使えません。
- 有効期間は1〜72時間です。同じパスに書き直すと更新になります。
- 4つの確認がすべて揃わない宣言は作れません。ファイルを編集して期間を延ばした場合や形式が違う場合は拒否します。
- 宣言は運用者の申告であり、内容が正しいことを証明するものではありません。状況が変わったら
  ファイルを削除してください。次の実行から失敗して止まります。

## タスクの内容

- 実行: 毎時1分（次の正時の1分から1時間ごと）。前回の実行中は重ねて起動しません。上限は5分です。
- コマンド: `live_cycle`を`--prepare`・`--flatten`なしで、`--notify`と`--result-output`付きで実行します。
- 確認項目: 4つの確認はタスクの引数に含めません。運用者が期限付きの宣言ファイル（`-Attestation`）を
  作り、毎時の実行はそれが有効な間だけ使います。期限が切れると、サイクルは何も読まずに
  `attestation_expired`で失敗し（失敗通知は理由が変わるまで1回）、更新するまで止まります。
  期限の24時間前に「定期実行の確認宣言が24時間以内に失効します」と1回通知します。
  計画の作成時にも宣言が有効であることを確かめます。
- 戦略: `-Ledger`・`-Hypothesis`で[実運用に昇格した凍結候補](promotion.md)の指定が必須です。
- 入力の検査: 毎時の実行で必ず拒否される値は、計画の作成時に拒否します。`-MaxSlippage`は正の値、
  `-ValuationTolerance`は0より大きく1以下、固定の`-Units`は台帳の最小・最大と単位の倍数に限ります。
- 対象: 同期・監視を登録した台帳だけを計画できます。タスク名は台帳の識別子から作り、
  同じ名前の別のタスクや別ユーザーのタスクは上書きしません。
- 鍵: 読取専用キーの参照だけを使います。発注用キーは使いません。

## 通知と結果ファイル

戦略が新規・決済を提案した場合は「戦略が実発注の注文を提案しました（未送信）」、
失敗した場合は「実発注の運用サイクルが失敗しました（未送信）」を通知します。見送りでは通知しません。
同じ提案が続く間（送信しないまま次の足になった場合など）は最初の1回だけ通知し、
提案が変わるか見送りを挟むと再び通知します。同じ提案とは、顧客注文IDと指値・上限価格を除く注文条件
（売買・新規/決済・数量・注文タイプ・決済する建玉）と保有状態がすべて同じものです。数量が変われば再通知します。
価格は毎時の気配に合わせて動くため比べず、送信時の実行内容で確認します。

結果ファイル（`--result-output`）は、履歴・状態ページの書込みの後に最後に書きます。履歴や状態ページの
書込みに失敗した場合は、結果ファイルの`history_written`・`dashboard_written`が`false`になります。
通知の失敗はサイクルの結果を変えません。
受付済み注文の照合で約定・取消・失効が確定すると、注文ごとに1回「実発注の注文の結果を照合しました」を通知します。
同じ理由の失敗が続く間は最初の1回だけ通知し、理由が変わると再び通知します（通知自体に失敗した場合は次回に再試行）。
準備済みで未送信の注文が残っていると以後の提案が見送られ続けるため、その注文について1回だけ
「準備済みで未送信の実発注注文があり、以後の提案を止めています」を通知します。

発注の許可（最大7日）の失効まで24時間を切ると、「実発注の許可が24時間以内に失効します」を
許可ごとに1回だけ通知します。通知済みの印（`approval_notice_for`）を結果ファイルに残し、毎時の
実行で引き継ぎます。許可を更新して失効日時が変われば、新しい許可について再び通知します。

各回の結果は`-ResultOutput`のファイルに置き換え書込みします。成功時はサイクルの出力に`ok: true`、
失敗時は`ok: false`と固定理由コードを保存し、通知の成否を`notified`に記録します。
タスクはpythonw.exeで実行するため、画面への出力は残りません。
`-HistoryOutput`（`--history-output`）を指定すると、各回の結果を1行のJSONとして追記します。
過去の判断・失敗・通知を後から追えます。
`-DashboardOutput`を指定すると、毎回[状態ページ](live-dashboard.md)を更新します。1回あたり数KBで、1年でおよそ数十MBになります。

## 状態診断タスク（任意）

`-DoctorIntervalSeconds`（300〜3600）を指定すると、`TradingLab-Live-<台帳識別子>-Doctor`も登録します。
指定間隔で[送信前の状態診断](live-doctor.md)を結果ファイルとともに実行し、送信を止めている条件の組が
変わった時だけ「実発注の送信を止めている条件があります」「…解消しました」を通知します。準備待ちの
注文（`order_queue`）は通知しません。前回の組は結果ファイルと同じ場所の`doctor-state.json`に保存し、
通知に失敗した場合は次回に再通知します。毎時のサイクルが2時間以上止まると`scheduled_cycle`で通知されます。

## 提案を受け取った後

提案は手順2の気配で作られています。送信時の鮮度（既定60秒）を過ぎているため、通知を見たら
`live_cycle --prepare`を改めて実行し、表示された実行内容を確認して`order_runtime submit`で送信します。

## 検証

`tests/test_live_tasks.py`で、登録済み台帳からの計画（`--prepare`なし、通知・確認・許容幅を含む、
空白を含む設定パス）、確認不足・不正な参照・数量・小数の拒否、未登録台帳の拒否、
提案だけを通知して結果を書き出すこと、失敗時の通知・結果・終了コード（通知の失敗を含む）、
登録スクリプトの`-PlanOnly`の往復を検証します。タスクの登録と実際の通知は行いません。
追加11試験を含む全2380テストのうち2378件が563.49秒で合格しました。残る2件はPC全体のコミット可能メモリ不足で子プロセスのnumpy（OpenBLAS）が起動できなかったもので、`OPENBLAS_NUM_THREADS=1`で関連24試験を再実行して合格しました。
