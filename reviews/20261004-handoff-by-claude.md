# 2026-10-04 Claudeによる引き継ぎメモ（Codexの作業の続き）

- ブランチ: `feat/fx-practical-operations`
- 範囲: `47f9272`（Codexの最終コミット）以降。Codexが作業途中だった発注用キーの保管と確認済み送信を
  引き継いで仕上げ、その後USD/JPYの実口座運用に必要な機能を機能単位でコミットしました（約45件の機能・修正と文書・試験）。
- 最終確認: 全2488テスト合格（`OPENBLAS_NUM_THREADS=1`）、Ruffの検査・整形確認、差分チェック合格。

## 追加した主な機能（文書へのリンクは`docs/`）

| 分類 | 内容 | 文書 |
| --- | --- | --- |
| 送信 | 発注用キーの別名前空間保管、表示した実行内容のSHA-256を要求する送信、`context --fetch-quote`、送信ログ | order-runtime.md |
| 気配・ルール | 公開tickerの確認用気配、業者の取引ルールと注文上限の照合 | live-quote.md、live-rules.md |
| 口座・注文 | 読取結果からの口座証拠更新（建玉時の評価額許容幅つき）、受付済み注文のGET照合 | live-account.md、live-order-sync.md |
| 復旧 | 有効なまま残る新規・決済の結果不明claim解消（`active-order`確認）、有効注文一覧からのID発見、停止中の無登録台帳の移行 | order-resolution.md、order-discovery.md、live-operations.md |
| 戦略 | 提案（`--flatten`、`--units auto`、市場閉鎖・損失停止・未決済で見送り）、凍結候補の昇格と合否条件の事前固定・判定 | live-signal.md、promotion.md |
| 運用 | 台帳CLI（作成・準備・破棄・有効化・状態・停止・バックアップ）、送信直前までのサイクル、毎時タスクと通知、状態診断と変化通知、損益レポート（決済の成績・月次・約定CSV・スリッページ）、状態ページ、同期の正常終了からの続行 | live-setup.md、live-cycle.md、live-tasks.md、live-doctor.md、live-report.md、live-dashboard.md、private-sync.md |
| 受入 | 読取の証拠・資料の指紋から有効化／再開／claim解消／取消の承認ファイル作成、小額の受入試行の手順 | live-acceptance.md、live-first-trial.md |

全体の早見表は`docs/live-commands.md`です。

## 設計上の判断

- **POSTは人の確認つきのまま**: 注文・取消のPOSTは`order_runtime submit/cancel`だけで、表示した実行内容の
  SHA-256・発注用キーの参照・`--order-permission-confirmed`が必要です。定期タスクは準備も送信もしません。
  自動送信は`docs/live-autonomy-proposal.md`に設計案として残し、実装していません。
- **完全性は明示確認でのみ宣言**: 口座読取（`complete-account`・`account-identity`・`external-writers-paused`）と
  注文の約定履歴（`complete-history`）は、運用者の確認があるときだけ完全として台帳の照合へ渡します。
  読取結果からの自動昇格はしません。台帳の照合・停止条件は既存のままです。
- **台帳の実装ハッシュ**: `CODE_FILES`に`live_account.py`・`live_order_sync.py`（完全性を宣言する経路）を追加しました。
  `live_signal.py`・`live_cycle.py`・`promotion.py`は提案側なので対象外です。
- **評価額の許容幅**: 建玉がある間、業者の評価時刻とtickerの時刻の差で照合が毎回失敗するため、
  `--valuation-tolerance`を明示した場合だけ、業者値との差が範囲内なら気配で評価し直します（余力は増やさない）。
  既定は従来どおり拒否です。業者の評価方法は実口座で要確認です。
- **移行**: 登録前に有効化した台帳は停止中だけ登録でき、旧許可は`LIVE_APPROVAL_VOIDED`へ移し、
  新しい承認での明示再開を必須にしました（台帳の「許可の指紋＝状態の指紋」の不変条件を守るため）。
- **実験台帳**: 昇格・合否条件は追加の表で、スキーマ版は変えていません。読取（`status`・`check`・実発注側の確認）は
  表を作らず、存在しないパスも作りません。実際の`runs/ledger.sqlite`で読取が台帳を変えないことを確認済みです。
  合否条件を登録した仮説では、`judge`が記録した`advance`だけが`live`昇格の根拠になります。

## 実データで見つけて直した点

- 週末は最後の足が金曜で古く、提案が毎時`stale_signal_data`で失敗していた → 市場閉鎖を足の鮮度より先に判定。
- 足の取得が固定10日で、SMA 24/120の120本が連休で足りなくなる恐れ → `warmup_bars`＋7日（最大30日）。
- 公開APIの実データ（ticker・symbols・klines）で取得と解析を確認。USD_JPYのルールは最小100・刻み1・呼値0.001でした。

## 環境上の注意

- このPCはコミット可能メモリがほぼ枯渇しており（ゲームクライアントが約14GB）、子プロセスでnumpy（OpenBLAS）が
  起動できず、PowerShell・子プロセスを使う試験が失敗することがあります。`OPENBLAS_NUM_THREADS=1`で全体試験が通ります。
- 全体試験の実行中に`CODE_FILES`のモジュールを編集すると、実装ハッシュが途中で変わり一時的な失敗が出ます。
- このPCで動いているペーパー運用のタスクは、作業中も正常終了を確認しました（最終確認16:12）。

## 未完了・判断待ち

- 実口座での受入（口座未準備）。手順と合格条件は`docs/live-first-trial.md`。
- 自動送信の実装可否と上限値（`docs/live-autonomy-proposal.md`）。
- 取消の結果不明で注文が有効なまま残る場合の解消、送信不到達（不在）の証明は未実装（設計上、時間経過では判断しない）。
- `reviews/20261003-by-claude.md`は未追跡のまま残しています（このセッションでは変更していません）。
