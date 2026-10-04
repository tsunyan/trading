# 実発注まわりのコマンド早見表

2026-10-04。USD/JPYの実口座運用で使うCLIの一覧です。手順は[手順書](live-setup.md)、最初の実口座試行は
[小額の受入試行](live-first-trial.md)を参照してください。「鍵」はWindows資格情報ストアのどのキーを読むか、
「通信」は業者へのHTTPの有無です。

| コマンド | 用途 | 鍵 | 通信 | 台帳の変更 |
| --- | --- | --- | --- | --- |
| `trading.live_rules check` | 注文上限と業者の取引ルールの照合（[説明](live-rules.md)） | なし | 公開GET | なし |
| `trading.live_setup create/prepare/abandon/activation-context/activate/status/stop/backup` | 台帳の作成・注文の準備と破棄・有効化・状態・停止・監査用の複製（[説明](live-setup.md)） | なし | なし | あり |
| `trading.private_order_operations context/bind` | 同期・監視の登録、停止中の台帳の移行（[説明](live-operations.md)） | なし | なし | あり |
| `trading.private_sync run/continue/status` | 継続同期の実行と正常終了からの続行（[説明](private-sync.md)） | 読取専用 | GET・WebSocket・通知用トークンのPOST/PUT/DELETE | 同期側 |
| `trading.private_operations watchdog` | 独立監視と台帳の停止・通知（[説明](private-operations.md)） | なし | なし | 停止のみ |
| `trading.order_credentials binding/save` | 発注用キーの保存（[説明](order-runtime.md)） | 保存のみ | なし | なし |
| `trading.live_quote` | 確認用気配の取得（[説明](live-quote.md)） | なし | 公開GET | なし |
| `trading.live_account` | 口座証拠の更新（[説明](live-account.md)） | 読取専用 | GET | 口座証拠 |
| `trading.live_signal` | 戦略の提案・手仕舞いの提案（[説明](live-signal.md)） | なし | 公開GET（足） | なし |
| `trading.live_cycle` | 照合・気配・口座証拠・提案・準備をまとめて実行（[説明](live-cycle.md)） | 読取専用 | GET・公開GET | あり（送信なし） |
| `trading.order_runtime context/submit/cancel-context/cancel` | 実行内容の確認と送信（[説明](order-runtime.md)） | 発注用 | POST | あり |
| `trading.live_order_sync` | 受付済み注文のGET照合（[説明](live-order-sync.md)） | 読取専用 | GET | 注文状態 |
| `trading.private_order_recovery context/reconcile/resolution-context/resolve` | 結果不明の調査とclaim解消（[説明](order-recovery.md)、[解消](order-resolution.md)） | 読取専用 | GET | あり |
| `trading.order_discovery` | 結果不明注文の業者ID発見（[説明](order-discovery.md)） | 読取専用 | GET | なし |
| `trading.private_order_restart context/restart` | 停止後の明示再開（[説明](order-restart.md)） | なし | なし | あり |
| `trading.live_acceptance read-evidence/file-evidence/approval/...` | 受入証拠と承認ファイルの作成（[説明](live-acceptance.md)） | 読取専用（read-evidenceのみ） | GET（read-evidenceのみ） | なし |
| `trading.live_doctor` | 送信を止めている条件の一覧（[説明](live-doctor.md)） | なし | なし | なし |
| `trading.live_report` | 損益とリスクの余裕（[説明](live-report.md)） | なし | なし | なし |
| `trading.live_dashboard` | 状態ページの書き出し（[説明](live-dashboard.md)） | なし | なし | なし |
| `trading.promotion status/promote/revoke/check` | 戦略候補の昇格管理（[説明](promotion.md)） | なし | なし | 実験台帳 |
| `trading.live_tasks plan` / `scripts/install-live-cycle.ps1` | 毎時サイクルのタスク計画・登録（[説明](live-tasks.md)） | なし | なし | なし |
| `trading.private_tasks plan` / `scripts/install-private-watchdog.ps1` | 監視・同期続行のタスク計画・登録（[説明](private-operations.md)） | なし | なし | なし |

注文・取消のPOSTを送るのは`order_runtime submit/cancel`だけです（同期は通知用トークンの操作だけを行います）。どちらも、事前に表示した実行内容の
SHA-256と発注用キーの参照、`--order-permission-confirmed`の指定を必要とします。

提案をそのまま送信する自動送信は実装していません。判断材料は[設計案](live-autonomy-proposal.md)にまとめました。
