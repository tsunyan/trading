# 凍結候補の昇格（ペーパー・実運用）

2026-10-04。[実験台帳](roadmap.md)で凍結した候補に、`paper`（模擬運用）と`live`（実運用）の昇格状態を
記録するCLIを追加しました。実発注の運用サイクルと戦略の提案は、台帳と仮説を指定した場合、
`live`に昇格した凍結候補とまったく同じ戦略設定でなければ新規を提案しません。

```powershell
uv run python -m trading.promotion status --ledger runs/ledger.sqlite --hypothesis H001
uv run python -m trading.promotion promote --ledger runs/ledger.sqlite --hypothesis H001 --stage paper --reason "凍結後のペーパー運用を開始"
uv run python -m trading.promotion promote --ledger runs/ledger.sqlite --hypothesis H001 --stage live --reason "forward OOSの事前条件を満たした"
uv run python -m trading.promotion check --ledger runs/ledger.sqlite --hypothesis H001 --config configs/fx.toml
uv run python -m trading.promotion revoke --ledger runs/ledger.sqlite --hypothesis H001 --reason "実運用の損失を確認するため停止"
```

## 昇格の条件

- `paper`: 仮説が凍結済み（`trading ledger freeze`）であること。取り消し後は再び`paper`から始めます。
- `live`: 現在が`paper`で、その仮説の台帳記録のうち区分が`forward_oos`のものに、最新の判断として
  `advance`が記録されていること。合否条件は結果を見る前に決めておきます（[ロードマップ](roadmap.md)）。
- `revoke`: 現在の昇格を取り消します。理由は必須です。

記録は追記のみで、取り消しも履歴に残ります。昇格時の凍結仕様（戦略・パラメーター・設定SHA-256・
コードSHA-256・実行条件）を各行に保存します。実験台帳には`promotions`表を追加するだけで、
既存の表とスキーマ版は変えません。昇格は運用者の記録であり、戦略の収益性を保証しません。

## 実発注との接続

`live_cycle`・`live_signal`・定期実行の計画（`live_tasks plan`、`install-live-cycle.ps1`）に
`--ledger`と`--hypothesis`（スクリプトでは`-Ledger`・`-Hypothesis`）を指定すると、次を確かめます。

- 仮説の現在の段階が`live`である。
- 実行に使う戦略設定（`--config`）の`Settings.fingerprint`が凍結仕様の設定SHA-256と一致し、
  戦略とパラメーターも一致する。

満たさない場合は、読取や発注用キーを使う前に`strategy_not_promoted_for_live`または
`strategy_config_differs_from_candidate`で拒否します。定期実行の計画は作成時点で確かめます。
`--flatten`（全建玉の手仕舞い）は昇格に関係なく使えます。取り消した後も建玉を閉じられるようにするためです。

台帳と仮説を指定しない場合は従来どおり確認しません。実口座で戦略を動かす前には指定してください。

## 検証

`tests/test_promotion.py`で、凍結前の昇格拒否、`paper`から`live`への順序、forward OOSの`advance`の要求、
設定の違い・取り消し後の拒否、スキーマ版の不変、CLIの理由表示を検証します。運用サイクルと定期実行の
計画では、`paper`段階での拒否（読取前）・手仕舞いの許可・`live`段階での提案を検証します。
