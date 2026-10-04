# 受入証拠と有効化承認ファイルの作成

2026-10-04。実発注台帳の有効化に必要な承認ファイル（`LiveApproval`）を、証拠ファイルから
作るCLIを追加しました。これまでは5種類の証拠の参照名・SHA-256と期限を手書きのJSONで用意していました。
このCLIは承認ファイルを書くだけで、有効化は[`live_setup activate`](live-setup.md)で別に行います。

## 1. 読取の証拠を保存する

```powershell
uv run python -m trading.live_acceptance read-evidence --directory runs/live-orders --read-control-directory runs/account-read-control --scope <scope> --kind account_baseline --credential-reference <read_only_reference> --output evidence/account-baseline.json
uv run python -m trading.live_acceptance read-evidence --directory runs/live-orders --read-control-directory runs/account-read-control --scope <scope> --kind read_acceptance --credential-reference <read_only_reference> --output evidence/read-acceptance.json
```

読取専用キーで口座を2回走査し（[口座読取](account-reader.md)）、資産・建玉・有効注文・応答の記録を、
台帳の口座IDと設定SHA-256とともに保存します。既存のファイルは上書きしません。
`account_identity_verified`は常に`false`です。保存した結果が口座本人性の証明になるわけではなく、
運用者が業者画面などと突き合わせて確認する資料です。

## 2. 運用者の資料を指紋化する

```powershell
uv run python -m trading.live_acceptance file-evidence --kind rules --file evidence/broker-rules.pdf
```

本人性（`identity`）・業者ルール（`rules`）・履歴（`history`）の資料ファイルのSHA-256を表示します。
空のファイルと64MiB超は拒否します。ファイルの中身は確認しません。

## 3. 承認ファイルを作る

```powershell
uv run python -m trading.live_acceptance approval --directory runs/live-orders --read-control-directory runs/account-read-control --scope <scope> --evidence identity=evidence/identity.pdf --evidence rules=evidence/broker-rules.pdf --evidence read_acceptance=evidence/read-acceptance.json --evidence account_baseline=evidence/account-baseline.json --evidence history=evidence/history.csv --hours 72 --output approval.json
```

台帳の現在の口座ID・設定SHA-256・実装SHA-256を使い、受入時刻を現在、失効を`--hours`後
（1〜168時間）にします。5種類の証拠をちょうど1つずつ要求します。出力の`expected_revision`を
`live_setup activate --expected-revision`に渡します。コードや設定が変わると実装・設定のSHA-256が
変わるため、承認ファイルを作り直します。

## 検証

`tests/test_live_acceptance.py`で、資料ファイルのSHA-256と不正な種類・空ファイルの拒否、読取の証拠の
保存内容・台帳を変えないこと・上書きの拒否、作った承認ファイルで登録済み台帳を有効化できること、
期限の範囲、証拠の種類の過不足、CLIで承認ファイルを書いても有効化しないことを検証します。
追加8試験が合格しました。Ruffの検査・整形確認、差分チェックも合格しました。
