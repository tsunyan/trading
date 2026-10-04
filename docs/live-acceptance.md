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

形式は`trading.read-evidence/2`で、収集ごとにランダムな`collection_id`を持ちます。収集の前後で台帳の口座ID・設定・実装SHA-256・revisionを読み、
GETの間に変わっていた場合は`journal_changed_during_collection`で保存しません。

## 2. 運用者の資料を指紋化する

```powershell
uv run python -m trading.live_acceptance file-evidence --kind rules --file evidence/broker-rules.pdf
```

本人性（`identity`）・業者ルール（`rules`）・履歴（`history`）の資料ファイルのSHA-256を表示します。
空のファイルと64MiB超は拒否します。ファイルの中身は確認しません。
`read_acceptance`・`account_baseline`は`read_evidence_requires_collection`で拒否します。
この2種類は手順1の収集で作ります。

## 3. 承認ファイルを作る

```powershell
uv run python -m trading.live_acceptance approval --directory runs/live-orders --read-control-directory runs/account-read-control --scope <scope> --evidence identity=evidence/identity.pdf --evidence rules=evidence/broker-rules.pdf --evidence read_acceptance=evidence/read-acceptance.json --evidence account_baseline=evidence/account-baseline.json --evidence history=evidence/history.csv --hours 72 --output approval.json
```

台帳の現在の口座ID・設定SHA-256・実装SHA-256を使い、受入時刻を現在、失効を`--hours`後
（1〜168時間）にします。5種類の証拠をちょうど1つずつ要求します。

- 読取の2種類は、形式どおりのJSONか（重複キーも拒否）、指定した種類と保存された種類が一致するか、
  台帳の現在の口座ID・設定SHA-256と一致するか、収集から24時間以内で未来でないかを検査します。
  形式の違うJSONや別の台帳で集めた証拠は使えません。
- 読取の2種類は別々の収集でなければなりません。1回の収集を複製して`kind`だけを書き換えると、
  内容とSHA-256は変わりますが`collection_id`が同じなので`read_evidence_reused_collection`で拒否します。
- 5種類（取消は3種類）の証拠はすべて異なる内容でなければなりません。同じファイルに複数の種類の
  ラベルを付けると`evidence_documents_must_differ`で拒否します。出力の`expected_revision`を
`live_setup activate --expected-revision`に渡します。コードや設定が変わると実装・設定のSHA-256が
変わるため、承認ファイルを作り直します。

## 4. 再開・claim解消・対象限定取消の承認ファイル

```powershell
uv run python -m trading.live_acceptance restart-approval --directory runs/live-orders --read-control-directory runs/account-read-control --scope <scope> --evidence identity=... --evidence rules=... --evidence read_acceptance=... --evidence account_baseline=... --evidence history=... --stop-review evidence/stop-review.md --hours 72 --output restart-approval.json
uv run python -m trading.live_acceptance resolution-approval --directory runs/live-orders --read-control-directory runs/account-read-control --scope <scope> --client-id Buy001 --evidence identity=... --evidence rules=... --evidence read_acceptance=... --evidence account_baseline=... --evidence history=... --minutes 10 --output resolution-approval.json
uv run python -m trading.live_acceptance cancel-approval --directory runs/live-orders --read-control-directory runs/account-read-control --scope <scope> --client-id Buy001 --evidence identity=... --evidence rules=... --evidence read_acceptance=... --minutes 10 --output cancel-approval.json
```

- `restart-approval`: [明示再開](order-restart.md)のcontextのSHA-256、現在のコードでの新しい`LiveApproval`、
  停止原因を確認した資料の指紋をまとめます。出力の`confirmations`が再開に必要な確認項目です。
- `resolution-approval`: [claim解消](order-resolution.md)のcontextのSHA-256と5種類の証拠をまとめます。
- `cancel-approval`: [対象限定の取消](live-cancel.md)のcontextのSHA-256と3種類（本人性・業者ルール・
  読取受入）の証拠をまとめます。

解消と取消の承認は有効期間が最大10分です。直前に作って、すぐに各手続きへ渡してください。
どのコマンドも承認ファイルを書くだけで、再開・解消・取消は行いません。

## 検証

`tests/test_live_acceptance.py`で、資料ファイルのSHA-256と不正な種類・空ファイルの拒否、読取の証拠の
保存内容・台帳を変えないこと・上書きの拒否、作った承認ファイルで登録済み台帳を有効化できること、
期限の範囲、証拠の種類の過不足、CLIで承認ファイルを書いても有効化しないことを検証します。
加えて、読取の種類を資料として指紋化できないこと、同じ資料での複数種類の拒否、手書き・種類違い・
別の設定・古い/未来の収集時刻・重複キーの読取証拠の拒否、収集中の台帳変更で保存しないことを検証します。

## 検査の範囲

このCLIの検査は、種類の付け間違い・古い証拠・別の台帳の証拠・同じ資料や同じ収集の使い回しを防ぎます。
一方で、ファイル・台帳・コードを操作できる運用者自身に対して、証拠の出所を証明するものではありません。
形式どおりで新しい`collection_id`を持つJSONを手で作れば、収集したものと区別できません。
運用者は信頼の起点であり、ここでの目的は誤操作を防ぐことです（2026-10-05の再レビュー対応で記述を改めました）。

台帳は証拠のSHA-256だけを記録します。承認ファイルを手書きした場合、読取証拠の中身と収集の検査は通りません。
ただし、種類ごとに異なるSHA-256であることは、承認のモデル自体（`LiveApproval`・`CancelApproval`・
`OrderResolutionApproval`）が検査するため、手書きの承認でも同じ資料を複数の種類に使うことはできません。
承認ファイルはこのCLIで作ってください。
追加8試験が合格しました。Ruffの検査・整形確認、差分チェックも合格しました。
再開・解消・取消の承認は`tests/test_live_acceptance_checkpoints.py`で、作った承認ファイルで実際に各手続きが通ることを含めて検証します（追加6試験）。
