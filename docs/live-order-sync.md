# 受付済み注文のGET照合

2026-10-04。送信が受け付けられた注文を、台帳が保存した業者の注文IDでGETし、台帳の注文状態を
進めるCLIを追加しました。POSTは使いません。

これまで、受付後の注文は`RECONCILING`のまま台帳に残り、通常の運用でGET証拠を台帳へ渡す経路は
結果不明注文の[GET照合](order-recovery.md)だけでした。`RECONCILING`の注文があると、台帳は次の注文の
準備を拒否し、[戦略の提案](live-signal.md)も見送り、[口座証拠の更新](live-account.md)も
未解決の注文として通りません。

```powershell
uv run python -m trading.live_order_sync --directory runs/live-orders --read-control-directory runs/account-read-control --scope <scope> --client-id Buy001 --credential-reference <read_only_reference> --confirm complete-history --confirm external-writers-paused
```

## 対象

台帳で`RECONCILING`・`WORKING`・`PARTIAL`・`CANCEL_PENDING`の注文です。注文IDは運用者が
指定せず、台帳の受付記録または照合済みの証拠から取ります。両方が食い違えば拒否します。
準備済み・送信中・結果不明の注文は対象外です。結果不明の注文は[GET照合](order-recovery.md)と
[claim解消](order-resolution.md)を使います。POST制御に処理中のclaimがある間も拒否します。

## 読取と完全性

[口座読取](account-reader.md)の`collect_order`で、注文と約定を2回ずつ取得し、内容が同じことを確かめます。
全量約定の注文は、約定数量の合計が注文数量と一致する必要があります。
読取の失敗・途中変化・別の注文IDの応答では、台帳を変更しません。

約定一覧はその注文の約定をすべて返す前提ですが、これを読取だけでは証明できません。
`complete-history`（この注文の約定一覧が完全）と`external-writers-paused`（台帳の外から口座を
操作していない）の確認がある場合だけ、`executions_complete=true`として台帳の照合に渡します。

## 台帳の照合

台帳は受付記録との一致（注文意図・親注文ID・注文ID・時刻・受付時の状態）、以前の証拠からの
約定の消失・変更、終了済み状態の変化、別の注文への業者IDの重複を検査します。
矛盾があれば台帳を停止します。状態は、約定がなければ`WORKING`、一部約定なら`PARTIAL`、
全量約定なら`FILLED`、取消・失効ならその状態です。取消待ちは終了まで`CANCEL_PENDING`のままです。

照合後は[口座証拠を更新](live-account.md)すると、次の提案・準備・送信へ進めます。

## 検証

`tests/test_live_order_sync.py`で、受付済み注文の`WORKING`化とその後の口座証拠の更新、全量約定の
`FILLED`化、確認不足・未送信・結果不明の注文でキーを読まないこと、別IDや空の応答で台帳を
変えないこと、受付記録と矛盾する約定での停止、CLIの失敗表示を検証します。実通信は行いません。
追加10試験を含む全2346テストが526.84秒で合格しました。Ruffの検査・整形確認、差分チェックも合格しました。
