# 実発注台帳の作成から確認済み送信までの手順

2026-10-04。専用の実発注台帳を作成・注文準備・有効化・状態確認・停止するCLI
（`trading.live_setup`）を追加し、既存のCLIと合わせて一連の手順をまとめました。
これまで台帳の作成・注文準備・初回有効化はライブラリ呼出しだけでした。
このCLIは資格情報を読まず、HTTPも送りません。GMO口座は未準備で、手順全体は合成環境でだけ検証しています。

## 設定ファイル

注文の上限と口座リスクの方針を1つのJSONにまとめます。作成時に台帳へ固定され、後から変更できません。
重複キー・未知の項目・64000バイト超を拒否し、エラーにファイルの内容を表示しません。

```json
{
  "limits": {"min_units": 1000, "max_units": 1000, "unit_step": 1000, "price_tick": "0.001", "max_reference_notional": "200000"},
  "policy": {"account_id": "<口座ID>", "bootstrap_at": "2026-10-10T00:00:00Z", "starting_balance": "100000", "...": "..."}
}
```

`policy`の項目は[口座ゲート](account-guard.md)の`AccountPolicy`です。上の値は形式の例で、推奨値ではありません。
実口座の上限は、業者の取引ルールと受入結果を確認してから決めます。

## 手順

`<scope>`・保存先は例です。各段階の出力にあるrevision・SHA-256を次の段階へ渡します。

1. GET制御と読取専用キー、POST制御を作ります（[GET制御](read-control.md)、[キー保管](credential-store.md)、
   [POST制御](post-control.md)）。

   ```powershell
   uv run python -m trading.read_control init --directory runs/account-read-control --scope <scope>
   uv run python -m trading.credential_store save --directory runs/account-read-control --scope <scope> --read-only-confirmed
   uv run python -m trading.post_control init --directory runs/account-post-control --read-control-directory runs/account-read-control --scope <scope>
   ```

2. 実発注台帳を作ります。DISABLEDで作られ、POST制御へ永久に紐付きます。

   ```powershell
   uv run python -m trading.live_setup create --directory runs/live-orders --read-control-directory runs/account-read-control --scope <scope> --config configs/live-orders.local.json
   ```

3. Private同期と独立監視を初期化し、台帳へ登録します（[同期](private-sync.md)、
   [送信前の同期・監視検査](live-operations.md)）。`private_sync register-live-orders`で
   台帳の注文を同期の既知注文へ登録します（[注文登録](live-order-catalog.md)）。
4. 同期を開始し、`private_operations watchdog`を定期実行します。照合成功と監視の確認を待ちます。
5. 発注用キーを保存します（[発注用キー](order-runtime.md)）。

   ```powershell
   uv run python -m trading.order_credentials save --directory runs/live-orders --read-control-directory runs/account-read-control --scope <scope> --order-permission-confirmed
   ```

6. 口座を読み取り、台帳の口座証拠を更新します（[口座証拠の更新](live-account.md)）。
7. 注文意図（`OrderIntent`のJSON）を準備します。

   ```powershell
   uv run python -m trading.live_setup prepare --directory runs/live-orders --read-control-directory runs/account-read-control --scope <scope> --intent order.json
   ```

8. 有効化の識別子を取得し、受入証拠を添えた承認ファイル（`LiveApproval`）を作って有効化します。

   ```powershell
   uv run python -m trading.live_setup activation-context --directory runs/live-orders --read-control-directory runs/account-read-control --scope <scope>
   uv run python -m trading.live_setup activate --directory runs/live-orders --read-control-directory runs/account-read-control --scope <scope> --approval approval.json --expected-revision <revision> --confirm live-orders --confirm account-identity --confirm broker-rules --confirm read-acceptance --confirm complete-account --confirm external-writers-paused
   ```

   CLIの`activate`は、同期・監視を台帳へ登録していない場合に拒否します。
   承認の内容と期限（最大7日）は[実発注台帳](live-orders.md)のとおりです。

9. [気配を取得](live-quote.md)し、`order_runtime context`で実行内容を確認してから、
   そのSHA-256を指定して`order_runtime submit`で送信します（[確認済み送信](order-runtime.md)）。
10. 受付後は[受付済み注文のGET照合](live-order-sync.md)で台帳の注文状態を進め、
    次の注文の前に[口座証拠を更新](live-account.md)します。

手順6〜9は[運用サイクル](live-cycle.md)の1回のコマンドで行えます。送信だけは別のコマンドです。

## 未送信の注文の破棄

```powershell
uv run python -m trading.live_setup abandon --directory runs/live-orders --read-control-directory runs/account-read-control --scope <scope> --client-id <client_id> --confirm-abandon
```

準備済み（PREPARED）で送信claimを一度も取っていない注文だけを破棄できます。送信された可能性がある
注文は拒否します。準備したまま送らない注文は、次の提案や準備を止めるため破棄してください。

## 状態確認と停止

```powershell
uv run python -m trading.live_setup status --directory runs/live-orders --read-control-directory runs/account-read-control --scope <scope>
uv run python -m trading.live_setup stop --directory runs/live-orders --read-control-directory runs/account-read-control --scope <scope> --confirm-stop
```

`status`は段階・revision・有効化の可否・停止・登録の有無・承認の期限・損失による新規停止・
口座証拠の時刻・注文ごとの状態だけを表示します。残高・注文本文・イベントの内容は表示しません。
`stop`は台帳を停止します。再開は[専用の再開手続き](order-restart.md)を使います。
POST制御の停止（`post_control stop`）とGET制御の停止（`read_control stop`）は別にあります。

## 検証

`tests/test_live_setup.py`で、一時的なGET・POST制御からの作成、同じPOST制御への二重作成の拒否、
注文準備、有効化の識別子、確認項目・revisionの不足、未登録台帳の有効化拒否、登録済み台帳の有効化、
停止の確認と状態表示、不正な設定ファイルを作成前に拒否することを検証します。
追加7試験が合格しました。Ruffの検査・整形確認、差分チェックも合格しました。
