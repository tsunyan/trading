# 業者に痕跡のない結果不明注文の解消

2026-10-05。新規・建玉指定決済の送信が結果不明になり、業者の注文IDも分からない注文について、
完全な口座証拠と運用者の確認でclaimを解消し、注文を`ABANDONED`にする手続きを追加しました。
業者が拒否した注文（余力不足 ERR-201、時刻ずれ ERR-5008 など）は受付の応答がなく、
注文IDも残りません。この手続きがない間は、台帳・POST制御・GET制御・同期環境を作り直すしかありませんでした
（[Claudeのレビュー指摘8](../reviews/20261003-by-claude.md)）。

## 主張する範囲

この手続きは「業者に届かなかった」ことを証明しません。証明するのは、
**この注文が口座に何の影響も残していない**ことです。

- 業者が拒否した注文、業者に届かなかった注文、受け付けられて約定せずに失効した注文は、
  口座からは区別できません。どれも残高・建玉・有効注文を変えないので、台帳では同じ`ABANDONED`として扱います。
- 一部でも約定していれば建玉か残高が変わり、有効なまま残っていれば有効注文一覧に出ます。
  どちらも口座の照合で拒否します。
- 結果は常に`absence_proven=false`です。業者の注文履歴に該当がないことの確認は、運用者が承認の
  `history`証拠として記録します。

## 解消できる条件

- POSTのclaimが`order`か`close_order`です。取消のclaimは対象外です。
- 注文が`SUBMITTING`か`UNKNOWN`のままで、受付・GET照合・業者の注文証拠が一件もありません。
  GETで見つかった注文は、[GET照合](order-recovery.md)と[解消](order-resolution.md)へ進みます。
- 実口座台帳とPOST制御が停止しています。GET制御も停止していないことが必要です。
- 口座証拠が、claimを止めた時刻から**300秒以上後**に観測されています。業者側の処理待ちと区別するためです。
  時間の経過だけでは解消しません。
- その口座証拠が、この注文を除いた台帳と完全に一致します（残高・建玉・有効注文・評価額）。
  口座証拠と気配は、承認と実行の時点でも鮮度期限内である必要があります。

## 手順

同期を止めた状態（READY）で行います（[再開手続きの順序](order-restart.md)と同じ）。

1. 有効注文一覧に残っていないか、[注文の発見](order-discovery.md)で確かめます。見つかった場合はこの手続きを使いません。
2. 業者の画面または約定・注文履歴で、該当の顧客注文IDがないことを確認し、その資料を保存します。
3. 読取専用キーで口座を読み、この注文を除いた台帳と一致することを記録します。
   有効注文一覧にこの顧客注文IDがあれば`unknown_order_is_active`で拒否します。
   通常の口座ゲート（`proof_json`）は更新しません。

   ```powershell
   uv run python -m trading.live_account --directory runs/live-orders --read-control-directory runs/account-read-control --scope <scope> --credential-reference <read_only_reference> --absent-order Buy001 --confirm ...
   ```

4. contextを確認し、承認ファイルを作ります。承認の`history`には手順2の資料を指定します。
   有効期間は最大10分です。

   ```powershell
   uv run python -m trading.private_order_recovery absence-context --directory runs/live-orders --read-control-directory runs/account-read-control --scope <scope> --client-id Buy001
   uv run python -m trading.live_acceptance absence-approval --directory runs/live-orders --read-control-directory runs/account-read-control --scope <scope> --client-id Buy001 --minutes 5 --output absence-approval.json --evidence identity=... --evidence rules=... --evidence read_acceptance=... --evidence account_baseline=... --evidence history=<broker-history>
   ```

5. 確認項目を指定して解消します。`terminal-order`の代わりに`order-absent`を指定し、ほかの6項目は
   [終端の解消](order-resolution.md)と同じです。

   ```powershell
   uv run python -m trading.private_order_recovery resolve-absence --directory runs/live-orders --read-control-directory runs/account-read-control --scope <scope> --client-id Buy001 --approval absence-approval.json --confirm order-absent --confirm complete-history --confirm complete-account --confirm account-identity --confirm external-writers-paused --confirm old-clients-closed --confirm preserve-stops
   ```

6. 通常の[口座証拠の更新](live-account.md)を行い、[明示再開](order-restart.md)へ進みます。

終端の`resolve`コマンドと確認項目では、この注文を解消できません。逆に`resolve-absence`は、
業者の注文証拠がある注文を解消しません。

## 保存境界と途中終了

1. 実口座台帳に`ORDER_ABSENCE_PREPARED`（context・承認・POST状態）をcommitします。
2. 台帳のtransactionを保持したまま同じcontext・承認・鮮度・コード・GET制御を再検査し、
   POST側へ`TRADE_RESOLVED`をcommitします。
3. 保持していた台帳のtransactionで、注文を`ABANDONED`にし`ORDER_ABSENCE_RESOLVED`を記録してcommitします。

1の後に終了するとclaimは残り、新しいcontextと承認が必要です。2の後、3の前に終了した場合は、
POST側の記録に承認済みの判断が残ります。同じ`resolve-absence`を再実行すると、新しい検査なしで3だけを
完了し、`completed_interrupted_resolution=true`を返します。注文が`UNKNOWN`のままなので、
完了するまで口座ゲートと再開は拒否されます。

台帳を開くたびに、`ORDER_ABSENCE_RESOLVED`がPOST側の解消記録（同じ準備ID・SHA-256）に対応し、
注文が`ABANDONED`であることを検査します。手で書き込んだ記録は台帳の整合性検査で拒否します。
解消した顧客注文IDは再利用せず、再送もしません。損失停止・明示停止・旧許可は保持します。

## 検証

`tests/test_order_absence.py`で、解消後の`ABANDONED`と停止の保持、300秒未満の口座証拠の拒否、
建玉・有効注文・残高の差による拒否、GET照合済み・取消claimの拒否、承認後の観測の更新と承認の失効、
POST commit後に台帳commitが失われた場合の完了、偽造した解消記録の拒否、
承認ファイル作成とCLIを検証します。`tests/test_live_account.py`では、読取専用GETだけで観測を記録し、
有効注文一覧に残る注文と建玉のある口座を拒否することを確かめます。実通信と資格情報の読込は行っていません。
