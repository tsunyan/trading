# 注文IDが分からない結果不明注文の発見

2026-10-04。送信の応答が失われ、業者の注文IDが分からない結果不明注文について、
有効注文一覧から顧客注文IDで注文を探す読取専用のCLIを追加しました。台帳は変更しません。

GMOの注文情報取得は注文IDを要求し、顧客注文IDでは検索できません。一方で
[有効注文一覧](https://api.coin.z.com/fxdocs/#active-orders)は顧客注文IDを返すため、
業者側で有効なまま残っている注文なら見つけられます。

```powershell
uv run python -m trading.order_discovery --directory runs/live-orders --read-control-directory runs/account-read-control --scope <scope> --client-id Buy001 --credential-reference <read_only_reference> --read-only-confirmed
```

## 動作

1. 台帳で送信claimを消費した結果不明注文であることを、[GET照合](order-recovery.md)と同じcontextで確認します。
   未送信の注文やclaimの不一致では、読取専用キーを読みません。
2. GET制御が停止中なら拒否します。
3. 読取専用キーで口座を2回走査し（[口座読取](account-reader.md)）、同じ顧客注文IDの有効注文を探します。
4. 見つかった注文の銘柄・売買・新規/決済・注文タイプ・数量・価格が台帳の注文意図と違えば拒否します。
   同じ顧客注文IDの有効注文が複数あっても拒否します。

見つかった場合は親注文IDと注文ID、状態を出力します。その注文IDを
`private_order_recovery reconcile --order-id`に渡して照合し、
[有効なまま残る注文のclaim解消](order-resolution.md)へ進みます。

## 見つからない場合

`found=false`は不在の証明ではありません。出力の`absence_proven`は常に`false`です。
全量約定・失効・拒否・未到達のどれかを、有効注文一覧だけでは区別できません。
業者画面や約定履歴で注文IDを確認するまで、claimは解消できません。

## 検証

`tests/test_order_discovery.py`で、結果不明の新規注文をGETだけで見つけて台帳を変えないこと、
見つからない場合に不在を主張しないこと、条件の違う同じ顧客注文IDの拒否、
未送信の注文・確認不足でキーを読まないこと、CLIの失敗表示を検証します。実通信は行いません。
追加7試験が合格しました。台帳と既存モジュールは変更していません。Ruffの検査・整形確認、差分チェックも合格しました。
