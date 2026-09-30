# GET専用の認証付き通信アダプター

2026-09-30。`trading.private_read.PrivateReadClient` を追加しました。
実口座への接続は行わず、HTTPの模擬応答で検証しています。
**コードには認証付きGET通信能力がありますが、注文・取消・送金の送信能力はありません。**
既存CLI、再生デモ、ペーパー運用、自動実行には組み込んでいません。

## 制限

- 送信先は `https://forex-api.coin.z.com/private` 固定。URL指定機能はありません。
- 許可するのは `/v1/account/assets`、`/v1/openPositions`、`/v1/activeOrders`、
  `/v1/orders`、`/v1/executions` のGETだけ。本文・未知のパラメータ・重複パラメータを拒否。
  一覧取得は口座全体を確認するため銘柄フィルターなし、注文照会は既知の単一注文IDに限定。
- APIキー・秘密鍵は `SecretStr` として呼出側から明示的に渡す必要があります。
  環境変数、設定ファイル、OSの資格情報、ブラウザからの自動読込はありません。
- GETのHMAC-SHA256署名は送信間隔の待機後に計算します。クエリは署名対象に含めません。
  壁時計が前回署名時刻より逆行すると、共有リミッターを停止します。
- TLS証明書検証は有効。リダイレクトを追わず、環境のプロキシ／証明書上書きを使いません。
  Cookieを要求に付けず、応答Cookieも保持しません。
- 接続・読込・書込・プール待機は各5秒が既定。受信中と受信後に経過10秒を検査します。
  同期通信なので、通信中に厳密な10秒で割り込む仕組みではありません。
  経過時間検査はブロック中のI/Oが戻るまで待ちます。1回のI/O自体には位相別timeoutが働きます。
- 応答は最大2MB。JSON以外、圧縮応答、重複JSONキー、不正／非有限の数値、
  不一致のContent-Length、HTTP/APIエラーを拒否します。
- 自動リトライはありません。ページ単位で継ぎ足さず、呼出側が収集全体を破棄して判断します。
  エラー本文・通信例外の本文・認証ヘッダーは表示・保存しません。

GMOの[公式FX API仕様](https://api.coin.z.com/fxdocs/)では同一口座のGET上限は毎秒6回です。
この実装は通信を直列化し、**前回の処理終了から250ms以上**空ける保守的な設定です。
複数キー／クライアントを同じ口座で使う場合、必ず1個の `AccountReadLimiter` を共有します。
別プロセス・別PC・他のアプリの通信量は制御できません。口座全体のレート制限保証ではありません。

HTTP 401/403/429と時計逆行では、その共有リミッターを停止します。
時間経過や次回呼出で自動復帰しません。メモリ内リミッターに解除メソッドはありません。
永続リミッターには、未完了claimのない一部の停止向けに[手動復旧](read-recovery.md)を追加しました。
原因と業者の待機指示を確認せずにリミッターを作り直して再接続しないでください。
メモリ内リミッターでは再起動をまたぐ停止は保持しません。
[永続停止・プロセス間制御](read-control.md)用の `PersistentReadLimiter` を渡す場合は、
停止・未完了claimを再起動後も保持します。どちらも自動復旧には接続しません。

## 呼出インターフェース

以下は将来の接続コードの形です。**実行すると認証付きGETを送信します。**
現段階で実口座のキーを設定・貼り付ける必要はありません。

```python
from pydantic import SecretStr
from trading.account_reader import AccountReader
from trading.private_read import AccountReadLimiter, PrivateReadClient


def read_account(api_key: SecretStr, secret: SecretStr, limiter: AccountReadLimiter):
    with PrivateReadClient(api_key, secret, limiter=limiter) as transport:
        return AccountReader(transport).collect_account()
```

口座開設後は業者側でも読み取り専用の権限を設定し、IP制限と時刻同期を確認してください。
[Windows資格情報マネージャーを使う明示的な保管・取得](credential-store.md)を追加しました。
実ストアでの受入確認は未実施です。キーをソースコード、CLI引数、チャット、transcriptへ入れません。
オブジェクトのreprはキーを表示しませんが、Pythonのメモリから秘密を完全消去する保証はありません。
close時に保持参照を外し、リクエストから認証ヘッダーを除去します。
デバッガー、ローカル変数付き例外表示、メモリダンプ、任意の通信フックは別途保護が必要です。
注入可能な低水準transportは信頼済みテスト用であり、悪意あるPythonコードを封じる境界ではありません。

## 検証と残項目

`uv run pytest tests/test_private_read.py -q` はダミーキー・MockTransportを使い、
ネットワーク接続を禁止して検証します。12応答の合成口座を収集器へ接続する結合テストも含みます。

取得結果は引き続き診断専用の `AccountReadReport` です。
`live_enabled=false` は**実注文無効**の意味で、今回のクライアントがGET通信不能という意味ではありません。
口座本人性・同一時点の一貫性・約定履歴の完全性は証明されず、
注文台帳の発注ゲートへ接続する変換はありません。

未実装:

- Windowsキー保管の実ストア受入確認、業者側キー失効と実口座／キーの紐付け確認
  （明示保存・参照ID指定読込・新しい参照IDによる切替は模擬検証済み）
- 別PC／他アプリを含む口座単位の通信制御、旧version 1/2の未完了claimの解決・移行
  （同じローカルDBの制御・永続停止・手動復旧と、version 3の[OSロック付きclaim解消](read-orphan.md)は実装済み）
- 変更イベントの実受信、業者側欠落の完全検出、約定履歴の再同期
  （[通知とRESTの構造照合・再取得](account-sync.md)はオフラインで追加。完全同期や発注許可にはしない）
  （[通知の永続記録](event-journal.md)は模擬入力で検証済み。実受信・長期運用には未接続）
- 実口座での残高・手数料・スワップ・丸め検証
- 実注文送信、直前再検査、緊急停止／再開、実資金上限と戦略の昇格判断
