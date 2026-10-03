# GMO注文POSTの受付記録

2026-10-03。[GMO公式仕様](https://api.coin.z.com/fxdocs/#order)の新規注文と
[建玉指定決済](https://api.coin.z.com/fxdocs/#close-order)の受付応答を扱います。
単一のUSD/JPY、NORMAL注文、LIMITまたは価格保護付きMARKETが対象です。
HTTP送信、実口座への有効化、口座本人性の確認はこの機能では行いません。

## 受付と約定の区別

POSTの`data`は1注文の配列です。GETの`data.list`とは別に解析します。

| POSTの状態 | 保存する内容 | 台帳の判断 |
| --- | --- | --- |
| WAITING | 親注文ID、注文ID、意図との一致、受付時刻 | GETで注文・約定の証拠を照合するまでRECONCILING |
| EXECUTED | 同上と、業者が返したEXECUTED | 約定ID・数量・価格・手数料が未確認なのでRECONCILING |
| EXPIRED | 同上と、業者が返したEXPIRED | 失効までの約定履歴が未確認なのでRECONCILING |

受付応答には約定明細や完全性フラグを付けません。WAITINGをORDEREDへ変換せず、
EXECUTEDを全量約定、EXPIREDを未約定の証明として扱いません。
取消受付も最終取消の証明ではなく、既存の取消照合経路を継続します。

## 厳密な解析

`trading.order_receipts.parse_submission_receipt`に、注文意図、応答の生bytes、
送信開始・受信完了のaware datetimeを渡します。最大64,000 bytesのJSONだけを受理し、
重複キー、非有限値、不明な項目、GET用の構造、複数注文を拒否します。
顧客ID、銘柄、方向、取引区分、数量、指値が意図と一致することを確認します。
親ID・注文IDは正の整数、時刻はタイムゾーン付き、期限は実在する8桁の日付を要求します。
MARKETの応答には指値を受理しません。PRICE_BOUNDはMARKETのEXPIREDにだけ受理します。

注文時刻から業者応答時刻、ローカル受信時刻までの順序を検査します。
明示した時計の許容差は最大1秒です。許容差の指定は時計同期や実口座受入の代わりにはなりません。
出力はimmutableな`SubmissionReceipt`で、生本文のSHA-256を含みます。
生本文、認証ヘッダー、業者のエラーメッセージは保存しません。

## 永続保存と後続照合

既存のオフライン`OrderJournal`に`submission_response`と`acknowledge_submission`を追加しました。
claim済みの意図だけに受付を結び付け、`SUBMISSION_ACK`イベントへcommitします。
未claimや放棄済みの注文は採用しません。同じ受付の再適用は何も追加せず、
異なる受付や他の意図との業者ID再利用では元の記録を保持して停止します。
不正なPOST応答ではUNKNOWNと永続停止を保存し、再送を拒否します。

GET照合時には受付の親ID・注文IDと一致し、受付後の業者時刻であることを要求します。
受付がEXECUTEDまたはEXPIREDなら、後のGET状態もその状態と一致する必要があります。
完全性未確認のGETは、約定明細が返ってもRECONCILINGに留まります。
終端状態になった後も受付を保持し、同じ受付の再適用で状態を戻しません。

保存先は従来の`order-lab.sqlite`のままです。既存のオフライン台帳のmodeは変えず、
実口座台帳への転用・自動昇格は追加していません。実送信経路には別の専用台帳、
明示的な有効化、現在の口座と気配によるリスク検査、送信結果別の復旧が必要です。

## 検証

```powershell
uv run pytest tests/test_order_receipts.py tests/test_order_journal.py tests/test_account_guard.py tests/test_broker_contracts.py -q
```

受付の88試験を含む関連223試験が合格しました。公式の新規・決済、成行・指値、
3つの状態の形を合成検証し、受付とGETの矛盾、再起動後の再送拒否、秘密情報を含むエラーを確認しました。
実際のGET HTTPクライアントとReaderを通した取得でも、完全性が未確認のまま保持されます。
実プロセスを受付commitの前後で終了させ、前ならSUBMITTING、後ならRECONCILINGを残し、
どちらも再送できないことを確認しました。実口座からのPOST応答はまだ受け取っていません。
