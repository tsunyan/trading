# 同期経路からの建玉拘束照合

2026-10-02。[建玉拘束数量の照合](position-reservations.md)を、同期モニター・通知捕捉・
Private WebSocket受信アダプターへ接続しました。現在の取得で集めた資料だけを使い、
`epoch`・`revision`・受信番号・台帳の `head` を結果に含めます。
比較は非永続の診断で、注文送信・拘束の補正・発注許可へは変換しません。

## 明示指定での比較

開始建玉を宣言したversion 2／3の台帳を使い、通知ジャーナルと同じローカル `scope` を指定します。
この名前の一致は実口座本人性を証明しません。

```python
def collect_reservations():
    # 元の注文意図と既知注文IDを運用者が明示する。
    # RESTの未約定注文一覧から決済割当を推定しない。
    return tuple(reader.collect_order(intent, order_id) for intent, order_id in known_orders)


assessment = capture.resync(
    reader.collect_account,
    collect_reservations=collect_reservations,
    reservation_book=book,
)
comparison = assessment.position_reservations
# receiver.resyncでも同じ引数を使用できます。
```

`collect_reservations` は引数なしのコールバックで、個別注文資料のtupleを返します。
すべての未約定注文に対応する資料と、必要なら取消・失効・全約定の資料を用意してください。
知らない注文の資料を自動作成せず、比較側で資料不足として返します。
台帳を指定してコールバックを省略した場合、またはscopeが違う場合は、REST取得前に拒否します。

取得順は「拘束照合用の個別注文資料 → 口座資料 → 約定照合用の個別注文資料」です。
最後の段階は従来の `collect_orders` を併用し、約定通知がある場合だけ実行します。
拘束照合と約定照合の個別資料は、それぞれの取得順・時刻を満たす必要があります。
同じコールバックや先に取った資料を、後段の新しい約定観測の代わりに使わないでください。

## 取得と比較を分ける場合

```python
assessment = capture.resync(reader.collect_account, collect_reservations=collect_reservations)
comparison = capture.compare_position_reservations(book, expected_revision=assessment.revision)
```

台帳を省略した取得では `position_reservations=None` です。コールバックを指定した場合だけ、
受け入れた口座資料と個別注文資料をモニター内部に一時保持します。
返却モデルや比較辞書を書き換えても、内部の資料は置き換わりません。
コールバックなしで再取得した場合、以前の資料を使い回しません。
保存済みジャーナルや前の捕捉オブジェクトから資料を復元する処理もありません。

モニター単独では `resync(session, ..., collect_reservations=...)` の後に
`compare_position_reservations(session, book, expected_revision=...)` を使えます。
ジャーナルの世代・未処理記録の検査は、捕捉アダプター経由で行ってください。

## 世代・鮮度・失敗

取得ごとにrevisionを更新します。通知到着・同じ通知の再配信・切断・新しい接続世代・
次の取得開始・時計異常で内部の資料を無効化します。取得中は受信ロックを保持しないため、
通知を保存・配信でき、その到着によって進行中の取得を拒否します。

拘束照合用資料の受信時刻は今回の取得開始以降、応答時刻は開始時刻の許容差以内以降を要求します。
全個別資料が口座観測の開始以前に集まったことも検査します。
収集全体の期限は既定30秒、受け入れた資料の保持期限も既定30秒です。
残高差やイベント差のため `structural_match=false` でも、資料の保持期限を検査します。
ストリームの生存期限とReaderに合わせた `clock_skew_ms` も引き続き適用します。

比較中は捕捉・モニターのロックとジャーナルのSQLite書込み予約を保持します。
別プロセスの世代引継ぎや処理結果不明の記録があれば比較を拒否します。
比較後にもrevisionと期限を確認するため、比較に時間がかかって資料が失効した場合は結果を返しません。
比較の失敗・中断では捕捉オブジェクトを停止し、受信アダプター経由ならソケット・トークンも終了します。
内部例外は `position_reservation_comparison_failed` に置き換え、中断は元の例外を伝えます。
収集失敗は従来どおり現在の観測を無効化します。自動再試行・再接続はありません。

再起動後は、台帳を開き直し、新しい捕捉世代と新しいREST取得を明示的に開始します。
世代の履歴欠落を解消したとは扱わず、ジャーナルと業者履歴の連続性の未確認条件を残します。

## 約定計上との併用

`cash_book=book` と `reservation_book=book` を併用できます。両方は同じDBを指定してください。
約定計上のための `collect_orders` も明示します。
従来の条件で個別約定を計上した後に、拘束数量を比較します。部分決済は新しい台帳建玉と比較します。
両結果の `head` が異なる場合は、別の書込みが入ったものとして捕捉を停止し、結果を返しません。

約定のcommitと拘束比較は、別のトランザクションです。比較が失敗しても、直前に成功した現金仕訳を
取り消しません。新しい捕捉世代・新しい取得で再確認し、既存の約定IDによる重複防止を使います。
比較だけの場合、約定・現金・拘束の書込みはありません。

`structural_match` と `mismatches` はイベント／REST構造の診断です。
拘束照合の結果は `position_reservations.reservation_match` とその `mismatches` で確認します。
拘束差異と未確認条件は、親assessmentの `blockers` にも含めます。
構造が一致しても拘束数量が不一致になり得ます。いずれも全口座会計の成功や発注許可を意味しません。
返却後の通知や別の台帳書込みで古くなる診断値で、原子的な口座証明ではありません。

`complete=false`、`live_enabled=false` を維持します。初期残高・初期建玉・全履歴・注文意図の認証、
業者の拘束規則、証拠金計算、実口座での受入検証は残っています。

## 合成デモ

```powershell
uv run python -m trading.position_reservation_sync_lab demo --directory runs/position-reservation-sync-demo
uv run pytest tests/test_position_reservation_sync.py -q
```

ネットワークを使わず、初回比較、部分決済後の比較、説明できない拘束差、期限切れによる拒否、
新しい捕捉世代での再取得を保存します。未使用ディレクトリにジャーナル・現金台帳・
`report.json` を作り、上書きしません。実機の遅延・実購読・業者の拘束規則は未検証です。
