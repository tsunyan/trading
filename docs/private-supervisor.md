# Private同期の継続実行

2026-10-03。`PrivateStreamSupervisor`は、通知受信を続けながらREST照合を別スレッドで行い、
容量・接続時間・イベント数に応じて記録区間と接続を切り替えるライブラリAPIです。
合成通信で検証済みです。実口座での長時間受入、資格情報・読取クライアントを組み合わせるCLI、
Windowsタスク登録・障害通知との接続は残っています。

## 明示的な開始

[区間保存ジャーナル](segmented-journal.md)、同じscopeの現金台帳、
[所有権・永続停止の制御DB](stream-control.md)、共有`PrivateStreamLimiter`を指定します。
生成と`status()`は通信しません。最低6記録の区間容量が必要です。

```python
from trading.event_capture import JournaledEventCapture
from trading.private_stream import PrivateStreamReceiver
from trading.private_stream_token import PrivateStreamLimiter, PrivateTokenClient
from trading.private_supervisor import PrivateStreamSupervisor, SupervisorPolicy

limiter = PrivateStreamLimiter()


def receiver_factory(current_journal, shared_limiter):
    capture = JournaledEventCapture(current_journal)
    tokens = PrivateTokenClient(api_key, secret, limiter=shared_limiter)
    return PrivateStreamReceiver(tokens, capture)


runner = PrivateStreamSupervisor(
    control,
    journal,
    book,
    limiter,
    receiver_factory,
    reader.collect_account,
    collect_orders=collect_known_orders,
    policy=SupervisorPolicy(),
)
state = control.snapshot()
runner.run(stop_event, expected_revision=state["revision"], expected_head=journal.head())
```

`api_key`と`secret`は呼出側が明示取得した`SecretStr`、`reader`は期限付きの読取クライアント、
`stop_event`は呼出側の`threading.Event`です。`collect_known_orders`は呼出側が用意する関数で、
通知から得た既知注文IDのtupleを受け取り、対応する`OrderReadReport`のtupleを返します。
`AccountReader.collect_order(intent, order_id)`に渡す注文意図の確認も呼出側が行います。
factoryは指定されたジャーナルとlimiterをそのまま使う、未開始のReceiverを毎回返してください。
直接操作する場合は`start`→`step`の繰返し→`close`でも実行できます。

## REST・切替・停止

| 条件 | 既定値・動作 |
| --- | --- |
| REST取得間隔 | 15秒。通知で前の結果が古くなった場合は最短1秒で前倒し |
| REST期限 | 35秒。期限後に完了した結果も拒否 |
| 取得中の正常な通知 | 古い結果を破棄し、1秒後に再試行。連続5回まで |
| 接続時間 | 8時間で予定切替 |
| 切替余裕 | 記録32件・本文256,000バイト・イベント50件。小さい容量では記録・イベント余裕を調整 |
| ワーカー終了待機 | 2秒。終わらなければ所有権を保持 |

RESTワーカーは非daemonで、同時に1本だけです。期限切れでも強制終了や置換はせず、
先に観測と受信を終了しSTOPPEDを保存します。残ったワーカーがいる間は復旧を拒否します。
ワーカー終了後に`close()`で後片付けを完了できます。スレッド起動中の割込みで起動結果が不明なら、
終了を確認できるまで所有権を保持します。
コールバックには、個々の通信・ページ走査も含めた期限が必要です。

1回の受信処理でpong記録・ACK・通知・ACKの4記録が増えることがあるため、END用の余裕も確保します。
残り5記録未満、本文の最終処理用余裕不足、イベント上限直前ではデータの取出しを短く止め、
最後の照合・計上を待ちます。トークン・pongの処理は続けますが、この間のpongでモニターの期限は延長しません。
ライブラリの有限バッファによるバックプレッシャーは維持し、受信期限が切れれば停止します。

ACKが既知で受信約定がすべて計上済みの場合だけ、受信を終了し、旧区間を保存して、
新しいReceiver・トークン・購読とREST基準を作ります。古いREST結果を新接続へ持ち越しません。
予定切替だけが自動再接続の対象です。通信・保存・計上・残高不一致・期限超過・再試行上限の障害は
永続停止し、[明示復旧](stream-control.md)を必要とします。コールバックの例外文や秘密情報は表示しません。

`collect_reservations`・`reservation_book`、`collect_quote`・`valuation_policy`・`valuation_book`も
既存の同期経路へ渡せます。REST診断の不一致は、現金台帳で一致を再確認できる
`balance_change_unverified`を除き、停止します。

受信前のバッファ内通知や接続間の履歴を網羅したとは扱いません。切替後も
`journal_rollover_gap_not_repaired`を残します。成功回数は受理した診断取得の回数であり、
全口座の証明ではありません。結果は`complete=false`、`live_enabled=false`です。

## 検証

```powershell
uv run pytest tests/test_private_supervisor.py tests/test_stream_control.py -q
```

容量8・9記録で20約定を複数接続にまたがって一度ずつ計上し、各注文のREST取得も1回になること、
時間・イベント・本文量での切替、pongと通知の同時記録、取得中の通知と上限付き再試行、
停止・後片付け不明・残高不一致・遅いREST結果・OSスレッド起動失敗を合成検証します。
