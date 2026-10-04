# Private同期の所有権・永続停止・手動復旧

2026-10-03。`StreamControl`は、区間保存ジャーナルと現金台帳に紐付くローカル制御DBです。
プロセス終了後も停止状態を残し、同じ制御DBを使う同期処理の二重起動をOSロックで拒否します。
実口座の履歴完全性や発注許可は復元しません。

## 初期化と状態

`SegmentedEventJournal`は新規・空・保存区間なし、`ExecutionCashBook`は同じscopeで停止していないことが
必要です。既存の観測用paper口座やversion 1ジャーナルは再作成せず、別の制御ディレクトリを使います。

```python
from trading.stream_control import StreamControl

control = StreamControl.create(control_directory, journal, book)
state = control.snapshot()  # 読取だけ。ネットワーク接続や自動復旧はしない。
# 再起動時は StreamControl(control_directory) で既存DBを開く。
```

`stream-control.sqlite`と`stream-owner.lock`を作ります。ジャーナルのseries・パス・チェックポイント、
現金台帳のinstance・パス、所有者、世代、revision、停止理由、REST成功・再試行回数を保存します。
状態本文のSHA-256は偶発的な破損の検査用です。署名や悪意ある書換えへの防御ではなく、
遷移テーブル自体の独立した暗号学的監査もありません。

| 状態 | 扱い |
| --- | --- |
| READY | 確認したrevisionとジャーナル末尾を指定して、新しい所有者が開始できる |
| RUNNING | ネットワーク操作前に保存。プロセス終了後も期限切れでREADYには戻らない |
| STOPPED | 障害・後片付け不明を保存。再度の正常終了やDBの開き直しでは解除しない |

所有権は安定したロックファイルを使い、受信の後片付けとRESTワーカーの終了まで保持します。
ファイルの欠損・置換・内容変更は拒否し、自動で作り直しません。
同じローカル制御DBを共有する処理だけが対象で、別PC・他アプリのAPI操作は制御しません。
通常のREST回数更新は遷移テーブルを増やしません。開始・終了・切替・復旧などの遷移は10,000件までで、
上限でも履歴は削除せず操作を拒否します。

## 復旧

実行中の所有者が不在であることをOSロックで確認した後、確認したrevision・理由・末尾を指定します。
トークン後片付け不明が残っている場合は、リモート側の残存・失効の扱いを確認して、
`acknowledge_token_uncertainty=True`を明示する必要があります。これは業者側の削除成功を証明しません。

```python
state = control.snapshot()
head = journal.head()
journal = control.recover(
    journal,
    book,
    expected_revision=state["revision"],
    expected_reason=state["reason"],
    expected_head=head,
    acknowledge_token_uncertainty=True,  # 不明なトークンの扱いを確認した場合だけ指定。
)
# 新しいSupervisor・Receiver・Capture・TokenClientで開始し、新しいREST基準を取得する。
```

復旧は全保存区間を監査し、ACK不明、停止中の現金台帳、未計上・内容矛盾の受信約定を拒否します。
旧プロセスが残した開いた区間は、保存した時計条件と計上証拠を確認してENDを追加します。
正常なENDの区間は追記せず保存切替します。ACK不明・FAULT・REJECTEDの区間は、`review_delivery_uncertainty`で
確認の`REVIEWED`を記録し、保存済みの約定をすべて計上した後に保存切替します（[手順](private-sync.md#受渡し結果不明fault区間の確認)）。
世代を進めてREADYに戻しても、接続間に失った約定・入出金・外部操作の履歴は修復しません。

`retire_for_recovery`単独では旧プロセスの不在を証明できません。通常は`control.recover`から使います。
復旧途中に停止した場合も、新しく状態・末尾を読み直して確認します。古い引数で繰り返しません。
新しい接続後も`complete=false`、`live_enabled=false`です。

検証は`uv run pytest tests/test_stream_control.py -q`で実行できます。
実プロセス終了後のOSロック解放、稼働所有者の復旧拒否、永続停止、時計逆行、
未計上約定・ACK不明・破損・ロック置換の拒否を確認します。
