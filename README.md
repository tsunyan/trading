# Trading Lab

FXと日本株向けの検証基盤。Python 3.12 / uv / Backtrader / SQLiteを使用します。
最初はUSD/JPYの1時間足を優先し、日本株は日足CSVによるバックテストから始めます。

採用方針と根拠は [docs/architecture.md](docs/architecture.md) を参照してください。

## 実装済みの範囲

- FX: GMOコインの認証不要APIからBID/ASKの過去足・現在レートを取得。
- FX / 日本株: CSV・Parquetを検証し、同じ移動平均ルールでバックテスト。
- ロング・ショート・見送り、実効レバレッジ、必要証拠金、余力、維持率、強制決済を記録。
- 時刻・方向・付与日数付きの履歴を指定した場合、FXスワップを口座残高へ反映。
- FX: 現在のBid/Askを使うローカル模擬売買。口座開設・APIキーは不要。
- 模擬口座・履歴をSQLiteに保存。同時実行・再起動時にも同じシグナルで買い増ししません。
- 約定履歴・資産曲線・最大ドローダウン・設定・入力データ・ハッシュを保存。
- 完結した取引の実現損益、勝率、期待値、プロフィットファクター、投資比率を保存。
- 固定戦略を連続口座、時系列の独立区間、取引コスト悪化条件で評価し、証拠不足を明示。
- SMAクロス、固定期間モメンタム、固定期間平均回帰を同じ会計で比較可能。

現在は単一銘柄・円建てで、設定により買い、売り、レバレッジを検証できます。
実口座への発注コードはありません。日本株のブローカー接続とJ-Quants自動取得は次段階です。
移動平均ルールは配線・会計の確認用で、収益性を検証した戦略ではありません。

## 起動

PowerShellで実行します。

```powershell
cd D:\GitHub\trading
uv sync --frozen
uv run trading --help
```

合成データで動作確認:

```powershell
uv run trading sample --config configs/fx.toml --output data/fx_sample.csv
uv run trading backtest --config configs/fx.toml --data data/fx_sample.csv --output runs/fx-demo

uv run trading sample --config configs/jp_equity.toml --output data/jp_sample.csv
uv run trading backtest --config configs/jp_equity.toml --data data/jp_sample.csv --output runs/jp-demo
```

合成データには取引カレンダーを再現しない単純な周期変動を使っています。
バックテスト出力先は毎回新しいディレクトリを指定してください。既存結果は上書きしません。

実データを取得して検証:

```powershell
uv run trading fetch-fx --config configs/fx.toml --start 2026-09-07 --end 2026-09-18 --output data/usdjpy.parquet
uv run trading backtest --config configs/fx.toml --data data/usdjpy.parquet --output runs/fx-history
uv run trading evaluate --config configs/fx.toml --data data/usdjpy.parquet --output runs/fx-evaluation
uv run trading evaluate --config configs/fx-short-2x.toml --data data/usdjpy.parquet --output runs/fx-short-2x-evaluation
uv run trading evaluate --config configs/fx-short-2x.toml --data data/usdjpy.parquet --swap-data data/usdjpy_swap.csv --output runs/fx-swap-evaluation
uv run trading compare --config configs/fx-short-2x.toml --config configs/fx-momentum-2x.toml --config configs/fx-mean-reversion-2x.toml --data data/usdjpy.parquet --output runs/fx-strategy-comparison
```

日付はGMOの取引日で、午前6時JSTが切り替わりです。日付範囲は両端を含みます。
未確定の足は取り込みません。通信・APIエラーは失敗として終了し、空データで継続しません。

## FXの模擬売買

```powershell
uv run trading paper-step --config configs/fx.toml --database runs/paper.sqlite
uv run trading paper-status --database runs/paper.sqlite
```

`paper-step` は直近8取引日の日付範囲から確定足を取り、現在レートを取得して1回判断します。
週末を含むため実際の取引日はそれより少なくなる場合があります。
これは履歴を高速再生する機能ではなく、呼び出した時点でのフォワード模擬売買です。
過去のシグナルをまとめて約定させる処理や、停止中の売買の補完は行いません。
CLI自体は常駐せず、1回だけ判断して終了します。現在のローカル環境では外部の毎時自動実行を
設定していますが、この設定はリポジトリには含まれません。自動実行が止まっている間は監視も決済もありません。

同じ確定足では新規判断を繰り返しません。最大ドローダウンの判定だけは、同じ足でも
新しいレートの取得ごとに行います。到達時は模擬決済し、そのDBでは停止状態が続きます。
設定を変える場合は別のDBを指定して別実験にします。
買いはAsk＋滑り、売りはBid−滑りで約定し、両側でAPI手数料相当額を控除します。売り建玉は
Askで時価評価し、Ask＋滑りで買い戻します。両方向・2倍上限の検証には
`configs/fx-short-2x.toml`と既存口座とは別のDBを使います。
スワップ履歴を使う模擬口座も、その履歴のハッシュに紐づく別DBを指定します。
価格が60秒超古い、10秒超の未来時刻、取引時間外、足が古い場合は売買しません。
スプレッド上限は新規建てに適用し、決済を妨げません。

`runs/fx-paper-smoke.sqlite` は開発時の疎通確認用です。通常の検証には新しいDBを使います。

## データ形式

```csv
timestamp,symbol,open,high,low,close,volume
2025-01-06T00:00:00+00:00,USD_JPY,150.0,150.2,149.8,150.1,0
```

タイムゾーン付きの時刻・単一銘柄・昇順・重複なしが必須です。欠損・無限大・不正なOHLCを拒否します。
FXは足の開始時刻、日本株の日足は取引日ラベル（例: 午前0時JST）です。
日足の日時は実際の注文送信時刻ではありません。株数と価格の単位は1株、FXは1米ドルです。
FXには出来高がないため0を使用します。

GMOのOHLCはBID/ASKを平均した近似Midです。取得データには元の`bid_*`、`ask_*` OHLCと
`received_at`も保存します。高値・安値は同時に発生したとは限りません。
バックテストは設定した固定スプレッドの半分＋滑りを各約定価格へ不利な方向に加えます。
模擬売買では観測したBid/Askを使うため、両者の約定価格は一致するとは限りません。

日本株は初期設定で100株単位ですが、銘柄の売買単位に合わせて設定してください。
株式分割・配当・上場廃止は自動処理しません。調整済み価格を実際の約定価格として扱うと
株数・資金計算が不正確になるため、分割をまたぐ検証には企業行動の処理が必要です。

## 計算上の範囲

バックテストでは足の終値確定後に判断し、次の足の始値で約定します。
最終足の未約定注文は残し、建玉は終値で時価評価します。最終日に強制決済しません。
数量は判断時の資産と価格から決定し、次の寄り付きで資金不足ならBacktraderが注文を拒否します。
価格の窓開けによって約定後の保有比率が設定値を超えることはあります。
最大ドローダウンまたは維持証拠金率による停止も次の足での決済なので、損失額の上限を保証しません。
`allocation`は証拠金へ割り当てる資産比率、`max_leverage`は建玉額に使える倍率です。

FXスワップは履歴を`--swap-data`で指定した場合だけ反映します。履歴の自動取得は未実装です。
未実装: 株の貸株可否・貸株料、配当、企業行動、板の厚み・部分約定、
ストップ高安・売買停止、複数銘柄の資金配分。
取引コストは口座の実条件に合わせて更新してください。とくにFXの持ち越し損益にはスワップ追加が必要です。

`report.json` の収益率は全期間の初期資産に対する変化です。含み損益を含みます。
`performance` は完結した取引だけを対象とし、`trades.csv` と対応します。未決済の建玉は
実現損益・勝率・プロフィットファクターには含めません。取引回数が少ない結果や、損失取引が
ないため `profit_factor` が `null` の結果は、収益性の根拠にしません。
短期の疎通テスト結果を戦略の収益性の根拠にはしません。

`evaluate` は先頭48本をウォームアップに使い、残りを一本の連続口座で評価すると同時に、
既定で3つの時系列区間へ分けて診断します。
各区間は同額の現金・建玉なしで開始し、区間より前の足だけをウォームアップに使うため、
将来データは売買判断に入りません。通常コストと、手数料・スプレッド・滑りを2倍にした条件を
同じ期間で比較します。昇格判定は通常・悪化条件の連続収益、DD、停止と区間一貫性を確認します。
`report.json` の `verdict.status` は `insufficient_evidence`、`rejected`、
`candidate` のいずれかです。`candidate` は最低限の研究条件を通った意味で、利益保証ではありません。
`compare`の順位も閲覧済みデータ上の記述値で、実運用の承認や未使用テスト結果ではありません。
連続・区間別の資産、注文、完結取引は`evaluation_scope`列で区別して監査できます。
同じ開始時点・配分・倍率・コストによる現金と買い持ちを`benchmarks`へ保存します。
買い持ちは期末時価と決済費用込み価値を分けます。`data_quality`には足間隔の空白を記録しますが、
休場と取得障害の分類はまだ行わないため、空白数だけで評価を合否判定しません。

スワップ履歴は以下の列を持つCSVまたはParquetです。金額は付与イベントごとの
1万通貨あたり円額で、プラスを受取、マイナスを支払として記録します。`days`はその金額が
何日分かを示す監査列で、金額へ再乗算しません。`timestamp`は付与イベントの実時刻です。

```csv
timestamp,symbol,long_jpy_per_10k,short_jpy_per_10k,days
2026-09-23T21:00:00+00:00,USD_JPY,0,0,1
```

上のゼロ値は形式例で、実際の検証には対象期間の公式履歴を使います。現在値を過去へ一律適用しません。

保存レポートには設定・データ・スワップに加え、`src/trading`のコードハッシュ、Gitコミット、
dirty状態、これらから生成した`experiment_id`を記録します。戦略名が同じでもコードや入力が変われば
別実験として扱います。

## 開発

```powershell
uv run pytest -q
uv run ruff check .
uv run ruff format --check .
```

依存関係は `uv.lock` で固定。Backtraderは約定エンジンとして利用し、戦略判定と数量決定は
模擬売買と共通の `src/trading/strategy.py` に置いています。
データ・模擬口座・実行結果・秘密情報はGitの対象外です。
