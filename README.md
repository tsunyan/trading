# Trading Lab

FXと日本株向けの検証基盤。Python 3.12 / uv / Backtrader / SQLiteを使用します。
最初はUSD/JPYの1時間足を優先し、日本株は日足CSVによるバックテストから始めます。

採用方針と根拠は [docs/architecture.md](docs/architecture.md) を参照してください。

## 実装済みの範囲

- FX: GMOコインの認証不要APIからBID/ASKの過去足・現在レートを取得。
- FX / 日本株: CSV・Parquetを検証し、同じ移動平均ルールでバックテスト。
- FX: 現在のBid/Askを使うローカル模擬売買。口座開設・APIキーは不要。
- 模擬口座・履歴をSQLiteに保存。同時実行・再起動時にも同じシグナルで買い増ししません。
- 約定履歴・資産曲線・最大ドローダウン・設定・入力データ・ハッシュを保存。

現在は買い持ち/ノーポジションのみ、単一銘柄、円建て、レバレッジなしの会計モデルです。
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
常駐・定期実行はまだ設定していません。プログラムが動いていない間は監視も決済もありません。

同じ確定足では新規判断を繰り返しません。最大ドローダウンの判定だけは、同じ足でも
新しいレートの取得ごとに行います。到達時は模擬決済し、そのDBでは停止状態が続きます。
設定を変える場合は別のDBを指定して別実験にします。
買いはAsk＋滑り、売りはBid−滑りで約定し、両側でAPI手数料相当額を控除します。
価格が30秒超古い、未来時刻、取引時間外、足が古い場合は売買しません。
スプレッド上限は新規買いに適用し、決済を妨げません。

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

GMOのOHLCはBID/ASKを平均した近似Midです。高値・安値は同時に発生したとは限りません。
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
最大ドローダウン停止も次の足での決済なので、損失額の上限を保証するものではありません。

未実装: FXスワップ、レバレッジ・証拠金・強制ロスカット、空売り、配当、企業行動、
板の厚み・部分約定・ストップ高安・売買停止、複数銘柄の資金配分。
取引コストは口座の実条件に合わせて更新してください。とくにFXの持ち越し損益にはスワップ追加が必要です。

`report.json` の収益率は全期間の初期資産に対する変化です。含み損益を含みます。
短期の疎通テスト結果を戦略の収益性の根拠にはしません。

## 開発

```powershell
uv run pytest -q
uv run ruff check .
uv run ruff format --check .
```

依存関係は `uv.lock` で固定。Backtraderは約定エンジンとして利用し、戦略判定と数量決定は
模擬売買と共通の `src/trading/strategy.py` に置いています。
データ・模擬口座・実行結果・秘密情報はGitの対象外です。
