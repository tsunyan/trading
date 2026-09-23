`tsunyan/trading` を、現在の `main` と PR #1 の HEAD `f308d15` まで確認しました。結論から言うと、**研究用の土台としてはかなり筋が良い一方、「金儲けできる自動取引システム」という目的に対しては、まだ戦略を評価するための研究基盤が足りません**。ブローカー発注やAI以前に、まず「その戦略に本当に再現可能なエッジがあるのか」を判定できる仕組みを作るのが最優先です。

なお PR #1 は現在、`tests / lint / secrets / CodeQL analyze` がすべて成功しています。以前失敗していた CodeQL は直っています。ただし未解決のロジック問題が1件あるので、**現状の PR #1 はそのまま merge しない方がよい**です。

## 総評

| 領域            | 現状                                                    | 評価        |
| ------------- | ----------------------------------------------------- | --------- |
| コード構造         | `data / strategy / backtest / paper / broker API` が分離 | 良い        |
| データ検証         | timezone、重複、OHLC、不正値をかなり厳格にチェック                       | 良い        |
| バックテスト        | Backtrader＋コスト＋次足約定                                   | 基礎は良い     |
| Paper trading | SQLite、排他、重複約定防止あり                                    | 良いがバグあり   |
| 再現性           | config/data hash、lockfile、run保存                       | 良い出発点     |
| CI / Security | pytest、ruff、gitleaks、CodeQL、Dependabot                | 良い        |
| 収益性検証         | SMA 1戦略＋単一バックテスト                                      | **大幅に不足** |
| OOS検証         | なし                                                    | **必須**    |
| コストモデル        | 固定spread/slippage、swapなし                              | **不足**    |
| リスクモデル        | DD停止のみ                                                | **不足**    |
| 実発注           | なし                                                    | 未着手       |
| AI/ML         | なし                                                    | 未着手       |
| 運用監視          | なし                                                    | 未着手       |

現在は「自動売買システム」というより、正確には **quant research / paper trading の骨格**です。

---

# 1. コードレビュー

一番重要なのは `paper.py` のこの部分です。

```python
age = (now - quote.timestamp).total_seconds()

...

close_times = frame.timestamp + pd.Timedelta(seconds=cfg.bar_seconds)
completed = frame.loc[close_times <= pd.Timestamp(quote.timestamp)]
```

PR #1 で GMO の時計が最大10秒未来でも許容するようになっています。

すると例えば、

```text
now             10:00:00
quote.timestamp 10:00:10
```

でも quote は有効です。

ところが確定足判定も `quote.timestamp` を使っているため、

```text
10:00:00 < close_time <= 10:00:10
```

に入る足を「確定済み」と認識できてしまいます。

これは典型的な **look-ahead / future leakage** です。

CodeRabbit の指摘は正しいです。

ここは、

```python
observation_time = pd.Timestamp(now)

completed = frame.loc[close_times <= observation_time]

signal_time = completed.timestamp.iloc[-1] + pd.Timedelta(
    seconds=cfg.bar_seconds
)

if (observation_time - signal_time).total_seconds() > cfg.max_signal_age_seconds:
    ...
```

にするべきです。

未来時刻許容はあくまで、

```python
quote が10秒くらい未来でも通信時計差として許す
```

だけに限定すべきです。

### データ検証にも重要な穴がある

`validate_bars()` は、

```python
frame.timestamp.diff() < cfg.bar_seconds
```

は拒否しますが、

```text
01:00
02:00
03:00
12:00
13:00
```

のような巨大な欠落は許容します。

移動平均戦略ではこれはかなり危険です。

48本移動平均が、

```text
48時間
```

ではなく、

```text
欠損を含む48 observations
```

になります。

FXなら週末は正規のギャップですが、API障害による3時間欠損は別物です。

なので、

```text
expected calendar
actual bars
↓
missing / unexpected gap detection
```

を追加した方がいいです。

FXなら取引セッションカレンダー、日本株ならJPXカレンダーを持たせます。

---

`completed_trades()` のFIFO処理自体は、**現在の「買い増しなし・ロングのみ」モデルでは妥当**です。

ただし、

```python
units = int(fill.filled_units)

...

remaining = -units
exit_commission_per_unit = float(fill.commission) / remaining
```

なので、万一

```python
filled_units == 0
status == Completed
```

のデータが入ればゼロ除算になります。

現状Backtraderでは普通発生しませんが、将来実ブローカーの fill データを流用するときには、

```python
if units == 0:
    raise ValueError(...)
```

程度の防御は入れたいです。

また将来買い増しを許可すると、

```text
100買う
100買う
200売る
```

は FIFO 上 `trades` が2件になります。

その場合、

```python
closed_trades = len(trades)
win_rate
average_trade_pnl
```

の「trade」が何を意味するのか曖昧になります。

今は問題ありませんが、将来、

```text
fill
lot
position
round trip trade
```

を別概念にした方がいいです。

---

もう一つ、将来的に結構効いてくるのがこれです。

```python
paper_tolerance_legacy_fingerprints
```

設定変更への互換処理としてはよく考えられていますが、この方式を続けると、

```text
config変更A
config変更B
config変更C
...
```

ごとに過去fingerprintを列挙することになります。

これは早めに、

```text
database_schema_version
strategy_version
execution_model_version
config_hash
```

を分離した方がいいです。

例えば、

```text
schema_version = 2
strategy_id = "sma_cross_v1"
execution_model = "gmo_fx_v1"
config_hash = ...
```

のようにします。

**設定の同一性とDBマイグレーションをSHAだけに背負わせない**方が長期的には安全です。

---

# 2. バックテスト設計

ここはかなり重要です。

現在のバックテストには良い点があります。

次足始値約定、spread/slippage、commission、未約定注文を無理に最終決済しない、future price が過去注文に影響しないことをテストしている、という設計はまともです。

特に、

```text
signal → next bar execution
```

を明示している点は非常に良いです。

ありがちな、

```text
終値を見て
↓
同じ終値で買う
```

というバックテスト詐欺になっていません。

ただし、収益性を判断するには指標がまだ全然足りません。

PR #1 の、

```text
win rate
profit factor
realized P/L
average trade
exposure
```

追加は正しい方向ですが、最低でも次が欲しいです。

| 指標                       | 理由            |
| ------------------------ | ------------- |
| CAGR / annualized return | 期間の違う戦略を比較    |
| annualized volatility    | リターンだけでは危険度不明 |
| Sharpe                   | リスク調整後収益      |
| Sortino                  | 下方リスク重視       |
| Max Drawdown             | 既にあり          |
| Max DD duration          | 回復まで何日耐えるか    |
| Calmar                   | CAGR / MaxDD  |
| expectancy               | 1取引あたり期待値     |
| avg win / avg loss       | 勝率だけでは意味がない   |
| payoff ratio             | 利益幅/損失幅       |
| turnover                 | コスト感応度        |
| holding period           | swap等に直結      |
| consecutive losses       | 資金・心理耐性       |
| MAE / MFE                | stop/exit改善   |
| return by year/month     | 特定期間依存の検出     |

特に、

```text
勝率 70%
```

なんて単独ではほぼ意味がありません。

---

# 3. 「金儲け」という目的に対して最大の不足

ここが本題です。

今のコードには、

```python
fast SMA > slow SMA
```

しかありません。

README自身にも書かれている通り、これは配線確認用です。

つまり現在は、

**「儲かるシステムを作った」のではなく「儲かる戦略が見つかったら検証できるシステムを作り始めた」段階**

です。

そして次にAIを追加するより先に、以下を作るべきです。

1. **Train / Validation / Test の完全な時系列分離**
   例えば2015–2022で研究、2023–2024でチューニング、2025–2026を最後まで触らないOOSにする。

2. **Walk-forward analysis**
   `train → test → train → test` を時系列で繰り返す。相場はstationaryではないので、1回のtrain/testだけでは弱いです。

3. **Benchmark**
   SMAが儲かったとしても、それが意味のあるedgeなのか比較対象が必要です。少なくともCash、Buy & Hold、単純Momentumなど。

4. **コスト・ストレステスト**
   spread ×1 / ×1.5 / ×2、slippage ×1 / ×2 / ×3 でも残る利益なのかを見る。

5. **Bootstrap / Monte Carlo**
   取引順を入れ替えたり、returnsをbootstrapして「たまたま勝っただけ」の可能性を見る。

6. **parameter sensitivity**
   `12/48` だけ儲かり、`11/47`, `13/49` は全滅なら過剰適合を疑う。広い領域で利益が残る戦略の方が信用できます。

7. **複数期間・複数regime**
   円高、円安、高vol、低vol、金融危機、レンジ、trend。

ここまでできて初めて、

```text
strategy candidate
```

と呼べます。

---

# 4. FXモデルとして不足しているもの

現在、

```text
JPY cash
USD/JPY
leverageなし
allocation 20%
```

というモデルです。

これはロジック検証にはよいですが、**実際の国内FX取引とは経済構造が違います**。

最大の不足は、

```text
margin
leverage
swap
forced liquidation
maintenance margin
```

です。

特に1時間足SMAならポジションを数日以上持つ可能性があります。

そのときswapを無視すると成績が変わります。

さらに実際のFXなら、

```text
100万円
20万円分だけUSDJPYを買う
```

という現物外貨のようなモデルではなく、証拠金ベースになります。

したがって、実運用を想定する段階では、

```text
Position
MarginAccount
FinancingModel
ExecutionCostModel
```

を分離した方がいいです。

---

# 5. AIについて

現在のリポジトリには **AI/MLは一切ありません**。

これは今の段階ではむしろ問題ありません。

いま、

```text
PyTorch
XGBoost
LLM
Transformer
```

を入れても、バックテスト基盤が過剰適合を見抜けなければ、

> AIが過去データを上手に暗記している

だけになります。

AIを入れる順番は、

```text
データ基盤
→ 正しいバックテスト
→ OOS / walk-forward
→ ベースライン
→ feature engineering
→ ML
```

がよいです。

最初のMLならLLMではなく、

```text
LightGBM / XGBoost
```

あたりが適しています。

例えば、

```text
momentum
volatility
ATR
MA distance
RSI
return distribution
time-of-day
spread
rolling high/low
```

をfeatureにして、

```text
次N時間の期待return
```

を推定します。

重要なのはaccuracyではありません。

目的関数は、

```text
予測精度
```

ではなく最終的には、

```text
after-cost expected return
risk-adjusted return
```

です。

---

# 6. Paper → Live で不足するもの

実発注に進む前には、最低でも次が必要です。

| 機能                            | 現状         |
| ----------------------------- | ---------- |
| broker adapter                | ×          |
| client order ID / idempotency | ×          |
| 注文照合                          | ×          |
| position reconciliation       | ×          |
| partial fill                  | ×          |
| reject / cancel / expire      | ×          |
| retry policy                  | ×          |
| API rate-limit                | ×          |
| connection recovery           | ×          |
| heartbeat                     | ×          |
| kill switch                   | ×          |
| max position                  | △ configのみ |
| max daily loss                | ×          |
| max order size                | △          |
| stale-data circuit breaker    | ○          |
| quote sanity check            | ○          |
| alerting                      | ×          |
| structured logging            | ×          |
| scheduler                     | ×          |
| NTP / clock drift monitoring  | ×          |

特に本番で重要なのは、

```text
ローカルDBでは1000 USD持っている
Brokerでは0 USD
```

のような不整合を絶対に放置しないことです。

本番では、

```text
broker = source of truth
```

にして、起動時・注文後・定期的にreconciliationする必要があります。

SQLite自体は個人運用なら十分使えます。

---

# 7. リポジトリ設定レビュー

PR #1 の改善はかなり良いです。

現在確認できたCIは、

```text
tests    success
lint     success
secrets  success
analyze  success
```

です。

また、

```yaml
persist-credentials: false
```

SHA pin、

```text
gitleaks
CodeQL
Dependabot
timeout-minutes
```

まで入っているので、この規模の個人リポジトリとしてはかなり堅いです。

ただし1点、再現性上の穴があります。

```yaml
python -m pip install --upgrade pip uv
```

です。

プロジェクト依存は `uv.lock` で固定しているのに、**そのlockfileを解釈するuv自身は最新バージョンを毎回取っています**。

例えば、

```text
uv==0.x.y
```

を明示するか、SHA pinした `astral-sh/setup-uv` ＋明示versionにした方がいいです。

さらに、

```text
pytest
ruff
gitleaks
CodeQL
```

に加えて、

```text
dependency vulnerability scan
type checking
coverage
```

を追加したいです。

例えば、

```text
Pyright
pytest-cov
OSV-Scanner / pip-audit
```

あたりです。

---

もう一つ重要なのが、**リポジトリがpublic**であることです。

今はSMAのテスト戦略なので何の問題もありません。

ただし本気でedgeを探し始めたら、

```text
features
strategy
parameters
execution tricks
```

そのものが資産になります。

「金儲け」が目的なら、戦略研究を始める段階で private にするのは合理的です。

API keyについてはもちろんGitHub Secretsだけの問題ではなく、本番実行環境側のsecret storeに置くべきです。

現在の、

```gitignore
.env
.env.*
```

は適切です。

---

GitHubのRulesets APIは現在空でした。一方、PR説明では classic branch protection に

```text
tests
lint
secrets
analyze
```

をrequiredとしているとのことです。

GitHub Appの権限制約で branch-protection API 自体は403になったので、**その設定だけは私から直接検証できていません**。

ここはGitHub Settings上で、

```text
Require pull request
Require status checks
Require branch up to date
Do not allow force pushes
Do not allow deletions
```

あたりを確認しておくとよいです。

---

# 8. 今、一番足りないもの

実装優先度を付けるならこうです。

|    優先度 | 実装                              | 理由               |
| -----: | ------------------------------- | ---------------- |
| **P0** | PR #1 の未来quote問題修正              | 現在の正当性バグ         |
| **P0** | 時系列 train/validation/test       | 収益性評価の前提         |
| **P0** | walk-forward runner             | 過剰適合検出           |
| **P0** | benchmark / strategy comparison | edge判定           |
| **P0** | 長期間の実データ保存                      | サンプル不足           |
| **P1** | Sharpe/Sortino/CAGR/Calmar等     | 評価指標             |
| **P1** | cost sensitivity                | 見せかけのedge排除      |
| **P1** | gap/calendar検証                  | データ品質            |
| **P1** | FX swap/marginモデル               | 現実との乖離を縮小        |
| **P1** | experiment registry             | cherry-picking防止 |
| **P2** | scheduler + monitoring          | forward test     |
| **P2** | ML feature/model framework      | AI研究             |
| **P2** | broker execution/reconciliation | 実弾投入             |
| **P3** | portfolio/multi-strategy        | 分散               |
| **P3** | live capital                    | 最後               |

特に重要なのは、**Broker APIを作るより先にResearch Runnerを作ること**です。

例えば最終的には、

```powershell
uv run trading experiment `
  --strategy sma `
  --data data/usdjpy-2015-2026.parquet `
  --walk-forward `
  --train-years 3 `
  --test-months 6 `
  --grid fast=6,12,18 slow=24,48,72
```

のようにするとよいです。

出力を、

```text
runs/
  experiment-id/
    manifest.json
    folds.csv
    trades.csv
    equity.csv
    metrics.json
    parameters.json
```

として、

```text
どのデータ
どのcommit
どのparameters
どのstrategy version
どのcost model
```

から結果が生成されたのか完全に固定します。

---

## 現段階での判断

**ソフトウェアとしてはかなりまともなスタートです。**

特に、

```text
入力検証
次足約定
コスト考慮
paper tradingの排他制御
状態永続化
再現性情報
CI
secret scan
CodeQL
```

を最初から入れているので、「とりあえずAIに売買させてみた」系よりは遥かに健全です。

一方で**収益システムとして見ると、最大の未実装部分はAIでも実発注でもなく「科学的にedgeを証明する仕組み」**です。

なので私なら次は、

**① PR #1 の時刻バグ修正 → ② Research/Experiment層 → ③ walk-forward＋OOS → ④ metrics強化 → ⑤ 長期USD/JPYデータでSMAをbaseline化**

まで一気に進めます。

その後に初めて、SMAを基準として **XGBoost/LightGBMなどのML戦略が本当に改善しているか**を比較します。これが「AI自動取引で金を稼ぐ」という目的に対して、一番遠回りに見えて実際には最短です。
