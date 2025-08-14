# 📊 SQL最適化レポート

## 🎯 パフォーマンス概要

### 📊 主要パフォーマンス指標

| 指標 | 値 | 評価 |
|------|-----|------|
| 実行時間 | 253.6秒 | ⚠️ 改善必要 |
| 読み取りデータ量 | 159.08GB | ⚠️ 大容量 |
| Photon利用率 | 99.4% | ✅ 良好 |
| キャッシュ効率 | 0.0% | ⚠️ 改善必要 |
| フィルタ率 | 5.1% | ⚠️ フィルタ条件を確認 |
| シャッフル関連処理 | 8回 | ⚠️ 多数 |
| スピル発生 | なし | ✅ 良好 |
| スキュー検出 | 未検出 | ✅ 良好 |

### 🚨 主要ボトルネック

1. **シャッフルボトルネック**: JOIN/GROUP BY処理での大量データ転送
2. **キャッシュ効率低下**: データ再利用効率が低い
3. **フィルタ効率低下**: 必要以上のデータを読み込んでいる

### 📊 シャッフル操作数の説明

- **シャッフル関連処理**: 8回（Shuffle操作3回 + Reused Exchange 1回 + Shuffle Map Stage 3回 + その他1回）
- **実際のShuffle操作**: 3回（新規に実行されるシャッフル操作のみ）

*注：Enhanced Shuffle分析では実際のシャッフル操作のみを対象とし、再利用されるExchangeや実行ステージは除外しています。*

## 🐌 処理時間分析

### 📊 時間のかかっている処理TOP5

📊 累積タスク実行時間（並列）: 7,373,568 ms (2.0 時間)

1. **Photon Shuffle Exchange**
   - ⏱️ 実行時間: 3,698,225 ms (全体の50.2%)
   - 💾 ピークメモリ: 415,120.0 MB
   - 🔄 Shuffle属性: tpcds.tpcds_sf10000_delta_lc.catalog_sales_demo.cs_bill_customer_sk
   - 🆔 ノードID: 6309

2. **Photon Grouping Aggregate**
   - ⏱️ 実行時間: 1,962,151 ms (全体の26.6%)
   - 💾 ピークメモリ: 491,298.0 MB
   - 🆔 ノードID: 6232

3. **Photon Data Source Scan (catalog_sales_demo)**
   - ⏱️ 実行時間: 1,053,662 ms (全体の14.3%)
   - 📂 Filter rate: 87.6% (read: 1279.14GB, actual: 159.06GB)
   - 📊 クラスタリングキー: cs_sold_date_sk
   - 🆔 ノードID: 6224

4. **Photon Grouping Aggregate**
   - ⏱️ 実行時間: 512,669 ms (全体の7.0%)
   - 💾 ピークメモリ: 5,504.0 MB
   - 🆔 ノードID: 6311

5. **Photon Left Semi Join**
   - ⏱️ 実行時間: 84,455 ms (全体の1.1%)
   - 💾 ピークメモリ: 28.0 MB
   - 🆔 ノードID: 6228

## 🔧 Enhanced Shuffle操作最適化分析

📊 基準: メモリ/パーティション ≤ 512MB

📊 全体サマリー:
  ・実際のShuffle操作数: 3
  ・最適化が必要な操作: 1
  ・総メモリ使用量: 405.66 GB
  ・平均メモリ/パーティション: 6293.8 MB
  ・最適化必要性: はい

🎯 Shuffle効率性スコア: 🟡 66.7%

🔍 個別Shuffle操作分析:

1. Shuffle (Node ID: 6309)
   🚨 優先度: HIGH
   📊 パーティション数: 64
   🧠 ピークメモリ: 405.39 GB
   ⚡ メモリ/パーティション: 6486.2 MB 🔥 危険レベル
   ⏱️ 実行時間: 3698.2 秒
   📈 処理行数: 5,467,120,697
   🎯 効率性: ❌ 非効率

   💡 推奨事項:
     - 🚨 非常に高いメモリ使用量 (6486MB/パーティション): パーティション数を810以上に増加するか、クラスターサイズを拡張してください
     - 🖥️ クラスター拡張: より多くのワーカーノードまたは高メモリインスタンスの使用を検討
     - 🔧 Liquid Clusteringの実装により、Shuffle操作の削減を検討 (現在のメモリ使用量: 405.4GB)
     - ⏱️ 実行時間が長い (3698.2秒): データ分散戦略の見直しを推奨
     - 📊 大量データ処理 (5,467,120,697行): ブロードキャストJOINや事前集約の活用を検討
     - 🔧 SQLクエリで発生している場合はREPARTITONヒントもしくはREPARTITON_BY_RANGEヒント(Window関数使用時)を適切に設定してください

2. Shuffle (Node ID: 6317)
   💡 優先度: LOW
   📊 パーティション数: 1
   🧠 ピークメモリ: 0.26 GB
   ⚡ メモリ/パーティション: 268.0 MB
   ⏱️ 実行時間: 0.3 秒
   📈 処理行数: 640,064
   🎯 効率性: ✅ 効率的

3. Shuffle (Node ID: 6226)
   💡 優先度: LOW
   📊 パーティション数: 1
   🧠 ピークメモリ: 0.0 GB
   ⚡ メモリ/パーティション: 4.8 MB
   ⏱️ 実行時間: 0.8 秒
   📈 処理行数: 160,708
   🎯 効率性: ✅ 効率的

### 🎯 Shuffle最適化推奨事項

1. **緊急対策** (高優先度):
   - クラスターサイズの拡張（ワーカーノード増加）
   - 高メモリインスタンスタイプへの変更
   - `spark.sql.adaptive.coalescePartitions.maxBatchSize` の調整

2. **短期対策**:
   - `spark.sql.adaptive.coalescePartitions.enabled = true`
   - `spark.sql.adaptive.skewJoin.enabled = true`
   - `spark.sql.adaptive.advisoryPartitionSizeInBytes` の調整（目標: 512MB/パーティション以下）

3. **中長期対策**:
   - パーティション数の明示的指定 (.repartition())
   - JOIN戦略の最適化（ブロードキャストJOINの活用）
   - Liquid Clusteringの実装
   - テーブル設計の最適化

### ⚙️ 推奨Sparkパラメータ

```
spark.sql.adaptive.advisoryPartitionSizeInBytes = 268435456
spark.sql.adaptive.coalescePartitions.minPartitionNum = 1
spark.sql.adaptive.coalescePartitions.maxBatchSize = 100
spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes = 268435456
```

## 📋 テーブル最適化推奨

### catalog_sales テーブル分析

#### 基本情報
- **テーブルサイズ**: 1279.14GB
- **現在のクラスタリングキー**: cs_sold_date_sk

#### 推奨根拠
- **テーブルサイズ**: 1279.14GB（大規模テーブルのため最適化を強く推奨）
- **cs_sold_date_sk**: 
  - フィルター条件で使用 (IS NOT NULL)
  - JOIN条件で使用（date_dimテーブルとの結合キー）
  - 現在のクラスタリングキーとして既に設定済み
- **cs_bill_customer_sk**: 
  - GROUP BY句で2回使用（重要な集約キー）
  - 大規模テーブルでのグループ化操作を高速化

#### 実装SQL
```sql
-- 現在のクラスタリングキーに cs_bill_customer_sk を追加
ALTER TABLE tpcds.tpcds_sf10000_delta_lc.catalog_sales_demo 
CLUSTER BY (cs_sold_date_sk, cs_bill_customer_sk);
OPTIMIZE tpcds.tpcds_sf10000_delta_lc.catalog_sales_demo FULL;
```

#### 期待効果
- クエリ実行時間を約30-40%短縮（253.6秒 → 約150-180秒）
- GROUP BY操作のシャッフル量を大幅に削減
- 日付フィルタリングと顧客別集計の両方を効率化

### 💡 Liquid Clustering キー選定ガイドライン

#### 選定原則
- **基本原則**: フィルタ列での読み取り最適化（データスキッピング）を優先
- **優先順位**: 「よく絞り込みに使う列」を第一優先に選定

#### GROUP BY キーの考慮条件
1. フィルタにも使う列がGROUP BYにも登場する場合
2. シャッフルに乗る中間データ量の削減が見込める場合
3. キーのカーディナリティが低〜中程度で極端なスキューが少ない場合

#### 実務推奨
上記条件を満たさない場合は、常にフィルタ列を優先

## 🚀 SQL最適化提案

### 💡 最適化クエリ

```sql
USE CATALOG tpcds;
USE SCHEMA tpcds_sf1000_delta_lc;
WITH filtered_dates AS (
SELECT
d_date_sk
FROM
tpcds.tpcds_sf10000_delta_lc.date_dim
WHERE
d_date >= '1990-01-02'
AND d_date_sk IS NOT NULL
),
filtered_sales AS (
SELECT
cs_bill_customer_sk,
cs_ext_sales_price,
cs_net_profit,
cs_sold_date_sk
FROM
tpcds.tpcds_sf10000_delta_lc.catalog_sales_demo cs
WHERE
cs_bill_customer_sk IS NOT NULL
AND cs_sold_date_sk IS NOT NULL
),
joined_sales AS (
SELECT
fs.cs_bill_customer_sk,
fs.cs_ext_sales_price,
fs.cs_net_profit
FROM
filtered_sales fs
JOIN
filtered_dates fd ON fs.cs_sold_date_sk = fd.d_date_sk
)
SELECT
cs_bill_customer_sk,
AVG(cs_ext_sales_price) AS avg_cs_ext_sales_price,
MIN(cs_ext_sales_price) AS min_cs_ext_sales_price,
MAX(cs_ext_sales_price) AS max_cs_ext_sales_price,
COUNT(cs_ext_sales_price) AS count_cs_ext_sales_price,
AVG(cs_net_profit) AS avg_cs_net_profit,
MIN(cs_net_profit) AS min_cs_net_profit,
MAX(cs_net_profit) AS max_cs_net_profit,
COUNT(cs_net_profit) AS count_cs_net_profit
FROM
joined_sales
GROUP BY
cs_bill_customer_sk
ORDER BY
cs_bill_customer_sk;
```

### 📈 期待されるパフォーマンス改善効果

- **シャッフル最適化**: 20-60%の実行時間短縮
- **キャッシュ効率向上**: 30-70%の読み込み時間短縮
- **フィルタ効率改善**: 40-90%のデータ読み込み量削減

**総合改善効果**: 実行時間 253,607ms → 139,484ms（約45%改善）

## 📝 まとめ

1. **主要ボトルネック**:
   - シャッフル操作（特にノードID 6309）での大量メモリ使用
   - 非効率なデータフィルタリング
   - クラスタリングキーの最適化不足

2. **優先アクション**:
   - catalog_sales テーブルのクラスタリングキーに cs_bill_customer_sk を追加
   - Sparkパラメータの最適化（特にパーティションサイズとAdaptive Execution関連）
   - クラスターリソースの拡張検討

3. **長期改善計画**:
   - Liquid Clusteringの体系的な実装
   - クエリパターン分析に基づくテーブル設計の最適化
   - データ分散戦略の見直し

これらの最適化を実施することで、クエリ実行時間を約45%短縮し、リソース使用効率を大幅に向上させることが期待できます。