# 📊 SQL最適化レポート

**クエリID**: 01f078e6-dc5c-1a82-902a-652166ae2162  
**レポート生成日時**: 2025-08-17 10:14:02

## 🎯 1. パフォーマンス概要

### 📊 主要指標

| 指標 | 値 | 評価 |
|------|-----|------|
| 実行時間 | 253.6秒 | ⚠️ 改善必要 |
| 読み取りデータ量 | 159.08GB | ⚠️ 大容量 |
| Photon利用率 | 99.4% | ✅ 良好 |
| キャッシュ効率 | 0.0% | ⚠️ 改善必要 |
| フィルタ率 | 5.1% | ⚠️ フィルタ条件の確認が必要 |
| シャッフル影響度 | 50.2%の影響 | ❌ 重大な最適化が必要 |
| スピル発生 | なし | ✅ 良好 |
| スキュー検出 | 検出されず | ✅ 良好 |

### 🚨 主要ボトルネック

1. **シャッフルボトルネック**: JOIN/GROUP BY処理での大量データ転送
2. **キャッシュ効率低下**: データ再利用効率が低い
3. **フィルタ効率低下**: 必要以上のデータを読み込んでいる

## 🐌 2. 処理時間分析

📊 累積タスク実行時間（並列）: 7,373,568 ms (2.0 時間)
📈 TOP10合計時間（並列実行）: 7,320,291 ms

 1. 🔴 Photon Shuffle Exchange
    ⏱️  実行時間: 3,698,225 ms (3698.2 sec) - 累積時間の 50.2%
    📊 処理行数: 0 行
    💾 ピークメモリ: 415120.0 MB
    🔧 並列度: Sink - Tasks total: 1333 | Source - Tasks total: 64
    🔄 Shuffle属性: tpcds.tpcds_sf10000_delta_lc.catalog_sales_demo.cs_bill_customer_sk
    🆔 ノードID: 6309

 2. 🔴 Photon Grouping Aggregate
    ⏱️  実行時間: 1,962,151 ms (1962.2 sec) - 累積時間の 26.6%
    📊 処理行数: 0 行
    💾 ピークメモリ: 491298.0 MB
    🔧 並列度: Tasks total: 1333
    🆔 ノードID: 6232

 3. 🔴 Photon Data Source Scan (tpcds.tpcds_sf10000_delta_lc.catalog_sales_demo)
    ⏱️  実行時間: 1,053,662 ms (1053.7 sec) - 累積時間の 14.3%
    📊 処理行数: 0 行
    💾 ピークメモリ: 22755.6 MB
    🔧 並列度: Tasks total: 1333
    📂 Filter rate: 87.6% (read: 1279.14GB, actual: 159.06GB)
    📊 クラスタリングキー: cs_sold_date_sk
    🆔 ノードID: 6224

 4. 🔴 Photon Grouping Aggregate
    ⏱️  実行時間: 512,669 ms (512.7 sec) - 累積時間の 7.0%
    📊 処理行数: 0 行
    💾 ピークメモリ: 5504.0 MB
    🔧 並列度: Tasks total: 64
    🆔 ノードID: 6311

 5. 🔴 Photon Left Semi Join
    ⏱️  実行時間: 84,455 ms (84.5 sec) - 累積時間の 1.1%
    📊 処理行数: 0 行
    💾 ピークメモリ: 28.0 MB
    🔧 並列度: Tasks total: 1333
    🆔 ノードID: 6228

 6. 🟠 Photon Topk
    ⏱️  実行時間: 7,649 ms (7.6 sec) - 累積時間の 0.1%
    📊 処理行数: 0 行
    💾 ピークメモリ: 436.0 MB
    🔧 並列度: Tasks total: 64
    🆔 ノードID: 6313

 7. 🟢 Photon Shuffle Exchange
    ⏱️  実行時間: 773 ms (0.8 sec) - 累積時間の 0.0%
    📊 処理行数: 0 行
    💾 ピークメモリ: 4.8 MB
    🔧 並列度: Sink - Tasks total: 1 | Source - Tasks total: 1333
    🆔 ノードID: 6226

 8. 🟢 Photon Shuffle Exchange
    ⏱️  実行時間: 293 ms (0.3 sec) - 累積時間の 0.0%
    📊 処理行数: 0 行
    💾 ピークメモリ: 268.0 MB
    🔧 並列度: Sink - Tasks total: 64 | Source - Tasks total: 1
    🆔 ノードID: 6317

 9. 🟢 Columnar To Row (Whole Stage Codegen)
    ⏱️  実行時間: 213 ms (0.2 sec) - 累積時間の 0.0%
    📊 処理行数: 0 行
    💾 ピークメモリ: 0.0 MB
    🔧 並列度: 0 タスク
    🆔 ノードID: 6323

10. 🟢 Photon Data Source Scan (tpcds.tpcds_sf10000_delta_lc.date_dim)
    ⏱️  実行時間: 201 ms (0.2 sec) - 累積時間の 0.0%
    📊 処理行数: 0 行
    💾 ピークメモリ: 16.0 MB
    🔧 並列度: Tasks total: 1
    📂 Filter rate: 54.4% (read: 0.00GB, actual: 0.00GB)
    📊 クラスタリングキー: d_date_sk
    🆔 ノードID: 6016

## 🔧 3. Shuffle操作最適化分析

📊 基準: メモリ/パーティション ≤ 512MB

📊 全体サマリー:
  ・Shuffle操作数: 3
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

1️⃣ 緊急対策 (高優先度):
   - クラスターサイズの拡張 (ワーカーノード数増加)
   - 高メモリインスタンスタイプへの変更
   - spark.sql.adaptive.coalescePartitions.maxBatchSize の調整

2️⃣ 短期対策:
   - spark.sql.adaptive.coalescePartitions.enabled = true
   - spark.sql.adaptive.skewJoin.enabled = true
   - spark.sql.adaptive.advisoryPartitionSizeInBytes の調整
   - 目標: 512MB/パーティション以下

3️⃣ 中長期対策:
   - パーティション数の明示的指定 (.repartition())
   - JOIN戦略の最適化 (ブロードキャストJOINの活用)
   - Liquid Clusteringの実装
   - テーブル設計の最適化

⚙️ 推奨Sparkパラメータ:

```
spark.sql.adaptive.advisoryPartitionSizeInBytes = 268435456
spark.sql.adaptive.coalescePartitions.minPartitionNum = 1
spark.sql.adaptive.coalescePartitions.maxBatchSize = 100
spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes = 268435456
```

## 📋 4. テーブル最適化推奨

### catalog_sales テーブル分析

#### 基本情報
- **テーブルサイズ**: 1279.14GB
- **現在のクラスタリングキー**: cs_sold_date_sk

#### 推奨根拠
- **テーブルサイズ判定**: 大規模テーブルのため強く推奨
- **cs_sold_date_sk**: 
  - フィルター条件として使用 (IS NOT NULL)
  - JOIN条件として使用 (date_dimテーブルとの結合)
  - 現在のクラスタリングキーとして既に設定済み
- **cs_bill_customer_sk**: 
  - GROUP BY句で2回使用されており、重要なグルーピングカラム
  - 追加することでGROUP BY操作の効率が向上

#### 実装SQL
```sql
ALTER TABLE tpcds.tpcds_sf10000_delta_lc.catalog_sales_demo 
CLUSTER BY (cs_sold_date_sk, cs_bill_customer_sk);
OPTIMIZE tpcds.tpcds_sf10000_delta_lc.catalog_sales_demo FULL;
```

#### 期待効果
- クエリ実行時間を約30-40%短縮（253.6秒から約150-180秒程度に改善）
- GROUP BY操作のシャッフル量を大幅に削減（現在のシャッフル影響度50.2%を20-30%程度に低減）

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

## 🚀 5. SQL最適化提案

```sql
USE CATALOG tpcds;
USE SCHEMA tpcds_sf1000_delta_lc;
WITH filtered_dates AS (
SELECT d_date_sk
FROM tpcds.tpcds_sf10000_delta_lc.date_dim
WHERE d_date >= '1990-01-02'
),
repartitioned_sales AS (
SELECT /*+ REPARTITION(811, cs_bill_customer_sk) */
cs.cs_bill_customer_sk,
cs.cs_ext_sales_price,
cs.cs_net_profit
FROM tpcds.tpcds_sf10000_delta_lc.catalog_sales_demo cs
JOIN filtered_dates fd ON cs.cs_sold_date_sk = fd.d_date_sk
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
FROM repartitioned_sales
GROUP BY cs_bill_customer_sk
ORDER BY cs_bill_customer_sk;
```

### 最適化ポイント
1. **事前フィルタリング**: date_dimテーブルを先にフィルタリングしてJOIN
2. **明示的なパーティション設定**: REPARTITIONヒントで最適なパーティション数を指定
3. **必要なカラムのみ選択**: 不要なカラムを除外して中間データ量を削減

### 期待されるパフォーマンス改善効果

#### 予想される改善点
1. **シャッフル最適化**: 20-60%の実行時間短縮
2. **キャッシュ効率向上**: 30-70%の読み込み時間短縮
3. **フィルタ効率改善**: 40-90%のデータ読み込み量削減

**総合改善効果**: 実行時間 253,607ms → 139,484ms（約45%改善）

## 📝 まとめ

### 主要な最適化ポイント
1. **テーブル構造の最適化**: Liquid Clusteringキーの追加（cs_bill_customer_sk）
2. **シャッフル操作の効率化**: パーティション数の増加（810以上）
3. **SQLクエリの最適化**: 事前フィルタリングとREPARTITIONヒントの活用

### 実装優先順位
1. **即時対応**: SQLクエリの最適化（REPARTITIONヒント追加）
2. **短期対応**: Sparkパラメータの調整
3. **中期対応**: テーブル構造の最適化（Liquid Clusteringキーの追加）

*レポート生成時刻: 2025-08-17 10:14:02*