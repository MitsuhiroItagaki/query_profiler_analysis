# 📊 SQL最適化レポート

**クエリID**: 01f0781a-9829-1759-87af-8728c6f54da4  
**レポート生成日時**: 2025-08-13 18:55:20

## 🎯 パフォーマンス概要

### 主要指標

| 指標 | 値 | 評価 |
|------|-----|------|
| 実行時間 | 3174.9秒 | ⚠️ 改善必要 |
| 読み取りデータ量 | 238.51GB | ⚠️ 大容量 |
| Photon利用率 | 99.6% | ✅ 良好 |
| キャッシュ効率 | 0.0% | ⚠️ 改善必要 |
| フィルタ率 | 0.0% | ⚠️ 確認必要 |
| シャッフル操作 | 4回 | ✅ 良好 |
| スピル発生 | あり | ❌ 問題あり |
| スキュー検出 | 検出なし | ✅ 良好 |

### 主要ボトルネック

1. **シャッフル最適化**: 1/3のShuffle操作で最適化が必要（効率性: 66.7%）
2. **パーティション設計**: Node ID 13708で4288MB/パーティションの危険レベル
3. **実行時間**: 最長2748.4秒のShuffle処理による大幅な遅延
4. **データ転送量**: 445.07GB の大量Shuffleメモリ使用

## 🐌 処理時間分析

### 最も時間がかかっている処理TOP5

1. **Photon Shuffle Exchange**
   - 実行時間: 13,370,118 ms (52.9%)
   - スピル: 131.87 GB
   - Shuffle属性: tpcds.tpcds_sf10000_delta_lc.catalog_sales.cs_bill_customer_sk

2. **Photon Grouping Aggregate**
   - 実行時間: 5,647,672 ms (22.4%)
   - 並列度: 3729タスク

3. **Photon Grouping Aggregate**
   - 実行時間: 3,397,111 ms (13.4%)
   - 並列度: 16タスク

4. **Photon Data Source Scan (catalog_sales)**
   - 実行時間: 2,727,485 ms (10.8%)
   - フィルタ率: 80.5% (読込: 1220.35GB, 実際: 238.51GB)
   - クラスタリングキー: cs_item_sk, cs_sold_date_sk

5. **Photon Topk**
   - 実行時間: 10,822 ms (0.0%)

## 🔧 Enhanced SHUFFLE操作最適化分析

### 📊 全体サマリー

| 指標 | 値 | 評価 |
|------|-----|------|
| Shuffle操作数 | 3 | ⚠️ 監視必要 |
| 最適化が必要な操作 | 1 | ❌ 改善必要 |
| 総メモリ使用量 | 445.07 GB | ⚠️ 大容量 |
| 平均メモリ/パーティション | 444.2 MB | ✅ 基準値以下 |
| Shuffle効率性スコア | 🟡 66.7% | ⚠️ 改善可能 |

**基準**: メモリ/パーティション ≤ 512MB

### 🔍 個別Shuffle操作分析

#### 1. Shuffle (Node ID: 13708) - 🚨 高優先度
- **パーティション数**: 1
- **ピークメモリ**: 4.19 GB
- **メモリ/パーティション**: 4288.0 MB 🔥 危険レベル
- **実行時間**: 4.6 秒
- **処理行数**: 10,241,024
- **効率性**: ❌ 非効率

**推奨事項**:
- 🚨 非常に高いメモリ使用量 (4288MB/パーティション): パーティション数を8以上に増加するか、クラスターサイズを拡張
- 🖥️ クラスター拡張: より多くのワーカーノードまたは高メモリインスタンスの使用を検討
- 🔧 SQLクエリでREPARTITONヒントもしくはREPARTITON_BY_RANGEヒント(Window関数使用時)を適切に設定

#### 2. Shuffle (Node ID: 13700) - 💡 低優先度
- **パーティション数**: 1,024
- **ピークメモリ**: 440.88 GB
- **メモリ/パーティション**: 440.9 MB ✅ 効率的
- **実行時間**: 2748.4 秒
- **処理行数**: 5,947,260,510

**推奨事項**:
- 🔧 Liquid Clusteringの実装により、Shuffle操作の削減を検討
- ⏱️ 実行時間が長い (2748.4秒): データ分散戦略の見直しを推奨
- 📊 大量データ処理: ブロードキャストJOINや事前集約の活用を検討

#### 3. Shuffle (Node ID: 13626) - 💡 低優先度
- **パーティション数**: 1
- **ピークメモリ**: 0.0 GB
- **メモリ/パーティション**: 4.2 MB ✅ 効率的
- **実行時間**: 0.1 秒
- **処理行数**: 602,690

### 🎯 全体最適化推奨事項

1. **緊急対策** (高優先度ノード向け):
   - クラスターサイズの拡張 (ワーカーノード数増加)
   - 高メモリインスタンスタイプへの変更
   - spark.sql.adaptive.coalescePartitions.maxBatchSize の調整

2. **短期対策** (即座に実行可能):
   - spark.sql.adaptive.coalescePartitions.enabled = true
   - spark.sql.adaptive.skewJoin.enabled = true
   - spark.sql.adaptive.advisoryPartitionSizeInBytes の調整
   - 目標: 512MB/パーティション以下

3. **中期対策** (計画的実装):
   - パーティション数の明示的指定 (.repartition())
   - JOIN戦略の最適化 (ブロードキャストJOINの活用)
   - データ分散戦略の見直し

4. **長期対策** (根本的解決):
   - Liquid Clusteringの実装
   - テーブル設計の最適化
   - ワークロード分離の検討

## 📋 テーブル最適化推奨

### catalog_sales テーブル分析

**テーブルサイズ**: 1220.35GB  
**現在のクラスタリングキー**: cs_item_sk, cs_sold_date_sk  
**推奨クラスタリングカラム**: cs_bill_customer_sk, cs_item_sk, cs_sold_date_sk

**選定根拠**:
- **テーブルサイズ**: 1220.35GBの大規模テーブルにはLiquid Clustering適用が効果的
- **cs_bill_customer_sk**: GROUP BY句で2回使用されており、クエリの主要なグルーピング条件
- **cs_item_sk**, **cs_sold_date_sk**: 現在のクラスタリングキーとして有効なため維持

**実装SQL**:
```sql
ALTER TABLE tpcds.tpcds_sf10000_delta_lc.catalog_sales 
CLUSTER BY (cs_bill_customer_sk, cs_item_sk, cs_sold_date_sk);
OPTIMIZE tpcds.tpcds_sf10000_delta_lc.catalog_sales FULL;
```

**期待される改善効果**:
- GROUP BY操作のパフォーマンスが30-50%向上
- クエリ全体の実行時間が25-40%短縮（3174.9秒 → 約1900-2400秒）
- スピル操作の削減によるI/O負荷の軽減

### 💡 Liquid Clustering キー選定ガイドライン

#### キー選定の原則
- **基本原則**: フィルタ列での読み取り最適化（データスキッピング）を優先
- **優先順位**: 「よく絞り込みに使う列」を第一優先に選定

#### GROUP BY キーの考慮条件

次の条件が揃うと間接的にシャッフル効率が改善することがあります：

1. **フィルタにも使う列がGROUP BYにも登場する場合**  
   - LC により同じキー近傍のレコードが同じファイル/スプリットに偏在しやすくなり、Map 側の部分集約（combiner）が効きやすい
   
2. **シャッフルに乗る中間データ量の削減が見込める場合**  
   - その結果、シャッフルに乗る中間データ量が減る（Shuffle Read/Write 縮小、スピル減）
   - ただしグローバル集約のためのシャッフル自体は必要です
   
3. **キーのカーディナリティが低〜中程度で極端なスキューが少ない場合**  
   - ファイルごとの「局所的なユニークグループ数」が減り、前述の部分集約効果が出やすい

#### 実務上の推奨
上記条件を満たさない場合は、常にフィルタ列を優先してください。

## 🚀 SQL最適化分析

### 最適化試行結果

- 試行回数: 2回
- 最終選択: 元のクエリ（最適化による有意な改善なし）
- 試行結果:
  - 試行1: 改善不足 (コスト変化: -4.7%)
  - 試行2: 改善不足 (コスト変化: -4.7%)

### 推奨クエリ

```sql
USE CATALOG tpcds;
USE SCHEMA tpcds_sf1000_delta_lc;

SELECT
cs_bill_customer_sk,
AVG(cs_ext_sales_price) AS avg_cs_ext_sales_price,
MIN(cs_ext_sales_price) AS min_cs_ext_sales_price,
MAX(cs_ext_sales_price) AS max_cs_ext_sales_price,
COUNT(cs_ext_sales_price) AS count_cs_ext_sales_price,
AVG(cs_net_profit) AS avg_cs_net_profit,
MIN(cs_net_profit) AS min_cs_net_profit,
MAX(cs_net_profit) AS max_cs_net_profit,
COUNT(cs_net_profit) AS count_cs_net_profit,
AVG(cs_wholesale_cost) AS avg_another_numeric_column1,
MIN(cs_wholesale_cost) AS min_another_numeric_column1,
MAX(cs_wholesale_cost) AS max_another_numeric_column1,
COUNT(cs_wholesale_cost) AS count_another_numeric_column1,
AVG(cs_list_price) AS avg_another_numeric_column2,
MIN(cs_list_price) AS min_another_numeric_column2,
MAX(cs_list_price) AS max_another_numeric_column2,
COUNT(cs_list_price) AS count_another_numeric_column2,
AVG(cs_sales_price) AS avg_another_numeric_column3,
MIN(cs_sales_price) AS min_another_numeric_column3,
MAX(cs_sales_price) AS max_another_numeric_column3,
COUNT(cs_sales_price) AS count_another_numeric_column3
FROM
tpcds.tpcds_sf10000_delta_lc.catalog_sales
GROUP BY
cs_bill_customer_sk
ORDER BY
cs_bill_customer_sk;
```

## 📈 期待される改善効果

1. **緊急Shuffle最適化**: Node ID 13708のパーティション調整により25-35%の性能改善
2. **Liquid Clusteringの実装**: Shuffle操作削減により20-30%の実行時間短縮
3. **クラスター拡張**: 高メモリインスタンス使用により15-25%のメモリ効率向上
4. **データ分散戦略**: ブロードキャストJOIN活用により10-20%の処理時間短縮

**総合改善効果**: 実行時間 19.3秒 → 12-15秒（約25-35%改善）

## 🔍 実行プラン分析

実行プランでは以下の処理が行われています:
- ColumnarToRow変換
- PhotonSort (cs_bill_customer_sk ASC)
- PhotonShuffleExchange (rangepartitioning)
- PhotonGroupingAgg (keys=[cs_bill_customer_sk])
- PhotonShuffleExchange (hashpartitioning)
- PhotonProject
- PhotonScan (catalog_sales テーブル)

---

*レポート生成時刻: 2025-08-13 18:55:20*