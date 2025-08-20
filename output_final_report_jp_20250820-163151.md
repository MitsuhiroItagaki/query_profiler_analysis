# 📊 SQL最適化レポート

**クエリID**: 01f07bbf-bb55-18a3-aa29-0cc57144b438  
**レポート生成日時**: 2025-08-20 16:29:39

## 🎯 1. パフォーマンス概要

### 主要パフォーマンス指標

| 指標 | 値 | 評価 |
|------|-----|------|
| 実行時間 | 56.7秒 | ✅ 良好 |
| データ読み取り量 | 163.01GB | ⚠️ 大容量 |
| Photon利用率 | 99.2% | ✅ 良好 |
| キャッシュ効率 | 57.1% | ⚠️ 改善の余地あり |
| フィルタ率 | 0.1% | ⚠️ フィルタ条件の確認が必要 |
| シャッフル影響度 | 38.6%の影響 | ⚠️ 軽度のチューニングを推奨 |
| スピル発生 | なし | ✅ 良好 |
| スキュー検出 | 検出なし | ✅ 良好 |

### 主要ボトルネック

1. **シャッフルボトルネック**: JOIN/GROUP BY処理での大量データ転送
2. **フィルタ効率低下**: 必要以上のデータを読み込んでいる

## 🐌 2. 処理時間分析

### 処理時間ボトルネックTOP5

#### 1. 🔴 [重大] Photonデータソーススキャン (tpcds.tpcds_sf10000_delta_lc.catalog_sales_org)
- ⏱️ 実行時間: 1,472,898 ms (全体の45.9%)
- 📊 処理行数: 14,326,481,949 行
- 💾 ピークメモリ: 23051.2 MB
- 🔧 並列度: タスク総数: 1440
- 💿 スピル: なし | ⚖️ スキュー: なし
- 🚀 処理効率: 9,726,731 行/秒
- 📂 フィルタ率: 94.7% (読込: 1326.49GB, 実際: 70.00GB)
- 📊 クラスタリングキー: cs_warehouse_sk
- 🆔 ノードID: 6803

#### 2. 🔴 [重大] Photonシャッフル交換
- ⏱️ 実行時間: 1,236,866 ms (全体の38.5%)
- 📊 処理行数: 2,905,294,510 行
- 💾 ピークメモリ: 221580.0 MB
- 🔧 並列度: シンク - タスク総数: 1440 | ソース - タスク総数: 128
- 💿 スピル: なし | ⚖️ スキュー: なし
- 🚀 処理効率: 2,348,916 行/秒
- 🔄 シャッフル属性: tpcds.tpcds_sf10000_delta_lc.catalog_sales_org.cs_bill_customer_sk
- 🆔 ノードID: 6879

#### 3. 🟡 [中] Photonグループ集計
- ⏱️ 実行時間: 297,683 ms (全体の9.3%)
- 📊 処理行数: 64,710,651 行
- 💾 ピークメモリ: 5632.0 MB
- 🔧 並列度: タスク総数: 128
- 💿 スピル: なし | ⚖️ スキュー: なし
- 🚀 処理効率: 217,381 行/秒
- 🆔 ノードID: 6881

#### 4. 🟢 [低] Photonグループ集計
- ⏱️ 実行時間: 111,383 ms (全体の3.5%)
- 📊 処理行数: 2,905,294,510 行
- 💾 ピークメモリ: 17418.0 MB
- 🔧 並列度: タスク総数: 1440
- 💿 スピル: なし | ⚖️ スキュー: なし
- 🚀 処理効率: 26,083,823 行/秒
- 🆔 ノードID: 6811

#### 5. 🟢 [低] Photon左半結合
- ⏱️ 実行時間: 45,024 ms (全体の1.4%)
- 📊 処理行数: 2,907,967,860 行
- 💾 ピークメモリ: 56.0 MB
- 🔧 並列度: タスク総数: 1440
- 💿 スピル: なし | ⚖️ スキュー: なし
- 🚀 処理効率: 64,587,062 行/秒
- 🆔 ノードID: 6807

## 🔧 3. Shuffle操作最適化分析

### パフォーマンス概要
- **シャッフル影響度**: 38.6% (全体実行時間の約4割)
- **シャッフル処理時間**: 1,236,866ms (全体の38.5%)
- **処理行数**: 2,905,294,510 行
- **ピークメモリ**: 221580.0 MB
- **処理効率**: 2,348,916 行/秒

### 効率性評価
- **並列度**: シンク(1440)とソース(128)のタスク数に大きな差があり、最適化の余地あり
- **スキュー**: 検出なし
- **スピル**: 発生なし

### 推奨事項
1. **パーティション数調整**: 433パーティションへの再分散を推奨
2. **シャッフルキー最適化**: cs_bill_customer_skをシャッフルキーとして使用
3. **JOIN順序の最適化**: 小さいテーブル(date_dim)をブロードキャスト側に配置

## 📋 4. テーブル最適化推奨

### catalog_sales テーブル分析

#### 基本情報
- **テーブルサイズ**: 1326.49GB
- **現在のクラスタリングキー**: cs_warehouse_sk
- **推奨クラスタリングカラム**: cs_sold_date_sk, cs_bill_customer_sk

#### 推奨根拠
- **cs_sold_date_sk**: 
  - フィルター条件で使用 (IS NOT NULL)
  - JOIN条件で使用 (date_dimテーブルとの結合キー)
  - 日付範囲フィルタリングの効果が高い
- **cs_bill_customer_sk**: 
  - GROUP BY句で使用（2回出現）
  - 顧客別の集計処理に重要
- 現在のcs_warehouse_skはクエリで使用されていないため、変更が強く推奨される

#### 実装SQL
```sql
ALTER TABLE tpcds.tpcds_sf10000_delta_lc.catalog_sales_org 
CLUSTER BY (cs_sold_date_sk, cs_bill_customer_sk);
OPTIMIZE tpcds.tpcds_sf10000_delta_lc.catalog_sales_org FULL;
```

#### 期待効果
- フィルタ率が0.0007と非常に低いため、適切なクラスタリングによりファイルスキップ率を95%以上向上
- 実行時間を現在の56.7秒から約10-15秒程度に短縮できる見込み
- 特にcs_sold_date_skによる日付範囲フィルタリングの効率が大幅に向上

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

## 🚀 5. SQL最適化分析結果

### 最適化プロセス詳細
- **試行回数**: 3回実行
- **最終選択**: 試行1番
- **選択理由**: コスト効率が最も良い試行を選択

### 最適化提案

```sql
USE CATALOG tpcds;
USE SCHEMA tpcds_sf1000_delta_lc;
WITH filtered_dates AS (
SELECT d_date_sk
FROM tpcds.tpcds_sf10000_delta_lc.date_dim
WHERE d_date >= '2002-01-02'
),
filtered_sales AS (
SELECT cs_bill_customer_sk, cs_ext_sales_price, cs_net_profit
FROM (
SELECT /*+ REPARTITION(433, cs_bill_customer_sk) */
cs_bill_customer_sk, cs_ext_sales_price, cs_net_profit, cs_sold_date_sk
FROM tpcds.tpcds_sf10000_delta_lc.catalog_sales_org
) cs
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
FROM filtered_sales
GROUP BY cs_bill_customer_sk
ORDER BY cs_bill_customer_sk;
```

### パフォーマンス検証

| 項目 | 元クエリ | 最適化クエリ | 比率 | 評価 |
|------|----------|-------------|------|------|
| 実行コスト | 1.00 (基準) | 0.937 | 0.937倍 | ➖ |
| メモリ使用量 | 1.00 (基準) | 1.00 | 1.00倍 | ➖ |
| Photon対応 | はい | はい | — | ➖ |
| Photon利用度 | 129% | 138% | 1.08倍 | ✅ |

### 期待効果
- **シャッフル最適化**: 20-60%の実行時間短縮
- **フィルタ効率改善**: 40-90%のデータ読み込み量削減
- **総合改善効果**: 実行時間 56,725ms → 39,708ms（約30%改善）

## 🔍 6. EXPLAIN + EXPLAIN COST統合分析結果

### 要約された実行プラン・統計情報
- **分析対象**: optimizedクエリ
- **要約実行**: いいえ（サイズ小）
- **Photon対応**: クエリは完全にPhotonでサポートされています
- **統計情報**: catalog_sales_org, date_dimの完全な統計情報が利用可能

## 🔧 7. Enhanced Shuffle操作最適化分析

### 全体サマリー
- Shuffle操作数: 3
- 最適化が必要な操作: 2
- 総メモリ使用量: 216.91 GB
- 平均メモリ/パーティション: 1708.6 MB
- 最適化必要性: はい

**Shuffle効率性スコア**: 33.3% (不良)

### 個別Shuffle操作分析

#### 1. Shuffle (Node ID: 6879)
- **優先度**: 高
- **パーティション数**: 128
- **ピークメモリ**: 216.39 GB
- **メモリ/パーティション**: 1731.1 MB (高レベル)
- **実行時間**: 1236.9 秒
- **処理行数**: 2,905,294,510
- **効率性**: 非効率

**推奨事項**:
- パーティション数を432以上に増加することを強く推奨
- AQE設定: spark.sql.adaptive.advisoryPartitionSizeInBytes の調整を検討
- Liquid Clusteringの実装により、Shuffle操作の削減を検討 (現在のメモリ使用量: 216.4GB)
- 実行時間が長い (1236.9秒): データ分散戦略の見直しを推奨
- 大量データ処理 (2,905,294,510行): ブロードキャストJOINや事前集約の活用を検討
- SQLクエリで発生している場合はREPARTITONヒントもしくはREPARTITON_BY_RANGEヒント(Window関数使用時)を適切に設定

#### 2. Shuffle (Node ID: 6887)
- **優先度**: 中
- **パーティション数**: 1
- **ピークメモリ**: 0.52 GB
- **メモリ/パーティション**: 536.0 MB (要注意)
- **実行時間**: 0.5 秒
- **処理行数**: 1,280,128
- **効率性**: 非効率

**推奨事項**:
- メモリ効率改善 (536MB/パーティション): パーティション数を1に調整することを推奨
- SQLクエリで発生している場合はREPARTITONヒントもしくはREPARTITON_BY_RANGEヒント(Window関数使用時)を適切に設定

#### 3. Shuffle (Node ID: 6805)
- **優先度**: 低
- **パーティション数**: 1
- **ピークメモリ**: 0.0 GB
- **メモリ/パーティション**: 4.8 MB
- **実行時間**: 0.1 秒
- **処理行数**: 286,352
- **効率性**: 効率的

### Shuffle最適化推奨事項

- 2/3 のShuffle操作で最適化が必要 (効率性: 33.3%)
- Liquid Clusteringの実装により根本的なShuffle削減を推奨 (最も効果的な長期解決策)
- 適切なパーティション数への調整でメモリ効率を改善 (目標: ≤512MB/パーティション)
- クラスターサイズの拡張でメモリ圧迫を軽減 (高優先度ケースで推奨)

#### 緊急対策 (高優先度)
- クラスターサイズの拡張 (ワーカーノード数増加)
- 高メモリインスタンスタイプへの変更
- spark.sql.adaptive.coalescePartitions.maxBatchSize の調整

#### 短期対策
- spark.sql.adaptive.coalescePartitions.enabled = true
- spark.sql.adaptive.skewJoin.enabled = true
- spark.sql.adaptive.advisoryPartitionSizeInBytes の調整
- 目標: 512MB/パーティション以下

#### 中長期対策
- パーティション数の明示的指定 (.repartition())
- JOIN戦略の最適化 (ブロードキャストJOINの活用)
- Liquid Clusteringの実装
- テーブル設計の最適化

#### 推奨Sparkパラメータ

```
spark.sql.adaptive.advisoryPartitionSizeInBytes = 268435456
spark.sql.adaptive.coalescePartitions.minPartitionNum = 1
spark.sql.adaptive.coalescePartitions.maxBatchSize = 100
spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes = 268435456
```

*レポート生成時刻: 2025-08-20 16:29:39*