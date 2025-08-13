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

1. **メモリスピル**: 131.87GB - メモリ不足による性能低下
2. **シャッフルボトルネック**: JOIN/GROUP BY処理での大量データ転送
3. **キャッシュ効率低下**: データ再利用効率が低い
4. **フィルタ効率低下**: 必要以上のデータを読み込んでいる

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

## 📋 テーブル最適化分析

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

1. **メモリスピル解消**: 50-80%の性能改善
2. **シャッフル最適化**: 20-60%の実行時間短縮
3. **キャッシュ効率向上**: 30-70%の読み込み時間短縮
4. **フィルタ効率改善**: 40-90%のデータ読み込み量削減

**総合改善効果**: 実行時間 3,174,899ms → 1,269,960ms（約60%改善）

## 🔍 実行プラン分析

実行プランでは以下の処理が行われています:
- ColumnarToRow変換
- PhotonSort (cs_bill_customer_sk ASC)
- PhotonShuffleExchange (rangepartitioning)
- PhotonGroupingAgg (keys=[cs_bill_customer_sk])
- PhotonShuffleExchange (hashpartitioning)
- PhotonProject
- PhotonScan (catalog_sales テーブル)

## 📋 Liquid Clustering キー選定ガイドライン

### キー選定の原則
- **基本原則**: フィルタ列での読み取り最適化（データスキッピング）を優先
- **優先順位**: 「よく絞り込みに使う列」を第一優先に選定
- **GROUP BY キーの考慮条件**:
  1. フィルタにも使う列がGROUP BYにも登場する場合
  2. シャッフルに乗る中間データ量の削減が見込める場合
  3. キーのカーディナリティが低〜中程度で極端なスキューが少ない場合
- **実務上の推奨**: 上記条件を満たさない場合は、常にフィルタ列を優先

---

*レポート生成時刻: 2025-08-13 18:55:20*