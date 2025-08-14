# Liquid Clustering Analysis Report

**Generated Date**: 2025-08-14 11:45:48  
**Analysis Method**: LLM-based  
**LLM Provider**: databricks

## 📊 Performance Overview

| Item | Value |
|------|-----|
| Execution Time | 19.3 seconds |
| Data Read | 0.00GB |
| Output Rows | 10,000 rows |
| Read Rows | 14,327,929,995 rows |
| Filter Rate | 0.0520 |

## 🔍 Extracted Metadata

### Filter Conditions (5 items)
1. `(tpcds.tpcds_sf10000_delta_lc.catalog_sales_demo.cs_sold_date_sk IS NOT NULL)` (ノード: Scan tpcds.tpcds_sf10000_delta_lc.catalog_sales_demo)
2. `(tpcds.tpcds_sf10000_delta_lc.date_dim.d_date IS NOT NULL)` (ノード: Scan tpcds.tpcds_sf10000_delta_lc.date_dim)
3. `(tpcds.tpcds_sf10000_delta_lc.date_dim.d_date >= DATE '1990-01-02')` (ノード: Scan tpcds.tpcds_sf10000_delta_lc.date_dim)
4. `(tpcds.tpcds_sf10000_delta_lc.date_dim.d_date <= DATE '2020-01-02')` (ノード: Scan tpcds.tpcds_sf10000_delta_lc.date_dim)
5. `(tpcds.tpcds_sf10000_delta_lc.date_dim.d_date_sk IS NOT NULL)` (ノード: Scan tpcds.tpcds_sf10000_delta_lc.date_dim)

### JOIN条件 (2個)
1. `tpcds.tpcds_sf10000_delta_lc.catalog_sales_demo.cs_sold_date_sk` (LEFT_KEYS)
2. `tpcds.tpcds_sf10000_delta_lc.date_dim.d_date_sk` (RIGHT_KEYS)

### GROUP BY条件 (3個)
1. `tpcds.tpcds_sf10000_delta_lc.catalog_sales_demo.cs_bill_customer_sk` (ノード: Grouping Aggregate)
2. `tpcds.tpcds_sf10000_delta_lc.catalog_sales_demo.cs_bill_customer_sk` (ノード: Grouping Aggregate)
3. `tpcds.tpcds_sf10000_delta_lc.date_dim.d_date_sk` (ノード: Grouping Aggregate)

### 集約関数 (16個)
1. `avg(unscaledvalue(tpcds.tpcds_sf10000_delta_lc.catalog_sales_demo.cs_ext_sales_price))` (ノード: Grouping Aggregate)
2. `min(tpcds.tpcds_sf10000_delta_lc.catalog_sales_demo.cs_ext_sales_price)` (ノード: Grouping Aggregate)
3. `max(tpcds.tpcds_sf10000_delta_lc.catalog_sales_demo.cs_ext_sales_price)` (ノード: Grouping Aggregate)
4. `count(tpcds.tpcds_sf10000_delta_lc.catalog_sales_demo.cs_ext_sales_price)` (ノード: Grouping Aggregate)
5. `avg(unscaledvalue(tpcds.tpcds_sf10000_delta_lc.catalog_sales_demo.cs_net_profit))` (ノード: Grouping Aggregate)
6. `min(tpcds.tpcds_sf10000_delta_lc.catalog_sales_demo.cs_net_profit)` (ノード: Grouping Aggregate)
7. `max(tpcds.tpcds_sf10000_delta_lc.catalog_sales_demo.cs_net_profit)` (ノード: Grouping Aggregate)
8. `count(tpcds.tpcds_sf10000_delta_lc.catalog_sales_demo.cs_net_profit)` (ノード: Grouping Aggregate)
9. `avg(unscaledvalue(tpcds.tpcds_sf10000_delta_lc.catalog_sales_demo.cs_ext_sales_price))` (ノード: Grouping Aggregate)
10. `min(tpcds.tpcds_sf10000_delta_lc.catalog_sales_demo.cs_ext_sales_price)` (ノード: Grouping Aggregate)
... 他 6個

## 🏷️ 識別されたテーブル (2個)

- **tpcds.tpcds_sf10000_delta_lc.catalog_sales_demo** (ノード: Scan tpcds.tpcds_sf10000_delta_lc.catalog_sales_demo)
  - 現在のクラスタリングキー: `cs_sold_date_sk`
- **tpcds.tpcds_sf10000_delta_lc.date_dim** (ノード: Scan tpcds.tpcds_sf10000_delta_lc.date_dim)
  - 現在のクラスタリングキー: `d_date_sk`

## 🔎 スキャンノード分析 (0個)


## 🤖 LLM分析結果

❌ Failed to obtain Databricks token. Please set the environment variable DATABRICKS_TOKEN.

#### キー選定の原則
- **基本原則**: フィルタ列での読み取り最適化（データスキッピング）を優先
- **優先順位**: 「よく絞り込みに使う列」を第一優先に選定

#### GROUP BY キーの考慮条件

1. フィルタにも使う列がGROUP BYにも登場する場合
2. シャッフルに乗る中間データ量の削減が見込める場合
3. キーのカーディナリティが低〜中程度で極端なスキューが少ない場合

#### 実務上の推奨

上記条件を満たさない場合は、常にフィルタ列を優先

## 📋 分析サマリー

- **分析対象テーブル数**: 2
- **フィルター条件数**: 5
- **JOIN条件数**: 2
- **GROUP BY条件数**: 3
- **集約関数数**: 16
- **スキャンノード数**: 0

---
*Report generation time: 2025-08-14 11:45:48*
