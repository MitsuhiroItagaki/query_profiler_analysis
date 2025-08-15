-- 📋 オリジナルクエリ（プロファイラーデータから抽出）
-- 抽出日時: 2025-08-15 09:25:00
-- ファイル: .//output_original_query_20250815-092500.sql
-- クエリ文字数: 1,401

-- 🗂️ カタログ・スキーマ設定（自動追加）
USE CATALOG tpcds;
USE SCHEMA tpcds_sf1000_delta_lc;

-- 🔍 オリジナルクエリ
-- 集計クエリ
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
  -- AVG(cs_wholesale_cost) AS avg_another_numeric_column1,
  -- MIN(cs_wholesale_cost) AS min_another_numeric_column1,
  -- MAX(cs_wholesale_cost) AS max_another_numeric_column1,
  -- COUNT(cs_wholesale_cost) AS count_another_numeric_column1,
  -- AVG(cs_list_price) AS avg_another_numeric_column2,
  -- MIN(cs_list_price) AS min_another_numeric_column2,
  -- MAX(cs_list_price) AS max_another_numeric_column2,
  -- COUNT(cs_list_price) AS count_another_numeric_column2,
  -- AVG(cs_sales_price) AS avg_another_numeric_column3,
  -- MIN(cs_sales_price) AS min_another_numeric_column3,
  -- MAX(cs_sales_price) AS max_another_numeric_column3,
  -- COUNT(cs_sales_price) AS count_another_numeric_column3
FROM
  tpcds.tpcds_sf10000_delta_lc.catalog_sales_demo
WHERE
  cs_sold_date_sk in (
    select
      d_date_sk
    from
      tpcds.tpcds_sf10000_delta_lc.date_dim
    where
      d_date >= '1990-01-02'
  )
GROUP BY
  cs_bill_customer_sk
ORDER BY
  cs_bill_customer_sk
