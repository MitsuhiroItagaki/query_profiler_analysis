-- =====================================================
-- Liquid Clustering 実装SQL例
-- 生成日時: 2025-08-14 10:22:17
-- =====================================================

-- 【重要】
-- 以下のSQL例は分析結果に基づく推奨事項です。
-- 実際の実装前に、テーブル構造やデータ特性を確認してください。


-- =====================================================
-- テーブル: tpcds.tpcds_sf10000_delta_lc.catalog_sales_demo
-- 現在のクラスタリングキー: cs_sold_date_sk
-- =====================================================

-- 既存テーブルにLiquid Clusteringを適用する場合:
-- ALTER TABLE tpcds.tpcds_sf10000_delta_lc.catalog_sales_demo CLUSTER BY (column1, column2, column3, column4);

-- 新規テーブル作成時にLiquid Clusteringを設定する場合:
-- CREATE TABLE tpcds.tpcds_sf10000_delta_lc.catalog_sales_demo_clustered
-- CLUSTER BY (column1, column2, column3, column4)
-- AS SELECT * FROM tpcds.tpcds_sf10000_delta_lc.catalog_sales_demo;

-- Delta Live Tablesでの設定例:
-- @dlt.table(
--   cluster_by=["column1", "column2", "column3", "column4"]
-- )
-- def catalog_sales_demo_clustered():
--   return spark.table("tpcds.tpcds_sf10000_delta_lc.catalog_sales_demo")

-- クラスタリング状況の確認:
-- DESCRIBE DETAIL tpcds.tpcds_sf10000_delta_lc.catalog_sales_demo;

-- クラスタリング統計の確認:
-- ANALYZE TABLE tpcds.tpcds_sf10000_delta_lc.catalog_sales_demo COMPUTE STATISTICS FOR ALL COLUMNS;


-- =====================================================
-- テーブル: tpcds.tpcds_sf10000_delta_lc.date_dim
-- 現在のクラスタリングキー: d_date_sk
-- =====================================================

-- 既存テーブルにLiquid Clusteringを適用する場合:
-- ALTER TABLE tpcds.tpcds_sf10000_delta_lc.date_dim CLUSTER BY (column1, column2, column3, column4);

-- 新規テーブル作成時にLiquid Clusteringを設定する場合:
-- CREATE TABLE tpcds.tpcds_sf10000_delta_lc.date_dim_clustered
-- CLUSTER BY (column1, column2, column3, column4)
-- AS SELECT * FROM tpcds.tpcds_sf10000_delta_lc.date_dim;

-- Delta Live Tablesでの設定例:
-- @dlt.table(
--   cluster_by=["column1", "column2", "column3", "column4"]
-- )
-- def date_dim_clustered():
--   return spark.table("tpcds.tpcds_sf10000_delta_lc.date_dim")

-- クラスタリング状況の確認:
-- DESCRIBE DETAIL tpcds.tpcds_sf10000_delta_lc.date_dim;

-- クラスタリング統計の確認:
-- ANALYZE TABLE tpcds.tpcds_sf10000_delta_lc.date_dim COMPUTE STATISTICS FOR ALL COLUMNS;


-- =====================================================
-- 一般的なLiquid Clustering実装パターン
-- =====================================================

-- パターン1: フィルター頻度の高いカラムを優先
-- 推奨順序: 1) フィルター条件カラム 2) JOIN条件カラム 3) GROUP BYカラム

-- パターン2: カーディナリティを考慮した順序
-- 低カーディナリティ → 高カーディナリティの順で配置

-- パターン3: データアクセスパターンに基づく配置
-- よく一緒に使用されるカラムを近い位置に配置

-- =====================================================
-- 実装後のパフォーマンス検証SQL
-- =====================================================

-- 1. クエリ実行計画の確認
-- EXPLAIN SELECT ... FROM table_name WHERE ...;

-- 2. ファイルスキップ統計の確認
-- SELECT * FROM table_name WHERE filter_column = 'value';
-- -- SQLプロファイラーでファイルスキップ数を確認

-- 3. データ配置の確認
-- SELECT 
--   file_path,
--   count(*) as row_count,
--   min(cluster_column1) as min_val,
--   max(cluster_column1) as max_val
-- FROM table_name
-- GROUP BY file_path
-- ORDER BY file_path;

-- =====================================================
-- 注意事項
-- =====================================================

-- 1. Liquid Clusteringは最大4カラムまで指定可能
-- 2. パーティショニングとは併用不可
-- 3. 既存のZORDER BYは自動的に無効化される
-- 4. クラスタリングの効果は時間とともに向上する（OPTIMIZE実行で最適化）
-- 5. 定期的なOPTIMIZE実行を推奨
-- 6. **重要**: カラムの指定順序はパフォーマンスに影響しません
--    * CLUSTER BY (col1, col2, col3) と CLUSTER BY (col3, col1, col2) は同等
--    * 従来のパーティショニングやZ-ORDERとは異なる重要な特性

-- OPTIMIZE実行例:
-- OPTIMIZE table_name;

-- =====================================================
-- 生成情報
-- =====================================================
-- 生成日時: 2025-08-14 10:22:17
-- 分析対象テーブル数: 2
-- 基づいた分析: LLMによるLiquid Clustering分析
