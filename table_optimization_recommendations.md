# 📋 テーブル最適化推奨

## catalog_sales テーブル分析

### テーブルサイズ・クラスタリングキー情報

| 項目 | 現在の状態 | 推奨値 |
|------|------------|--------|
| **テーブルサイズ** | 1,220.35 GB | - |
| **読み取りデータ量** | 238.51 GB (実際) | - |
| **現在のクラスタリングキー** | `cs_item_sk`, `cs_sold_date_sk` | `cs_bill_customer_sk`, `cs_item_sk`, `cs_sold_date_sk` |
| **フィルタ効率** | 80.5% | 90%+ (目標) |
| **主要ボトルネック** | Shuffle Exchange (52.9%) | シャッフル操作の最適化 |

**パフォーマンス指標**:
- 実行時間: 3,174.9秒 (⚠️ 改善必要)
- メモリスピル: 131.87GB (❌ 問題あり)
- Photon利用率: 99.6% (✅ 良好)
- キャッシュ効率: 0.0% (⚠️ 改善必要)

### 選定根拠

#### 1. データ量とアクセスパターン分析
- **大規模テーブル**: 1,220.35GBの大容量データに対してLiquid Clustering適用が効果的
- **頻繁なGROUP BY操作**: `cs_bill_customer_sk`で複数回のグルーピング処理が発生
- **現在のキー活用**: `cs_item_sk`, `cs_sold_date_sk`は既存の有効なクラスタリングキーとして維持

#### 2. ボトルネック分析に基づく選定
- **Shuffle Exchange処理**: 実行時間の52.9%を占める最大ボトルネック
- **スピル発生**: 131.87GBのメモリスピルによる性能低下
- **グルーピング処理**: `cs_bill_customer_sk`による集約処理が重複実行

#### 3. クエリパターンとの適合性
```sql
-- 分析対象クエリの主要パターン
SELECT cs_bill_customer_sk,
       AVG(cs_ext_sales_price) AS avg_cs_ext_sales_price,
       -- その他の集約関数...
FROM tpcds.tpcds_sf10000_delta_lc.catalog_sales
GROUP BY cs_bill_customer_sk
ORDER BY cs_bill_customer_sk;
```

- **フィルタ条件**: `cs_bill_customer_sk`が主要なグルーピングキー
- **データ局所性**: 同一顧客の取引データを物理的に近接配置
- **部分集約効果**: Map側での前処理効率向上

### 実装SQL

#### Phase 1: クラスタリングキー変更
```sql
-- Step 1: テーブル構造の確認
DESCRIBE EXTENDED tpcds.tpcds_sf10000_delta_lc.catalog_sales;

-- Step 2: 現在のクラスタリング情報確認
SHOW CREATE TABLE tpcds.tpcds_sf10000_delta_lc.catalog_sales;

-- Step 3: Liquid Clusteringキーの更新
ALTER TABLE tpcds.tpcds_sf10000_delta_lc.catalog_sales 
CLUSTER BY (cs_bill_customer_sk, cs_item_sk, cs_sold_date_sk);
```

#### Phase 2: テーブル最適化実行
```sql
-- Step 4: 完全最適化の実行 (データ再配置)
OPTIMIZE tpcds.tpcds_sf10000_delta_lc.catalog_sales FULL;

-- Step 5: テーブル統計情報の更新
ANALYZE TABLE tpcds.tpcds_sf10000_delta_lc.catalog_sales 
COMPUTE STATISTICS FOR ALL COLUMNS;
```

#### Phase 3: 効果測定と検証
```sql
-- Step 6: クラスタリング状態の確認
DESCRIBE DETAIL tpcds.tpcds_sf10000_delta_lc.catalog_sales;

-- Step 7: パフォーマンステスト実行
-- (元のクエリを再実行して性能改善を測定)
```

### 期待される改善効果

#### 1. 実行時間短縮効果
| 処理フェーズ | 現在の時間 | 予想改善後 | 改善率 |
|-------------|------------|------------|--------|
| **Shuffle Exchange** | 13,370,118 ms | 6,000,000-8,000,000 ms | 40-55% |
| **Grouping Aggregate** | 5,647,672 ms | 3,400,000-4,200,000 ms | 25-40% |
| **Data Source Scan** | 2,727,485 ms | 1,900,000-2,200,000 ms | 15-30% |
| **全体実行時間** | 3,174.9秒 | 1,900-2,400秒 | 25-40% |

#### 2. リソース効率改善
- **メモリスピル削減**: 131.87GB → 50-80GB (50-80%改善)
- **シャッフルデータ量**: 30-50%削減
- **I/O負荷軽減**: データ局所性向上によるディスクアクセス最適化
- **CPU使用率**: 部分集約効果による処理効率向上

#### 3. 運用面での効果
- **クエリレスポンス**: エンドユーザー体験の大幅改善
- **リソースコスト**: コンピュートクラスター使用量の削減
- **同時実行性**: 他クエリへの影響軽減
- **メンテナンス性**: テーブル管理の簡素化

---

## 💡 Liquid Clustering キー選定ガイドライン

### キー選定の原則

#### 1. 基本原則: データスキッピング最優先
```
優先順位:
1. フィルタ列 (WHERE句) > その他すべて
2. 結合キー (JOIN条件)
3. グルーピングキー (GROUP BY)
4. 並べ替えキー (ORDER BY)
```

**理由**: Liquid Clusteringの主要機能はデータスキッピングによる読み取り最適化

#### 2. カーディナリティ考慮事項
| カーディナリティレベル | 特徴 | 推奨度 | 注意事項 |
|----------------------|------|--------|----------|
| **低** (100-10,000) | 効率的なファイル分割 | ⭐⭐⭐ | スキュー注意 |
| **中** (10,000-1,000,000) | バランス良好 | ⭐⭐⭐⭐⭐ | 最適範囲 |
| **高** (1,000,000+) | ファイル断片化リスク | ⭐⭐ | 慎重検討 |

#### 3. データ分散とアクセスパターン
```sql
-- 良い例: 時系列データでの日付ベースクラスタリング
CLUSTER BY (order_date, customer_id, product_category)

-- 悪い例: 極端に偏ったデータでのクラスタリング
CLUSTER BY (is_premium_customer) -- Boolean値は避ける
```

### GROUP BY キーの考慮条件

#### 条件1: フィルタとGROUP BYの重複
```sql
-- 効果的なパターン
SELECT customer_id, SUM(sales_amount)
FROM sales_table 
WHERE customer_id IN (1001, 1002, 1003)  -- フィルタ条件
GROUP BY customer_id;                     -- 同じ列でグルーピング

-- クラスタリング推奨: customer_id を第一キーに
CLUSTER BY (customer_id, order_date, product_id)
```

**効果**:
- 同一顧客データの物理的集約
- Map側部分集約の効率化
- シャッフルデータ量の削減

#### 条件2: 中間データ量削減の定量評価
| シナリオ | 部分集約効果 | シャッフル削減率 | 総合効果 |
|----------|-------------|-----------------|----------|
| **理想的** | 80%+ | 60%+ | 🟢 高効果 |
| **良好** | 50-80% | 30-60% | 🟡 中効果 |
| **限定的** | <50% | <30% | 🔴 低効果 |

#### 条件3: キーバランスの評価基準
```sql
-- スキュー分析クエリ例
SELECT 
  customer_id,
  COUNT(*) as record_count,
  COUNT(*) * 100.0 / SUM(COUNT(*)) OVER() as percentage
FROM catalog_sales 
GROUP BY customer_id 
ORDER BY record_count DESC 
LIMIT 20;

-- 判定基準:
-- ✅ 上位20%のキーが全データの50%未満を占める
-- ⚠️ 上位20%のキーが全データの70%以上を占める
-- ❌ 単一キーが全データの20%以上を占める
```

### 実務上の推奨

#### 1. キー選定のフローチャート
```
Step 1: WHERE句の頻出列を特定
   ↓
Step 2: カーディナリティとスキューを評価
   ↓
Step 3: JOIN条件での使用頻度を確認
   ↓
Step 4: GROUP BY併用効果を検証
   ↓
Step 5: 最大3-4列でキー組み合わせを決定
```

#### 2. 業界別推奨パターン

##### Eコマース・小売業
```sql
-- 注文データ
CLUSTER BY (order_date, customer_id, product_category)

-- 在庫データ  
CLUSTER BY (warehouse_id, product_id, stock_date)
```

##### 金融業
```sql
-- 取引データ
CLUSTER BY (transaction_date, account_id, transaction_type)

-- 顧客データ
CLUSTER BY (customer_segment, region_id, account_creation_date)
```

##### IoT・センサーデータ
```sql
-- 時系列データ
CLUSTER BY (sensor_id, measurement_date, device_type)

-- イベントログ
CLUSTER BY (event_date, user_id, event_type)
```

#### 3. 避けるべきアンチパターン

❌ **アンチパターン1: Boolean列の使用**
```sql
-- 悪い例
CLUSTER BY (is_active, is_premium, has_discount)
```

❌ **アンチパターン2: 極端な高カーディナリティ**
```sql
-- 悪い例 (UUIDなど)
CLUSTER BY (transaction_uuid, timestamp_microsecond)
```

❌ **アンチパターン3: 無関係な列の混在**
```sql
-- 悪い例 (アクセスパターンが全く異なる)
CLUSTER BY (customer_name, product_price, random_id)
```

#### 4. パフォーマンス監視指標
```sql
-- 定期的な効果測定クエリ
-- 1. クラスタリング効率
SELECT 
  table_name,
  clustering_keys,
  num_files,
  size_in_bytes / (1024*1024*1024) as size_gb
FROM table_metadata;

-- 2. クエリパフォーマンス追跡
SELECT 
  query_hash,
  avg(execution_time_ms) as avg_time,
  avg(data_read_gb) as avg_data_read,
  count(*) as execution_count
FROM query_history 
WHERE table_name = 'catalog_sales'
GROUP BY query_hash;
```

---

### 📊 実装チェックリスト

- [ ] **事前分析完了**: テーブルサイズ・アクセスパターン確認
- [ ] **キー候補選定**: フィルタ列を最優先で3-4列選定
- [ ] **スキュー分析実施**: データ分散の偏りを定量評価
- [ ] **テスト環境検証**: 小規模データでの効果測定
- [ ] **本番適用計画**: ダウンタイム・リソース計画策定
- [ ] **効果測定準備**: 適用前後の比較指標設定
- [ ] **運用監視設定**: 継続的パフォーマンス追跡体制確立

---

*ガイドライン作成日: 2025-01-13*  
*最終更新: テーブル最適化推奨統合版*