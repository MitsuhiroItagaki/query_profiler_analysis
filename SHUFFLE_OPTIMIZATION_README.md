# 🔧 Enhanced Databricks SQL Profiler Analysis Tool - Shuffle Optimization

## 概要

このツールは、Databricks SQLクエリプロファイルを分析し、**Shuffle操作の妥当性を検証**する拡張機能を提供します。特に、**ピークメモリ/パーティション数が512MB以下**かどうかを判定し、最適化推奨事項を提示します。

## 🆕 新機能: Shuffle妥当性チェック

### 主要な判定基準
- **メモリ効率性**: ピークメモリ ÷ パーティション数 ≤ 512MB
- **最適化優先度**: HIGH/MEDIUM/LOW の3段階評価
- **具体的推奨事項**: パーティション数調整、クラスター拡張、Liquid Clustering

### 検証対象
現在のツールでは、以下のShuffle操作を検証していませんでしたが、**新ツールで追加されました**：

✅ **新機能で追加された検証項目**:
- Sink - Number of partitions の妥当性
- パーティション数に対するピークメモリ使用量の効率性
- データスキューとメモリ圧迫の検出
- 具体的なパーティション数調整提案
- クラスターサイジングアドバイス

## 🚀 使用方法

### コマンドライン実行

```bash
# 日本語レポート生成
python3 enhanced_query_profiler_analysis.py query-profile.json --language ja

# 英語レポート生成  
python3 enhanced_query_profiler_analysis.py query-profile.json --language en

# ファイル出力
python3 enhanced_query_profiler_analysis.py query-profile.json --output report.txt
```

### Python スクリプト内での使用

```python
from enhanced_query_profiler_analysis import analyze_query_profile_with_shuffle_optimization

# 分析実行
result = analyze_query_profile_with_shuffle_optimization(
    json_file_path="query-profile.json",
    output_language="ja"  # または "en"
)

print(result)
```

## 📊 レポート例

### パーティション数64のケース (非効率)

```
🔧 SHUFFLE操作最適化分析レポート
================================================================================
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
     - 🚨 非常に高いメモリ使用量 (6486MB/パーティション): 
       パーティション数を810以上に増加するか、クラスターサイズを拡張してください
     - 🖥️ クラスター拡張: より多くのワーカーノードまたは高メモリインスタンスの使用を検討
     - 🔧 Liquid Clusteringの実装により、Shuffle操作の削減を検討
```

## ⚙️ 設定パラメータ

```python
SHUFFLE_ANALYSIS_CONFIG = {
    # メモリ/パーティション閾値 (512MB)
    "memory_per_partition_threshold_mb": 512,
    
    # パーティション数の推奨範囲
    "min_partition_count": 1,
    "max_partition_count": 2000,
    
    # パフォーマンス閾値
    "high_memory_threshold_gb": 100,  # GB
    "long_execution_threshold_sec": 300,  # 5分
    
    # 最適化推奨事項の有効化
    "enable_liquid_clustering_advice": True,
    "enable_partition_tuning_advice": True, 
    "enable_cluster_sizing_advice": True
}
```

## 🎯 最適化優先度の判定基準

| 優先度 | メモリ/パーティション | アクション |
|--------|---------------------|-----------|
| 🚨 HIGH | > 1GB | 緊急対応: クラスター拡張 + パーティション調整 |
| ⚠️ MEDIUM | 512MB - 1GB | 計画的対応: パーティション調整 |
| 💡 LOW | ≤ 512MB | 効率的: 現状維持または微調整 |

## 🔧 推奨される最適化手順

### 1️⃣ 緊急対策 (HIGH優先度向け)
- クラスターサイズ拡張 (ワーカーノード増加)
- 高メモリインスタンスへの変更
- `spark.sql.adaptive.coalescePartitions.maxBatchSize` 調整

### 2️⃣ 短期対策 (即座実行可能)
```sql
-- AQE設定の有効化
SET spark.sql.adaptive.coalescePartitions.enabled = true;
SET spark.sql.adaptive.skewJoin.enabled = true;
SET spark.sql.adaptive.advisoryPartitionSizeInBytes = 268435456; -- 256MB
```

### 3️⃣ 中期対策 (計画的実装)
```sql
-- パーティション数の明示的指定
SELECT * FROM large_table.repartition(200);

-- ブロードキャストJOINの活用
SELECT /*+ BROADCAST(small_table) */ * 
FROM large_table JOIN small_table ON ...;
```

### 4️⃣ 長期対策 (根本的解決)
```sql
-- Liquid Clusteringの実装
ALTER TABLE catalog_sales_demo 
CLUSTER BY (cs_bill_customer_sk);

-- テーブル設計の最適化
CREATE TABLE optimized_table (...) 
CLUSTER BY (frequently_joined_column);
```

## 📈 期待される改善効果

| 対策 | メモリ削減 | 実行時間短縮 | 実装難易度 |
|------|-----------|-------------|-----------|
| パーティション調整 | 30-50% | 20-40% | 低 |
| クラスター拡張 | 50-70% | 40-60% | 中 |
| Liquid Clustering | 70-90% | 60-80% | 高 |

## 🔍 既存ツールとの比較

### 従来のツール
❌ Shuffle処理の妥当性チェックなし  
❌ パーティション数とメモリ使用量の関係性未検証  
❌ 具体的なパーティション数調整提案なし  

### 拡張ツール (新機能)
✅ **512MB/パーティション基準での効率性検証**  
✅ **優先度付き最適化推奨事項**  
✅ **具体的なパーティション数計算**  
✅ **クラスターサイジングアドバイス**  
✅ **段階的実装手順の提示**  
✅ **Sparkパラメータ推奨値の自動計算**  

## 🚨 特に注意すべきケース

### 危険レベル (🔥)
- メモリ/パーティション > 2GB
- 即座にクラスター拡張またはパーティション増加が必要

### 警告レベル (⚠️)  
- メモリ/パーティション > 1GB
- 計画的なパーティション調整を推奨

### 要注意レベル (📈)
- メモリ/パーティション > 512MB
- 改善の余地ありだが、緊急性は低い

## 💡 よくある質問

**Q: なぜ512MBが基準なのですか？**  
A: Databricksのベストプラクティスでは、パーティションあたり100MB-1GBが推奨されており、512MBは効率性とリソース使用量のバランスが取れた値です。

**Q: パーティション数はどのように計算されますか？**  
A: `(現在のメモリ/パーティション ÷ 512MB) × 現在のパーティション数` で最適なパーティション数を算出します。

**Q: Liquid Clusteringはいつ実装すべきですか？**  
A: 複数のShuffle操作で非効率性が検出され、根本的な解決を求める場合に推奨されます。

## 📝 ライセンス

このツールは元のDatabricks SQL Profiler Analysis Toolを拡張したものです。Shuffle最適化機能は追加実装されています。