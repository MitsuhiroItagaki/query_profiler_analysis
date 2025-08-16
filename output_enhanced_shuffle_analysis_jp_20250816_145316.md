
================================================================================
🔧 Enhanced SHUFFLE操作最適化分析レポート
================================================================================
📊 基準: メモリ/パーティション ≤ 512MB
================================================================================

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

🎯 全体最適化推奨事項:

  🔧 1/3 のShuffle操作で最適化が必要 (効率性: 66.7%)
  💎 Liquid Clusteringの実装により根本的なShuffle削減を推奨 (最も効果的な長期解決策)
  ⚙️ 適切なパーティション数への調整でメモリ効率を改善 (目標: ≤512MB/パーティション)
  🖥️ クラスターサイズの拡張でメモリ圧迫を軽減 (高優先度ケースで推奨)

📋 実装手順 (優先度順):

1️⃣ 緊急対策 (高優先度ノード向け):
   - クラスターサイズの拡張 (ワーカーノード数増加)
   - 高メモリインスタンスタイプへの変更
   - spark.sql.adaptive.coalescePartitions.maxBatchSize の調整

2️⃣ 短期対策 (即座に実行可能):
   - spark.sql.adaptive.coalescePartitions.enabled = true
   - spark.sql.adaptive.skewJoin.enabled = true
   - spark.sql.adaptive.advisoryPartitionSizeInBytes の調整
   - 目標: 512MB/パーティション以下

3️⃣ 中期対策 (計画的実装):
   - パーティション数の明示的指定 (.repartition())
   - JOIN戦略の最適化 (ブロードキャストJOINの活用)
   - データ分散戦略の見直し

4️⃣ 長期対策 (根本的解決):
   - Liquid Clusteringの実装
   - テーブル設計の最適化
   - ワークロード分離の検討

⚙️ 推奨Sparkパラメータ:

spark.sql.adaptive.advisoryPartitionSizeInBytes = 268435456
spark.sql.adaptive.coalescePartitions.minPartitionNum = 1
spark.sql.adaptive.coalescePartitions.maxBatchSize = 100
spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes = 268435456
