# 🔧 REPARTITIONヒント付与条件の更新

## 📊 **更新概要**

Enhanced Shuffle操作最適化分析で「最適化必要性：YES」と判定された場合にREPARTITIONヒントを付与するように修正しました。

## 🚨 **修正前の条件（従来）**

```python
# 従来: スピル検出時のみREPARTITIONヒント適用
if node_analysis["is_shuffle_node"] and spill_detected and spill_bytes > 0:
    # REPARTITIONヒント生成
```

**制限事項**:
- スピルが検出されない限り、REPARTITIONヒントは一切適用されない
- メモリ効率が悪くてもスピル未発生なら最適化対象外

## ✅ **修正後の条件（新機能）**

### 1. **拡張された適用条件**

```python
# 新条件: 2つの条件のいずれかを満たす場合
if node_analysis["is_shuffle_node"]:
    # 条件1: 従来のスピル検出
    if spill_detected and spill_bytes > 0:
        should_add_repartition_hint = True
        repartition_reason = f"Spill({spill_gb:.2f}GB) improvement"
    
    # 条件2: メモリ効率悪化（新機能）
    elif num_tasks > 0:
        memory_per_partition_mb = (peak_memory_bytes / num_tasks) / (1024 * 1024)
        if memory_per_partition_mb > 512:  # 512MB閾値
            should_add_repartition_hint = True
            repartition_reason = f"Memory efficiency improvement ({memory_per_partition_mb:.0f}MB/partition > 512MB threshold)"
```

### 2. **適用基準**

| 条件 | 従来 | 修正後 |
|------|------|--------|
| **スピル検出時** | ✅ 適用 | ✅ 適用（継続） |
| **メモリ効率悪化時** | ❌ 対象外 | ✅ **新たに適用** |
| **判定基準** | スピル発生のみ | スピル発生 OR メモリ/パーティション > 512MB |

### 3. **パーティション数計算の改善**

```python
# メモリ効率改善の場合: 目標512MB/パーティションに基づいて計算
if "Memory efficiency improvement" in repartition_reason:
    memory_per_partition_mb = (peak_memory_bytes / num_tasks) / (1024 * 1024)
    target_partitions = int((memory_per_partition_mb / 512) * num_tasks)
    suggested_partitions = max(target_partitions, 200, num_tasks * 2)
else:
    # スピル改善の場合: 従来のロジック
    suggested_partitions = max(num_tasks * 2, 200)
```

## 🎯 **実際の適用例**

### **テストケース**
- **ノード**: Photon Shuffle Exchange (ノードID: 6309)
- **ピークメモリ**: 405.27 GB
- **タスク数**: 64
- **メモリ/パーティション**: 6,484 MB (約6.3GB)

### **判定結果**
```
✅ 新しい条件によりREPARTITIONヒントが適用される
推奨パーティション数: 810
推奨SQL: REPARTITION(810, cs_bill_customer_sk)
理由: Memory efficiency improvement (6484MB/partition > 512MB threshold)
```

## 📈 **期待される効果**

### 1. **対象範囲の拡大**
- Enhanced Shuffle分析で最適化必要性=YESのケースをカバー
- スピル未発生でもメモリ効率問題を検出・対処

### 2. **メモリ効率の改善**
- 目標: 512MB/パーティション以下
- 実例: 6,484MB/partition → 約400MB/partition (810パーティション時)

### 3. **Shuffle効率性スコアの向上**
- 現在: 🟡 66.7%
- 改善後: 🟢 90%+ を期待

## 🔄 **更新されたルール**

### **REPARTITIONヒント適用の新しいルール**:
- **✅ スピル検出時**: REPARTITIONヒントを適用（従来通り）
- **✅ メモリ効率悪化時**: メモリ/パーティション > 512MB の場合もREPARTITIONヒントを適用（**新機能**）
- **⚠️ 適用条件**: Enhanced Shuffle分析で最適化が必要と判定された場合も対象
- **📊 判定基準**: スピル検出 OR メモリ効率問題（512MB/パーティション超過）

## 📋 **影響範囲**

### **修正対象ファイル**
1. `/workspace/query_profiler_analysis.py` - メインファイル
2. `/workspace/current_query_profiler_analysis.py` - 現行版ファイル

### **修正内容**
- `extract_detailed_bottleneck_analysis()` 関数内のREPARTITIONヒント生成ロジック
- パーティション数計算アルゴリズムの改善
- ドキュメント内のルール説明更新

## 🎉 **導入効果**

この修正により、Enhanced Shuffle操作最適化分析で「最適化必要性：YES」と判定されるケースに対して、適切なREPARTITIONヒントが提供され、より包括的なパフォーマンス最適化が可能になります。