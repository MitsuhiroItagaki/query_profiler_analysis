#!/usr/bin/env python3
"""
REPARTITION並列度算出の最適化ツール

分析結果に基づいてREPARTITION値を正確に計算する
"""

class RepartitionOptimizer:
    def __init__(self):
        # Memory per partition threshold (512MB)
        self.memory_per_partition_threshold_mb = 512
        self.memory_per_partition_threshold_bytes = 512 * 1024 * 1024
    
    def calculate_repartition_hint(self, shuffle_metrics):
        """
        シャッフル処理のメトリクスに基づいてREPARTITION値を計算
        
        Args:
            shuffle_metrics (dict): シャッフル処理のメトリクス
                - sink_peak_memory_bytes: シンクのピークメモリ使用量（バイト）
                - sink_number_of_partitions: 現在のパーティション数
                - sink_tasks_total: タスク総数
                
        Returns:
            int: 推奨されるREPARTITION値
        """
        sink_peak_memory = shuffle_metrics.get('sink_peak_memory_bytes')
        current_partitions = shuffle_metrics.get('sink_number_of_partitions')
        tasks_total = shuffle_metrics.get('sink_tasks_total')
        
        if not all([sink_peak_memory, current_partitions]):
            raise ValueError("必要なメトリクスが不足しています")
        
        # 現在のパーティションあたりのメモリ使用量を計算
        memory_per_partition = sink_peak_memory / current_partitions
        memory_per_partition_mb = memory_per_partition / (1024 * 1024)
        
        print(f"現在のパーティションあたりメモリ使用量: {memory_per_partition_mb:.1f} MB")
        print(f"メモリしきい値: {self.memory_per_partition_threshold_mb} MB")
        
        # しきい値を超える場合のみREPARTITIONが必要
        if memory_per_partition > self.memory_per_partition_threshold_bytes:
            # メモリベースの正確な計算
            required_partitions = sink_peak_memory / self.memory_per_partition_threshold_bytes
            
            print(f"メモリしきい値を超過: REPARTITION({required_partitions:.0f})が必要")
            return int(round(required_partitions))
        else:
            print("メモリしきい値以下: REPARTITIONは不要")
            return current_partitions
    
    def analyze_current_implementation(self, shuffle_metrics, actual_repartition_value):
        """
        現在の実装と理論値を比較分析
        """
        correct_value = self.calculate_repartition_hint(shuffle_metrics)
        tasks_based_value = shuffle_metrics.get('sink_tasks_total', 0) * 2
        
        print("\n=== 分析結果 ===")
        print(f"理論的に正しいREPARTITION値: {correct_value}")
        print(f"実際のREPARTITION値: {actual_repartition_value}")
        print(f"タスク数×2による計算: {tasks_based_value}")
        
        if actual_repartition_value == correct_value:
            print("✅ 正しいメモリベース計算が使用されています")
        elif actual_repartition_value == tasks_based_value:
            print("❌ タスク数ベース計算が使用されています（非効率）")
            print(f"   メモリ効率を改善するため、{correct_value}を使用すべきです")
        else:
            print("❓ 不明な計算方式が使用されています")
        
        return correct_value

def main():
    # 実際のサンプルデータに基づく分析
    optimizer = RepartitionOptimizer()
    
    # Node 7 Shuffleのメトリクス（sample.jsonから取得）
    shuffle_metrics = {
        'sink_peak_memory_bytes': 435284869120,  # 405.39 GB
        'sink_number_of_partitions': 64,
        'sink_tasks_total': 1333
    }
    
    print("=== REPARTITION並列度算出の最適化 ===")
    print()
    
    # 正しいREPARTITION値を計算
    recommended_value = optimizer.calculate_repartition_hint(shuffle_metrics)
    
    print()
    
    # 実際の値と比較分析
    actual_repartition = 2666  # sample.jsonで言及された値
    optimizer.analyze_current_implementation(shuffle_metrics, actual_repartition)
    
    print()
    print("=== 推奨事項 ===")
    print(f"1. 現在のREPARTITION(2666)を{recommended_value}に変更")
    print(f"2. メモリ使用量を{shuffle_metrics['sink_peak_memory_bytes']/1024/1024/1024:.1f} GB から")
    print(f"   {recommended_value * 512:.0f} MB × {recommended_value} = {recommended_value * 512 / 1024:.1f} GB に最適化")
    print(f"3. パーティション数を64から{recommended_value}に増加してメモリ効率を向上")

if __name__ == "__main__":
    main()