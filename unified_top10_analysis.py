# 統一されたTop 10プロセス分析機能
# 日本語版と英語版の完全統一を実現

from typing import Dict, List, Any, Optional

def generate_unified_recommendations(
    memory_per_partition_mb: float,
    threshold_mb: float,
    optimal_partition_count: int,
    peak_memory_gb: float,
    duration_sec: float,
    rows_processed: int,
    output_language: str = 'ja'
) -> List[str]:
    """
    統一された推奨事項生成関数
    言語に依存しない共通ロジックで推奨事項を生成
    
    Args:
        memory_per_partition_mb: パーティションあたりメモリ使用量(MB)
        threshold_mb: 閾値(MB) 
        optimal_partition_count: 最適パーティション数
        peak_memory_gb: ピークメモリ使用量(GB)
        duration_sec: 実行時間(秒)
        rows_processed: 処理行数
        output_language: 出力言語 ('ja' or 'en')
    
    Returns:
        List[str]: 推奨事項リスト
    """
    recommendations = []
    
    # 共通の推奨事項テンプレート
    templates = {
        'ja': {
            'very_high_memory': "🚨 非常に高いメモリ使用量 ({:.0f}MB/パーティション): パーティション数を{}以上に増加するか、クラスターサイズを拡張してください",
            'cluster_expansion': "🖥️ クラスター拡張: より多くのワーカーノードまたは高メモリインスタンスの使用を検討",
            'high_memory': "⚠️ 高いメモリ使用量 ({:.0f}MB/パーティション): パーティション数を{}以上に増加することを強く推奨",
            'aqe_settings': "⚙️ AQE設定: spark.sql.adaptive.advisoryPartitionSizeInBytes の調整を検討",
            'memory_efficiency': "💡 メモリ効率改善 ({:.0f}MB/パーティション): パーティション数を{}に調整することを推奨",
            'liquid_clustering': "🔧 Liquid Clusteringの実装により、Shuffle操作の削減を検討 (現在のメモリ使用量: {:.1f}GB)",
            'long_execution': "⏱️ 実行時間が長い ({:.1f}秒): データ分散戦略の見直しを推奨",
            'large_data': "📊 大量データ処理 ({:,}行): ブロードキャストJOINや事前集約の活用を検討",
            'repartition_hint': "🔧 SQLクエリで発生している場合はREPARTITONヒントもしくはREPARTITON_BY_RANGEヒント(Window関数使用時)を適切に設定してください"
        },
        'en': {
            'very_high_memory': "🚨 Very high memory usage ({:.0f}MB/partition): Increase partition count to {} or more, or expand cluster size",
            'cluster_expansion': "🖥️ Cluster expansion: Consider using more worker nodes or high-memory instances",
            'high_memory': "⚠️ High memory usage ({:.0f}MB/partition): Strongly recommend increasing partition count to {} or more",
            'aqe_settings': "⚙️ AQE settings: Consider adjusting spark.sql.adaptive.advisoryPartitionSizeInBytes",
            'memory_efficiency': "💡 Memory efficiency improvement ({:.0f}MB/partition): Recommend adjusting partition count to {}",
            'liquid_clustering': "🔧 Consider implementing Liquid Clustering to reduce Shuffle operations (current memory usage: {:.1f}GB)",
            'long_execution': "⏱️ Long execution time ({:.1f} seconds): Recommend reviewing data distribution strategy",
            'large_data': "📊 Large data processing ({:,} rows): Consider using broadcast JOIN or pre-aggregation",
            'repartition_hint': "🔧 If occurring in SQL queries, please appropriately configure REPARTITION hints or REPARTITION_BY_RANGE hints (when using Window functions)"
        }
    }
    
    # 言語テンプレートを選択
    lang_templates = templates.get(output_language, templates['ja'])
    
    # メモリ使用量に基づく推奨事項
    if memory_per_partition_mb > threshold_mb * 4:  # 2GB以上
        recommendations.append(lang_templates['very_high_memory'].format(memory_per_partition_mb, optimal_partition_count))
        recommendations.append(lang_templates['cluster_expansion'])
    elif memory_per_partition_mb > threshold_mb * 2:  # 1GB以上
        recommendations.append(lang_templates['high_memory'].format(memory_per_partition_mb, optimal_partition_count))
        recommendations.append(lang_templates['aqe_settings'])
    elif memory_per_partition_mb > threshold_mb:  # 512MB以上
        recommendations.append(lang_templates['memory_efficiency'].format(memory_per_partition_mb, optimal_partition_count))
    
    # 高メモリ使用量に対する推奨事項
    if peak_memory_gb > 100:  # 100GB以上
        recommendations.append(lang_templates['liquid_clustering'].format(peak_memory_gb))
    
    # 長時間実行に対する推奨事項
    if duration_sec > 300:  # 5分以上
        recommendations.append(lang_templates['long_execution'].format(duration_sec))
    
    # 大量データ処理に対する推奨事項
    if rows_processed > 1000000000:  # 10億行以上
        recommendations.append(lang_templates['large_data'].format(rows_processed))
    
    # SQLクエリ最適化ヒント
    if memory_per_partition_mb > threshold_mb:
        recommendations.append(lang_templates['repartition_hint'])
    
    return recommendations


def generate_unified_top10_analysis_data(
    extracted_metrics: Dict[str, Any], 
    limit_nodes: int = 10,
    output_language: str = 'ja'
) -> Dict[str, Any]:
    """
    統一されたTop 10プロセス分析データ生成
    完全に言語非依存の共通ロジック
    
    Args:
        extracted_metrics: 抽出されたメトリクス
        limit_nodes: 分析するノード数
        output_language: 出力言語
    
    Returns:
        Dict[str, Any]: 統一された分析データ
    """
    # 既存のgenerate_top10_time_consuming_processes_data関数を呼び出し
    # 言語依存部分を統一された関数で処理
    
    # ノードを実行時間でソート
    sorted_nodes = sorted(extracted_metrics['node_metrics'], 
                         key=lambda x: x['key_metrics'].get('durationMs', 0), 
                         reverse=True)
    
    # 指定されたノード数まで処理
    final_sorted_nodes = sorted_nodes[:limit_nodes]
    
    # 統一されたデータ構造を初期化
    analysis_data = {
        'summary': {
            'total_nodes_analyzed': len(final_sorted_nodes),
            'total_duration': 0,
            'total_top_nodes_duration': 0,
            'calculation_method': 'unknown',
            'output_language': output_language
        },
        'nodes': []
    }
    
    if not final_sorted_nodes:
        return analysis_data
    
    # 全体時間の計算（デグレ防止）
    overall_metrics = extracted_metrics.get('overall_metrics', {})
    total_duration = overall_metrics.get('total_time_ms', 0)
    task_total_time_ms = overall_metrics.get('task_total_time_ms', 0)
    
    if task_total_time_ms > 0:
        total_duration = task_total_time_ms
        calculation_method = 'task_total_time_ms'
    elif total_duration <= 0:
        execution_time_ms = overall_metrics.get('execution_time_ms', 0)
        if execution_time_ms > 0:
            total_duration = execution_time_ms
            calculation_method = 'execution_time_ms'
        else:
            max_node_time = max([node['key_metrics'].get('durationMs', 0) for node in sorted_nodes], default=1)
            total_duration = int(max_node_time * 1.2)
            calculation_method = 'estimated'
    else:
        calculation_method = 'total_time_ms'
    
    # 各ノードの分析
    for i, node in enumerate(final_sorted_nodes):
        duration_ms = node['key_metrics'].get('durationMs', 0)
        rows_num = node['key_metrics'].get('rowsNum', 0)
        memory_mb = node['key_metrics'].get('peakMemoryBytes', 0) / 1024 / 1024
        
        # 時間パーセンテージ計算（デグレ防止）
        time_percentage = min((duration_ms / max(total_duration, 1)) * 100, 100.0)
        
        # 重要度とアイコン
        if duration_ms >= 10000:
            severity = "CRITICAL"
            time_icon = "🔴"
        elif duration_ms >= 5000:
            severity = "HIGH"
            time_icon = "🟠"
        elif duration_ms >= 1000:
            severity = "MEDIUM"
            time_icon = "🟡"
        else:
            severity = "LOW"
            time_icon = "🟢"
        
        # メモリアイコン
        memory_icon = "💚" if memory_mb < 100 else "⚠️" if memory_mb < 1000 else "🚨"
        
        # ノードデータを構造化（完全に言語非依存）
        node_data = {
            'rank': i + 1,
            'node_id': node.get('node_id', node.get('id', 'N/A')),
            'node_name': node.get('name', ''),
            'duration_ms': duration_ms,
            'time_percentage': time_percentage,
            'rows_processed': rows_num,
            'memory_mb': memory_mb,
            'severity': severity,
            'icons': {
                'time': time_icon,
                'memory': memory_icon
            },
            'processing_efficiency': {
                'rows_per_sec': (rows_num * 1000) / duration_ms if duration_ms > 0 else 0
            }
        }
        
        analysis_data['nodes'].append(node_data)
    
    # サマリー情報を更新
    analysis_data['summary'].update({
        'total_duration': total_duration,
        'total_top_nodes_duration': sum(node['key_metrics'].get('durationMs', 0) for node in final_sorted_nodes),
        'calculation_method': calculation_method
    })
    
    return analysis_data


def format_unified_top10_report(analysis_data: Dict[str, Any], output_language: str = 'ja') -> str:
    """
    統一されたTop 10レポートフォーマッター
    完全に同じ詳細度、分析精度、最適化提案を提供
    
    Args:
        analysis_data: 統一された分析データ
        output_language: 出力言語
    
    Returns:
        str: フォーマットされたレポート
    """
    report_lines = []
    report_lines.append("")
    
    summary = analysis_data['summary']
    nodes = analysis_data['nodes']
    limit_nodes = len(nodes)
    
    # 言語別テンプレート
    templates = {
        'ja': {
            'cumulative_time': "📊 累積タスク実行時間（並列）: {:,} ms ({:.1f} 時間)",
            'top_total_time': "📈 TOP{}合計時間（並列実行）: {:,} ms",
            'execution_time': "**実行時間**: {:,}ms ({:.1f}% of total)",
            'severity': "**重要度**: {}",
            'detailed_metrics': "**📊 詳細メトリクス:**",
            'exec_time': "- ⏱️  実行時間: {:>8,} ms ({:>6.1f} sec)",
            'rows_processed': "- 📊 処理行数: {:>8,} 行",
            'peak_memory': "- 💾 ピークメモリ: {:>6.1f} MB",
            'processing_efficiency': "- 🚀 処理効率: {:>8,.0f} 行/秒",
            'node_id': "- 🆔 ノードID: {}",
            'no_metrics': "⚠️ ノードメトリクスが見つかりませんでした"
        },
        'en': {
            'cumulative_time': "📊 Cumulative task execution time (parallel): {:,} ms ({:.1f} hours)",
            'top_total_time': "📈 TOP{} total time (parallel execution): {:,} ms",
            'execution_time': "**Execution Time**: {:,}ms ({:.1f}% of total)",
            'severity': "**Severity**: {}",
            'detailed_metrics': "**📊 Detailed Metrics:**",
            'exec_time': "- ⏱️  Execution time: {:>8,} ms ({:>6.1f} sec)",
            'rows_processed': "- 📊 Rows processed: {:>8,} rows",
            'peak_memory': "- 💾 Peak memory: {:>6.1f} MB",
            'processing_efficiency': "- 🚀 Processing efficiency: {:>8,.0f} rows/sec",
            'node_id': "- 🆔 Node ID: {}",
            'no_metrics': "⚠️ No node metrics found"
        }
    }
    
    # 言語テンプレートを選択
    lang_templates = templates.get(output_language, templates['ja'])
    
    if nodes:
        report_lines.append(lang_templates['cumulative_time'].format(
            summary['total_duration'], summary['total_duration']/3600000
        ))
        report_lines.append(lang_templates['top_total_time'].format(
            limit_nodes, summary['total_top_nodes_duration']
        ))
        report_lines.append("")
        
        for node in nodes:
            # 基本情報表示
            icons = node['icons']
            severity = node['severity']
            node_name = node['node_name'][:100] + "..." if len(node['node_name']) > 100 else node['node_name']
            
            report_lines.append(f"### {node['rank']}. {icons['time']}{icons['memory']} [{severity:8}] {node_name}")
            report_lines.append("")
            report_lines.append(lang_templates['execution_time'].format(
                node['duration_ms'], node['time_percentage']
            ))
            report_lines.append(lang_templates['severity'].format(severity))
            report_lines.append("")
            report_lines.append(lang_templates['detailed_metrics'])
            report_lines.append(lang_templates['exec_time'].format(
                node['duration_ms'], node['duration_ms']/1000
            ))
            report_lines.append(lang_templates['rows_processed'].format(node['rows_processed']))
            report_lines.append(lang_templates['peak_memory'].format(node['memory_mb']))
            
            # 処理効率
            efficiency = node['processing_efficiency']['rows_per_sec']
            if efficiency > 0:
                report_lines.append(lang_templates['processing_efficiency'].format(efficiency))
            
            # ノードID
            report_lines.append(lang_templates['node_id'].format(node['node_id']))
            report_lines.append("")
    else:
        report_lines.append(lang_templates['no_metrics'])
    
    return "\n".join(report_lines)


# 完全統一されたメイン関数
def generate_completely_unified_top10_report(
    extracted_metrics: Dict[str, Any], 
    limit_nodes: int = 10, 
    output_language: str = None
) -> str:
    """
    完全に統一されたTop 10プロセス分析レポート生成
    
    OUTPUT_LANGUAGEの設定に関係なく：
    - 完全に同じ詳細度
    - 完全に同じ分析精度  
    - 完全に同じ最適化提案
    を提供
    
    Args:
        extracted_metrics: 抽出されたメトリクス
        limit_nodes: 表示するノード数
        output_language: 出力言語 ('ja' or 'en')
    
    Returns:
        str: 統一されたレポート
    """
    if output_language is None:
        import sys
        if 'globals' in dir(sys.modules.get('__main__', sys.modules[__name__])):
            output_language = globals().get('OUTPUT_LANGUAGE', 'ja')
        else:
            output_language = 'ja'
    
    # 統一されたデータ構造を生成
    analysis_data = generate_unified_top10_analysis_data(extracted_metrics, limit_nodes, output_language)
    
    # 統一されたフォーマットでレポートを生成
    return format_unified_top10_report(analysis_data, output_language)