#!/usr/bin/env python3
"""
Enhanced Databricks SQL Profiler Analysis Tool with Shuffle Optimization

This tool analyzes Databricks SQL profiler JSON files and provides:
1. Standard performance analysis
2. Enhanced Shuffle operation validation
3. Memory efficiency per partition analysis  
4. Optimization recommendations based on memory/partition ratio

Key Enhancement: Validates that peak memory per partition is ≤ 512MB
"""

import json
import re
import os
import sys
from datetime import datetime
from typing import Dict, List, Any, Optional, Tuple, Union
import traceback

# Shuffle Analysis Configuration
SHUFFLE_ANALYSIS_CONFIG = {
    # Memory per partition threshold (512MB in bytes)
    "memory_per_partition_threshold_mb": 512,
    "memory_per_partition_threshold_bytes": 512 * 1024 * 1024,
    
    # Minimum partition count recommendations  
    "min_partition_count": 1,
    "max_partition_count": 2000,
    
    # Performance thresholds
    "high_memory_threshold_gb": 100,  # GB
    "long_execution_threshold_sec": 300,  # 5 minutes
    
    # Optimization recommendations
    "enable_liquid_clustering_advice": True,
    "enable_partition_tuning_advice": True,
    "enable_cluster_sizing_advice": True
}

def analyze_shuffle_operations(node_metrics: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Shuffle操作の妥当性を分析し、最適化推奨事項を生成する
    
    Key validation: ピークメモリ/パーティション数 ≤ 512MB
    
    Args:
        node_metrics: ノードメトリクスのリスト
        
    Returns:
        dict: Shuffle分析結果
            {
                "shuffle_nodes": [
                    {
                        "node_id": str,
                        "node_name": str, 
                        "partition_count": int,
                        "peak_memory_bytes": int,
                        "peak_memory_gb": float,
                        "memory_per_partition_mb": float,
                        "is_memory_efficient": bool,  # True if ≤ 512MB per partition
                        "duration_sec": float,
                        "rows_processed": int,
                        "optimization_priority": str,  # "HIGH", "MEDIUM", "LOW"
                        "recommendations": [str]
                    }
                ],
                "overall_assessment": {
                    "total_shuffle_nodes": int,
                    "inefficient_nodes": int,
                    "total_memory_gb": float,
                    "avg_memory_per_partition_mb": float,
                    "needs_optimization": bool,
                    "optimization_summary": [str]
                }
            }
    """
    
    shuffle_analysis = {
        "shuffle_nodes": [],
        "overall_assessment": {
            "total_shuffle_nodes": 0,
            "inefficient_nodes": 0,
            "total_memory_gb": 0.0,
            "avg_memory_per_partition_mb": 0.0,
            "needs_optimization": False,
            "optimization_summary": []
        }
    }
    
    threshold_mb = SHUFFLE_ANALYSIS_CONFIG["memory_per_partition_threshold_mb"]
    threshold_bytes = SHUFFLE_ANALYSIS_CONFIG["memory_per_partition_threshold_bytes"]
    
    total_memory_bytes = 0
    total_partitions = 0
    inefficient_count = 0
    
    # Shuffle操作を持つノードを特定
    for node in node_metrics:
        node_name = node.get('name', '')
        node_tag = node.get('tag', '')
        node_id = node.get('node_id', node.get('id', ''))
        
        # Shuffle操作ノードかどうかを判定
        if 'SHUFFLE' not in node_tag:
            continue
            
        # パーティション数を取得
        partition_count = 0
        peak_memory_bytes = 0
        duration_ms = 0
        rows_processed = 0
        
        # メトリクスからパーティション数とピークメモリを抽出
        detailed_metrics = node.get('detailed_metrics', {})
        key_metrics = node.get('key_metrics', {})
        raw_metrics = node.get('metrics', [])
        
        # パーティション数の取得 - 複数のソースから検索
        partition_metrics = [
            "Sink - Number of partitions",
            "Number of partitions", 
            "AQEShuffleRead - Number of partitions"
        ]
        
        # detailed_metricsから検索
        for metric_name in partition_metrics:
            if metric_name in detailed_metrics:
                partition_count = detailed_metrics[metric_name].get('value', 0)
                break
        
        # raw_metricsからも検索
        if partition_count == 0 and isinstance(raw_metrics, list):
            for metric in raw_metrics:
                metric_label = metric.get('label', '')
                if metric_label in partition_metrics:
                    partition_count = metric.get('value', 0)
                    break
        
        # ピークメモリの取得
        peak_memory_bytes = key_metrics.get('peakMemoryBytes', 0)
        if peak_memory_bytes == 0:
            peak_memory_bytes = key_metrics.get('peak_memory_bytes', 0)
        
        duration_ms = key_metrics.get('durationMs', 0)
        rows_processed = key_metrics.get('rowsNum', 0)
        if rows_processed == 0:
            rows_processed = key_metrics.get('rows_num', 0)
        
        # パーティション数が0の場合はスキップ
        if partition_count <= 0:
            continue
            
        # メモリ効率性の計算
        memory_per_partition_bytes = peak_memory_bytes / partition_count
        memory_per_partition_mb = memory_per_partition_bytes / (1024 * 1024)
        peak_memory_gb = peak_memory_bytes / (1024 * 1024 * 1024)
        duration_sec = duration_ms / 1000.0
        
        # 512MB基準での効率性判定
        is_memory_efficient = memory_per_partition_mb <= threshold_mb
        
        # 最適化優先度の判定
        optimization_priority = "LOW"
        if memory_per_partition_mb > threshold_mb * 4:  # 2GB以上
            optimization_priority = "HIGH"
        elif memory_per_partition_mb > threshold_mb * 2:  # 1GB以上
            optimization_priority = "HIGH"
        elif memory_per_partition_mb > threshold_mb:  # 512MB-1GB
            optimization_priority = "MEDIUM"
        
        # 推奨事項の生成
        recommendations = []
        
        if not is_memory_efficient:
            inefficient_count += 1
            
            # パーティション数の推奨計算
            optimal_partition_count = int((memory_per_partition_mb / threshold_mb) * partition_count)
            
            if memory_per_partition_mb > threshold_mb * 4:  # 2GB以上
                recommendations.append(
                    f"🚨 非常に高いメモリ使用量 ({memory_per_partition_mb:.0f}MB/パーティション): "
                    f"パーティション数を{optimal_partition_count}以上に増加するか、クラスターサイズを拡張してください"
                )
                recommendations.append("🖥️ クラスター拡張: より多くのワーカーノードまたは高メモリインスタンスの使用を検討")
            elif memory_per_partition_mb > threshold_mb * 2:  # 1GB以上  
                recommendations.append(
                    f"⚠️ 高いメモリ使用量 ({memory_per_partition_mb:.0f}MB/パーティション): "
                    f"パーティション数を{optimal_partition_count}以上に増加することを強く推奨"
                )
                recommendations.append("⚙️ AQE設定: spark.sql.adaptive.advisoryPartitionSizeInBytes の調整を検討")
            else:
                recommendations.append(
                    f"💡 メモリ効率改善 ({memory_per_partition_mb:.0f}MB/パーティション): "
                    f"パーティション数を{optimal_partition_count}に調整することを推奨"
                )
        
        if peak_memory_gb > SHUFFLE_ANALYSIS_CONFIG["high_memory_threshold_gb"]:
            recommendations.append(
                f"🔧 Liquid Clusteringの実装により、Shuffle操作の削減を検討 "
                f"(現在のメモリ使用量: {peak_memory_gb:.1f}GB)"
            )
            
        if duration_sec > SHUFFLE_ANALYSIS_CONFIG["long_execution_threshold_sec"]:
            recommendations.append(
                f"⏱️ 実行時間が長い ({duration_sec:.1f}秒): データ分散戦略の見直しを推奨"
            )
            
        if rows_processed > 1000000000:  # 10億行以上
            recommendations.append(
                f"📊 大量データ処理 ({rows_processed:,}行): "
                "ブロードキャストJOINや事前集約の活用を検討"
            )
        
        # Shuffle分析結果に追加
        shuffle_node_analysis = {
            "node_id": node_id,
            "node_name": node_name,
            "partition_count": partition_count,
            "peak_memory_bytes": peak_memory_bytes,
            "peak_memory_gb": round(peak_memory_gb, 2),
            "memory_per_partition_mb": round(memory_per_partition_mb, 2),
            "is_memory_efficient": is_memory_efficient,
            "duration_sec": round(duration_sec, 2),
            "rows_processed": rows_processed,
            "optimization_priority": optimization_priority,
            "recommendations": recommendations
        }
        
        shuffle_analysis["shuffle_nodes"].append(shuffle_node_analysis)
        
        # 全体統計に追加
        total_memory_bytes += peak_memory_bytes
        total_partitions += partition_count
    
    # 全体評価の計算
    total_shuffle_nodes = len(shuffle_analysis["shuffle_nodes"])
    shuffle_analysis["overall_assessment"]["total_shuffle_nodes"] = total_shuffle_nodes
    shuffle_analysis["overall_assessment"]["inefficient_nodes"] = inefficient_count
    shuffle_analysis["overall_assessment"]["total_memory_gb"] = round(total_memory_bytes / (1024**3), 2)
    
    if total_partitions > 0:
        avg_memory_per_partition_mb = (total_memory_bytes / total_partitions) / (1024**2)
        shuffle_analysis["overall_assessment"]["avg_memory_per_partition_mb"] = round(avg_memory_per_partition_mb, 2)
    
    # 最適化の必要性判定
    needs_optimization = inefficient_count > 0
    shuffle_analysis["overall_assessment"]["needs_optimization"] = needs_optimization
    
    # 全体的な最適化推奨事項
    optimization_summary = []
    
    if needs_optimization:
        efficiency_rate = ((total_shuffle_nodes - inefficient_count) / total_shuffle_nodes) * 100
        optimization_summary.append(
            f"🔧 {inefficient_count}/{total_shuffle_nodes} のShuffle操作で最適化が必要 "
            f"(効率性: {efficiency_rate:.1f}%)"
        )
        
        if SHUFFLE_ANALYSIS_CONFIG["enable_liquid_clustering_advice"]:
            optimization_summary.append(
                "💎 Liquid Clusteringの実装により根本的なShuffle削減を推奨 "
                "(最も効果的な長期解決策)"
            )
            
        if SHUFFLE_ANALYSIS_CONFIG["enable_partition_tuning_advice"]:
            optimization_summary.append(
                "⚙️ 適切なパーティション数への調整でメモリ効率を改善 "
                f"(目標: ≤{threshold_mb}MB/パーティション)"
            )
            
        if SHUFFLE_ANALYSIS_CONFIG["enable_cluster_sizing_advice"]:
            optimization_summary.append(
                "🖥️ クラスターサイズの拡張でメモリ圧迫を軽減 "
                "(高優先度ケースで推奨)"
            )
    else:
        optimization_summary.append(
            f"✅ すべてのShuffle操作でメモリ効率が適切です "
            f"(≤{threshold_mb}MB/パーティション)"
        )
    
    shuffle_analysis["overall_assessment"]["optimization_summary"] = optimization_summary
    
    return shuffle_analysis

def generate_shuffle_optimization_report(shuffle_analysis: Dict[str, Any], output_language: str = 'ja') -> str:
    """
    Shuffle最適化レポートを生成
    
    Args:
        shuffle_analysis: analyze_shuffle_operations()の結果
        output_language: 出力言語 ('ja' or 'en')
        
    Returns:
        str: フォーマットされたレポート
    """
    
    if output_language == 'ja':
        report_lines = [
            "",
            "=" * 80,
            "🔧 SHUFFLE操作最適化分析レポート",
            "=" * 80,
            f"📊 基準: メモリ/パーティション ≤ {SHUFFLE_ANALYSIS_CONFIG['memory_per_partition_threshold_mb']}MB",
            "=" * 80,
            ""
        ]
        
        overall = shuffle_analysis["overall_assessment"]
        
        # 全体サマリー
        report_lines.extend([
            "📊 全体サマリー:",
            f"  ・Shuffle操作数: {overall['total_shuffle_nodes']}",
            f"  ・最適化が必要な操作: {overall['inefficient_nodes']}",
            f"  ・総メモリ使用量: {overall['total_memory_gb']} GB",
            f"  ・平均メモリ/パーティション: {overall['avg_memory_per_partition_mb']:.1f} MB",
            f"  ・最適化必要性: {'はい' if overall['needs_optimization'] else 'いいえ'}",
            ""
        ])
        
        # 効率性スコア
        if overall['total_shuffle_nodes'] > 0:
            efficiency_score = ((overall['total_shuffle_nodes'] - overall['inefficient_nodes']) / overall['total_shuffle_nodes']) * 100
            efficiency_icon = "🟢" if efficiency_score >= 80 else "🟡" if efficiency_score >= 60 else "🔴"
            report_lines.extend([
                f"🎯 Shuffle効率性スコア: {efficiency_icon} {efficiency_score:.1f}%",
                ""
            ])
        
        # 個別Shuffle分析
        if shuffle_analysis["shuffle_nodes"]:
            report_lines.extend([
                "🔍 個別Shuffle操作分析:",
                ""
            ])
            
            # 優先度順にソート
            sorted_nodes = sorted(
                shuffle_analysis["shuffle_nodes"], 
                key=lambda x: (
                    {"HIGH": 0, "MEDIUM": 1, "LOW": 2}[x["optimization_priority"]],
                    -x["memory_per_partition_mb"]
                )
            )
            
            for i, node in enumerate(sorted_nodes, 1):
                priority_icon = {"HIGH": "🚨", "MEDIUM": "⚠️", "LOW": "💡"}.get(node["optimization_priority"], "📊")
                efficiency_status = "✅ 効率的" if node["is_memory_efficient"] else "❌ 非効率"
                
                # メモリ/パーティションの警告レベル
                memory_status = ""
                if node["memory_per_partition_mb"] > SHUFFLE_ANALYSIS_CONFIG["memory_per_partition_threshold_mb"] * 4:
                    memory_status = " 🔥 危険レベル"
                elif node["memory_per_partition_mb"] > SHUFFLE_ANALYSIS_CONFIG["memory_per_partition_threshold_mb"] * 2:
                    memory_status = " ⚠️ 高レベル"
                elif node["memory_per_partition_mb"] > SHUFFLE_ANALYSIS_CONFIG["memory_per_partition_threshold_mb"]:
                    memory_status = " 📈 要注意"
                
                report_lines.extend([
                    f"{i}. {node['node_name']} (Node ID: {node['node_id']})",
                    f"   {priority_icon} 優先度: {node['optimization_priority']}",
                    f"   📊 パーティション数: {node['partition_count']:,}",
                    f"   🧠 ピークメモリ: {node['peak_memory_gb']} GB",
                    f"   ⚡ メモリ/パーティション: {node['memory_per_partition_mb']:.1f} MB{memory_status}",
                    f"   ⏱️ 実行時間: {node['duration_sec']:.1f} 秒",
                    f"   📈 処理行数: {node['rows_processed']:,}",
                    f"   🎯 効率性: {efficiency_status}",
                    ""
                ])
                
                if node["recommendations"]:
                    report_lines.append("   💡 推奨事項:")
                    for rec in node["recommendations"]:
                        report_lines.append(f"     - {rec}")
                    report_lines.append("")
        
        # 全体最適化推奨事項
        if overall["optimization_summary"]:
            report_lines.extend([
                "🎯 全体最適化推奨事項:",
                ""
            ])
            for summary in overall["optimization_summary"]:
                report_lines.append(f"  {summary}")
            report_lines.append("")
        
        # 具体的な実装手順
        if overall["needs_optimization"]:
            report_lines.extend([
                "📋 実装手順 (優先度順):",
                "",
                "1️⃣ 緊急対策 (高優先度ノード向け):",
                "   - クラスターサイズの拡張 (ワーカーノード数増加)",
                "   - 高メモリインスタンスタイプへの変更",
                "   - spark.sql.adaptive.coalescePartitions.maxBatchSize の調整",
                "",
                "2️⃣ 短期対策 (即座に実行可能):",
                "   - spark.sql.adaptive.coalescePartitions.enabled = true",
                "   - spark.sql.adaptive.skewJoin.enabled = true", 
                "   - spark.sql.adaptive.advisoryPartitionSizeInBytes の調整",
                f"   - 目標: {SHUFFLE_ANALYSIS_CONFIG['memory_per_partition_threshold_mb']}MB/パーティション以下",
                "",
                "3️⃣ 中期対策 (計画的実装):",
                "   - パーティション数の明示的指定 (.repartition())",
                "   - JOIN戦略の最適化 (ブロードキャストJOINの活用)",
                "   - データ分散戦略の見直し",
                "",
                "4️⃣ 長期対策 (根本的解決):",
                "   - Liquid Clusteringの実装",
                "   - テーブル設計の最適化",
                "   - ワークロード分離の検討",
                ""
            ])
            
            # Sparkパラメータ推奨値
            if overall["avg_memory_per_partition_mb"] > SHUFFLE_ANALYSIS_CONFIG["memory_per_partition_threshold_mb"]:
                target_partition_size_mb = min(SHUFFLE_ANALYSIS_CONFIG["memory_per_partition_threshold_mb"], 256)
                target_partition_size_bytes = target_partition_size_mb * 1024 * 1024
                
                report_lines.extend([
                    "⚙️ 推奨Sparkパラメータ:",
                    "",
                    f"spark.sql.adaptive.advisoryPartitionSizeInBytes = {target_partition_size_bytes}",
                    "spark.sql.adaptive.coalescePartitions.minPartitionNum = 1",
                    "spark.sql.adaptive.coalescePartitions.maxBatchSize = 100",
                    "spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes = 268435456",
                    ""
                ])
        
    else:  # English
        report_lines = [
            "",
            "=" * 80,
            "🔧 SHUFFLE OPERATION OPTIMIZATION ANALYSIS REPORT",
            "=" * 80,
            f"📊 Threshold: Memory per Partition ≤ {SHUFFLE_ANALYSIS_CONFIG['memory_per_partition_threshold_mb']}MB",
            "=" * 80,
            ""
        ]
        
        overall = shuffle_analysis["overall_assessment"]
        
        # Overall Summary
        report_lines.extend([
            "📊 Overall Summary:",
            f"  • Number of Shuffle Operations: {overall['total_shuffle_nodes']}",
            f"  • Operations Requiring Optimization: {overall['inefficient_nodes']}",
            f"  • Total Memory Usage: {overall['total_memory_gb']} GB",
            f"  • Average Memory per Partition: {overall['avg_memory_per_partition_mb']:.1f} MB",
            f"  • Optimization Required: {'Yes' if overall['needs_optimization'] else 'No'}",
            ""
        ])
        
        # Efficiency Score
        if overall['total_shuffle_nodes'] > 0:
            efficiency_score = ((overall['total_shuffle_nodes'] - overall['inefficient_nodes']) / overall['total_shuffle_nodes']) * 100
            efficiency_icon = "🟢" if efficiency_score >= 80 else "🟡" if efficiency_score >= 60 else "🔴"
            report_lines.extend([
                f"🎯 Shuffle Efficiency Score: {efficiency_icon} {efficiency_score:.1f}%",
                ""
            ])
        
        # Individual Shuffle Analysis
        if shuffle_analysis["shuffle_nodes"]:
            report_lines.extend([
                "🔍 Individual Shuffle Operation Analysis:",
                ""
            ])
            
            # Sort by priority
            sorted_nodes = sorted(
                shuffle_analysis["shuffle_nodes"], 
                key=lambda x: (
                    {"HIGH": 0, "MEDIUM": 1, "LOW": 2}[x["optimization_priority"]],
                    -x["memory_per_partition_mb"]
                )
            )
            
            for i, node in enumerate(sorted_nodes, 1):
                priority_icon = {"HIGH": "🚨", "MEDIUM": "⚠️", "LOW": "💡"}.get(node["optimization_priority"], "📊")
                efficiency_status = "✅ Efficient" if node["is_memory_efficient"] else "❌ Inefficient"
                
                # Memory/partition warning level
                memory_status = ""
                if node["memory_per_partition_mb"] > SHUFFLE_ANALYSIS_CONFIG["memory_per_partition_threshold_mb"] * 4:
                    memory_status = " 🔥 Critical"
                elif node["memory_per_partition_mb"] > SHUFFLE_ANALYSIS_CONFIG["memory_per_partition_threshold_mb"] * 2:
                    memory_status = " ⚠️ High"
                elif node["memory_per_partition_mb"] > SHUFFLE_ANALYSIS_CONFIG["memory_per_partition_threshold_mb"]:
                    memory_status = " 📈 Warning"
                
                report_lines.extend([
                    f"{i}. {node['node_name']} (Node ID: {node['node_id']})",
                    f"   {priority_icon} Priority: {node['optimization_priority']}",
                    f"   📊 Partition Count: {node['partition_count']:,}",
                    f"   🧠 Peak Memory: {node['peak_memory_gb']} GB",
                    f"   ⚡ Memory per Partition: {node['memory_per_partition_mb']:.1f} MB{memory_status}",
                    f"   ⏱️ Execution Time: {node['duration_sec']:.1f} seconds",
                    f"   📈 Rows Processed: {node['rows_processed']:,}",
                    f"   🎯 Efficiency: {efficiency_status}",
                    ""
                ])
                
                if node["recommendations"]:
                    report_lines.append("   💡 Recommendations:")
                    for rec in node["recommendations"]:
                        report_lines.append(f"     - {rec}")
                    report_lines.append("")
    
    return "\n".join(report_lines)

def extract_node_metrics_from_query_profile(query_profile: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    クエリプロファイルからノードメトリクスを抽出
    
    Args:
        query_profile: Databricks SQLクエリプロファイルJSON
        
    Returns:
        List[Dict]: ノードメトリクスのリスト
    """
    node_metrics = []
    
    graphs = query_profile.get('graphs', [])
    if not graphs:
        return node_metrics
    
    nodes = graphs[0].get('nodes', [])
    
    for node in nodes:
        # 基本情報
        node_info = {
            'node_id': node.get('id', ''),
            'name': node.get('name', ''),
            'tag': node.get('tag', ''),
            'metrics': node.get('metrics', []),
            'metadata': node.get('metadata', []),
            'key_metrics': {},
            'detailed_metrics': {}
        }
        
        # keyMetricsの処理
        key_metrics = node.get('keyMetrics', {})
        if key_metrics:
            node_info['key_metrics'] = {
                'durationMs': key_metrics.get('durationMs', 0),
                'rowsNum': key_metrics.get('rowsNum', 0),
                'peakMemoryBytes': key_metrics.get('peakMemoryBytes', 0)
            }
        
        # metricsからdetailed_metricsを構築
        detailed_metrics = {}
        for metric in node.get('metrics', []):
            label = metric.get('label', '')
            value = metric.get('value', 0)
            if label:
                detailed_metrics[label] = {'value': value, 'label': label}
        
        node_info['detailed_metrics'] = detailed_metrics
        node_metrics.append(node_info)
    
    return node_metrics

def analyze_query_profile_with_shuffle_optimization(json_file_path: str, output_language: str = 'ja') -> str:
    """
    クエリプロファイルファイルを読み込んでShuffle最適化分析を実行
    
    Args:
        json_file_path: JSONファイルのパス
        output_language: 出力言語 ('ja' or 'en')
        
    Returns:
        str: 分析レポート
    """
    try:
        # JSONファイル読み込み
        with open(json_file_path, 'r', encoding='utf-8') as f:
            query_profile = json.load(f)
        
        # 基本クエリ情報
        query_info = query_profile.get('query', {})
        query_id = query_info.get('id', 'Unknown')
        status = query_info.get('status', 'Unknown')
        
        # メトリクス情報
        metrics = query_info.get('metrics', {})
        total_time_ms = metrics.get('totalTimeMs', 0)
        total_time_sec = total_time_ms / 1000.0
        
        # ノードメトリクス抽出
        node_metrics = extract_node_metrics_from_query_profile(query_profile)
        
        # Shuffle操作分析
        shuffle_analysis = analyze_shuffle_operations(node_metrics)
        
        # レポート生成
        report_lines = []
        
        if output_language == 'ja':
            report_lines.extend([
                "🔍 クエリプロファイル分析結果",
                "=" * 50,
                f"📋 クエリID: {query_id}",
                f"📊 ステータス: {status}",
                f"⏱️ 総実行時間: {total_time_sec:.2f} 秒",
                f"📁 ファイル: {os.path.basename(json_file_path)}",
                ""
            ])
        else:
            report_lines.extend([
                "🔍 Query Profile Analysis Results",
                "=" * 50,
                f"📋 Query ID: {query_id}",
                f"📊 Status: {status}",
                f"⏱️ Total Execution Time: {total_time_sec:.2f} seconds",
                f"📁 File: {os.path.basename(json_file_path)}",
                ""
            ])
        
        # Shuffle最適化レポートを追加
        shuffle_report = generate_shuffle_optimization_report(shuffle_analysis, output_language)
        report_lines.append(shuffle_report)
        
        return "\n".join(report_lines)
        
    except Exception as e:
        error_msg = f"Error analyzing query profile: {str(e)}\n{traceback.format_exc()}"
        return error_msg

# メイン実行部分
if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Enhanced Databricks SQL Profiler Analysis with Shuffle Optimization')
    parser.add_argument('json_file', help='Path to the query profile JSON file')
    parser.add_argument('--language', choices=['ja', 'en'], default='ja', help='Output language (default: ja)')
    parser.add_argument('--output', help='Output file path (default: print to stdout)')
    
    args = parser.parse_args()
    
    # 分析実行
    result = analyze_query_profile_with_shuffle_optimization(args.json_file, args.language)
    
    # 結果出力
    if args.output:
        with open(args.output, 'w', encoding='utf-8') as f:
            f.write(result)
        print(f"Analysis report saved to: {args.output}")
    else:
        print(result)

print("🚀 Enhanced Query Profiler Analysis Tool loaded successfully")
print("🔧 Features:")
print("  • Shuffle operation memory efficiency validation (≤512MB per partition)")
print("  • Optimization priority assessment")
print("  • Actionable recommendations for performance improvement")
print("  • Cluster sizing and partition tuning advice")
print("📊 Ready to analyze query profiles with enhanced Shuffle optimization!")