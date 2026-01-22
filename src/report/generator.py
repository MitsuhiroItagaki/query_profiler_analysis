"""Report generation for optimization results."""

from datetime import datetime
from typing import List, Optional

from ..config import get_config, t
from ..models import (
    ExtractedMetrics,
    OptimizationResult,
    BottleneckIndicator,
)
from ..profiler import format_bottleneck_report
from ..optimization import (
    format_performance_comparison,
    format_optimization_attempts_summary,
)
from ..utils.io import save_text_file, save_sql_file, generate_timestamp_filename


def generate_comprehensive_report(
    metrics: ExtractedMetrics,
    bottleneck_indicators: List[BottleneckIndicator],
    optimization_result: Optional[OptimizationResult] = None,
) -> str:
    """Generate a comprehensive optimization report.

    Args:
        metrics: Extracted metrics from profiler
        bottleneck_indicators: List of identified bottlenecks
        optimization_result: Optional optimization result

    Returns:
        Formatted markdown report
    """
    config = get_config()
    language = config.output_language
    lines = []

    # Header
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    if language == "ja":
        lines.append("# SQL最適化レポート")
        lines.append("")
        lines.append(f"**生成日時**: {timestamp}")
        lines.append(f"**クエリID**: {metrics.query_metrics.query_id or 'N/A'}")
    else:
        lines.append("# SQL Optimization Report")
        lines.append("")
        lines.append(f"**Generated**: {timestamp}")
        lines.append(f"**Query ID**: {metrics.query_metrics.query_id or 'N/A'}")

    lines.append("")
    lines.append("---")
    lines.append("")

    # Executive Summary
    lines.append(_generate_executive_summary(metrics, optimization_result, language))
    lines.append("")

    # Metrics Overview
    lines.append(_generate_metrics_overview(metrics, language))
    lines.append("")

    # Bottleneck Analysis
    lines.append(format_bottleneck_report(bottleneck_indicators, language))
    lines.append("")

    # Optimization Results (if available)
    if optimization_result:
        lines.append(_generate_optimization_section(optimization_result, language))
        lines.append("")

    # Recommendations
    lines.append(_generate_recommendations(bottleneck_indicators, language))

    return "\n".join(lines)


def _generate_executive_summary(
    metrics: ExtractedMetrics,
    optimization_result: Optional[OptimizationResult],
    language: str,
) -> str:
    """Generate executive summary section."""
    lines = []
    qm = metrics.query_metrics

    if language == "ja":
        lines.append("## 概要")
        lines.append("")

        # Overall status
        if optimization_result and optimization_result.optimization_success:
            perf = optimization_result.final_performance
            if perf and perf.comprehensive_cost_ratio < 0.9:
                lines.append("🎯 **大幅なパフォーマンス改善が可能です**")
            else:
                lines.append("✅ **パフォーマンス改善の余地があります**")
        else:
            lines.append("ℹ️ **クエリの分析結果**")

        lines.append("")
        lines.append(f"- 実行時間: {qm.execution_time_ms:,.0f} ms")
        lines.append(f"- データサイズ: {qm.total_size_bytes / (1024**3):.2f} GB")
        lines.append(f"- 処理行数: {qm.row_count:,}")

        if qm.spill_to_disk_bytes > 0:
            lines.append(f"- ⚠️ ディスクスピル: {qm.spill_to_disk_bytes / (1024**3):.2f} GB")

    else:
        lines.append("## Executive Summary")
        lines.append("")

        if optimization_result and optimization_result.optimization_success:
            perf = optimization_result.final_performance
            if perf and perf.comprehensive_cost_ratio < 0.9:
                lines.append("🎯 **Significant performance improvement possible**")
            else:
                lines.append("✅ **Performance improvement opportunities identified**")
        else:
            lines.append("ℹ️ **Query Analysis Results**")

        lines.append("")
        lines.append(f"- Execution time: {qm.execution_time_ms:,.0f} ms")
        lines.append(f"- Data size: {qm.total_size_bytes / (1024**3):.2f} GB")
        lines.append(f"- Rows processed: {qm.row_count:,}")

        if qm.spill_to_disk_bytes > 0:
            lines.append(f"- ⚠️ Disk spill: {qm.spill_to_disk_bytes / (1024**3):.2f} GB")

    return "\n".join(lines)


def _generate_metrics_overview(metrics: ExtractedMetrics, language: str) -> str:
    """Generate metrics overview section."""
    lines = []
    qm = metrics.query_metrics

    if language == "ja":
        lines.append("## パフォーマンスメトリクス")
        lines.append("")
        lines.append("| 指標 | 値 |")
        lines.append("|------|-----|")
        lines.append(f"| 実行時間 | {qm.execution_time_ms:,.0f} ms |")
        lines.append(f"| データサイズ | {qm.total_size_bytes / (1024**3):.2f} GB |")
        lines.append(f"| 処理行数 | {qm.row_count:,} |")
        lines.append(f"| キャッシュヒット率 | {qm.cache_hit_ratio * 100:.1f}% |")
        lines.append(f"| ディスクスピル | {qm.spill_to_disk_bytes / (1024**3):.2f} GB |")
        lines.append(f"| シャッフルデータ | {qm.shuffle_bytes / (1024**3):.2f} GB |")
    else:
        lines.append("## Performance Metrics")
        lines.append("")
        lines.append("| Metric | Value |")
        lines.append("|--------|-------|")
        lines.append(f"| Execution Time | {qm.execution_time_ms:,.0f} ms |")
        lines.append(f"| Data Size | {qm.total_size_bytes / (1024**3):.2f} GB |")
        lines.append(f"| Rows Processed | {qm.row_count:,} |")
        lines.append(f"| Cache Hit Ratio | {qm.cache_hit_ratio * 100:.1f}% |")
        lines.append(f"| Disk Spill | {qm.spill_to_disk_bytes / (1024**3):.2f} GB |")
        lines.append(f"| Shuffle Data | {qm.shuffle_bytes / (1024**3):.2f} GB |")

    return "\n".join(lines)


def _generate_optimization_section(result: OptimizationResult, language: str) -> str:
    """Generate optimization results section."""
    lines = []

    if language == "ja":
        lines.append("## 最適化結果")
        lines.append("")

        if result.optimization_success:
            lines.append(f"✅ 最適化成功（試行 {result.best_attempt_number}）")
        else:
            lines.append("❌ 最適化による改善が見られませんでした")

        lines.append("")
    else:
        lines.append("## Optimization Results")
        lines.append("")

        if result.optimization_success:
            lines.append(f"✅ Optimization successful (attempt {result.best_attempt_number})")
        else:
            lines.append("❌ No improvement from optimization")

        lines.append("")

    # Performance comparison
    if result.final_performance:
        lines.append(format_performance_comparison(result.final_performance, language))
        lines.append("")

    # Attempt history
    if result.attempts:
        lines.append(format_optimization_attempts_summary(result.attempts, language))

    return "\n".join(lines)


def _generate_recommendations(
    indicators: List[BottleneckIndicator],
    language: str,
) -> str:
    """Generate recommendations section."""
    lines = []

    if language == "ja":
        lines.append("## 推奨アクション")
        lines.append("")

        if not indicators:
            lines.append("現時点で重大なボトルネックは検出されていません。")
        else:
            for i, indicator in enumerate(indicators[:5], 1):
                lines.append(f"{i}. **{indicator.name}**: {indicator.recommendation}")
    else:
        lines.append("## Recommended Actions")
        lines.append("")

        if not indicators:
            lines.append("No significant bottlenecks detected at this time.")
        else:
            for i, indicator in enumerate(indicators[:5], 1):
                lines.append(f"{i}. **{indicator.name}**: {indicator.recommendation}")

    return "\n".join(lines)


def save_optimization_files(
    original_query: str,
    optimization_result: OptimizationResult,
    report_content: str,
    query_id: str = "",
) -> dict:
    """Save optimization files to disk.

    Args:
        original_query: Original SQL query
        optimization_result: Optimization result
        report_content: Generated report content
        query_id: Optional query ID for filename

    Returns:
        Dictionary of saved file paths
    """
    config = get_config()
    saved_files = {}

    # Save original query
    original_filename = generate_timestamp_filename("original_query", "sql", query_id)
    saved_files["original_sql"] = save_sql_file(
        original_query,
        original_filename,
        config.catalog,
        config.database,
    )

    # Save optimized query
    if optimization_result.optimization_success:
        optimized_filename = generate_timestamp_filename("optimized_query", "sql", query_id)
        saved_files["optimized_sql"] = save_sql_file(
            optimization_result.best_optimized_query,
            optimized_filename,
            config.catalog,
            config.database,
        )

    # Save report
    lang_suffix = "ja" if config.output_language == "ja" else "en"
    report_filename = generate_timestamp_filename(f"optimization_report_{lang_suffix}", "md", query_id)
    saved_files["report"] = save_text_file(report_content, report_filename)

    return saved_files
