"""Bottleneck analysis from extracted metrics."""

from typing import Any, Dict, List

from ..models import (
    ExtractedMetrics,
    BottleneckIndicator,
    OptimizationPriority,
    NodeMetrics,
)
from ..config import t


def analyze_bottlenecks(metrics: ExtractedMetrics) -> List[BottleneckIndicator]:
    """Analyze extracted metrics to identify bottlenecks.

    Args:
        metrics: Extracted metrics from profiler data

    Returns:
        List of identified bottleneck indicators
    """
    indicators: List[BottleneckIndicator] = []

    # Check for spill to disk
    spill_indicator = _check_spill_bottleneck(metrics)
    if spill_indicator:
        indicators.append(spill_indicator)

    # Check for cache inefficiency
    cache_indicator = _check_cache_bottleneck(metrics)
    if cache_indicator:
        indicators.append(cache_indicator)

    # Check for shuffle bottlenecks
    shuffle_indicators = _check_shuffle_bottlenecks(metrics)
    indicators.extend(shuffle_indicators)

    # Check for data skew
    skew_indicator = _check_data_skew(metrics)
    if skew_indicator:
        indicators.append(skew_indicator)

    # Check for slow nodes
    slow_node_indicators = _check_slow_nodes(metrics)
    indicators.extend(slow_node_indicators)

    return indicators


def _check_spill_bottleneck(metrics: ExtractedMetrics) -> BottleneckIndicator | None:
    """Check for memory spill issues."""
    spill_bytes = metrics.query_metrics.spill_to_disk_bytes

    if spill_bytes <= 0:
        return None

    spill_gb = spill_bytes / (1024 ** 3)

    if spill_gb > 10:
        severity = OptimizationPriority.HIGH
    elif spill_gb > 1:
        severity = OptimizationPriority.MEDIUM
    else:
        severity = OptimizationPriority.LOW

    return BottleneckIndicator(
        name=t("メモリスピル", "Memory Spill"),
        severity=severity,
        description=t(
            f"ディスクへのスピルが {spill_gb:.2f} GB 検出されました",
            f"Spill to disk detected: {spill_gb:.2f} GB",
        ),
        recommendation=t(
            "spark.sql.adaptive.advisoryPartitionSizeInBytes の調整、"
            "またはクラスタのメモリ増加を検討してください",
            "Consider adjusting spark.sql.adaptive.advisoryPartitionSizeInBytes "
            "or increasing cluster memory",
        ),
    )


def _check_cache_bottleneck(metrics: ExtractedMetrics) -> BottleneckIndicator | None:
    """Check for cache inefficiency."""
    cache_ratio = metrics.query_metrics.cache_hit_ratio

    if cache_ratio >= 0.8:
        return None

    if cache_ratio < 0.3:
        severity = OptimizationPriority.HIGH
    elif cache_ratio < 0.5:
        severity = OptimizationPriority.MEDIUM
    else:
        severity = OptimizationPriority.LOW

    return BottleneckIndicator(
        name=t("キャッシュ効率低下", "Cache Inefficiency"),
        severity=severity,
        description=t(
            f"キャッシュヒット率が {cache_ratio * 100:.1f}% と低い値です",
            f"Cache hit ratio is low: {cache_ratio * 100:.1f}%",
        ),
        recommendation=t(
            "Delta Lake のキャッシュ設定を確認し、頻繁にアクセスされるデータの"
            "キャッシングを最適化してください",
            "Review Delta Lake cache settings and optimize caching for "
            "frequently accessed data",
        ),
    )


def _check_shuffle_bottlenecks(metrics: ExtractedMetrics) -> List[BottleneckIndicator]:
    """Check for shuffle-related bottlenecks."""
    indicators: List[BottleneckIndicator] = []

    for shuffle in metrics.shuffle_metrics:
        total_shuffle = shuffle.shuffle_read_bytes + shuffle.shuffle_write_bytes
        shuffle_gb = total_shuffle / (1024 ** 3)

        if shuffle_gb < 1:
            continue

        if shuffle_gb > 100:
            severity = OptimizationPriority.HIGH
        elif shuffle_gb > 10:
            severity = OptimizationPriority.MEDIUM
        else:
            severity = OptimizationPriority.LOW

        indicator = BottleneckIndicator(
            name=t("シャッフルボトルネック", "Shuffle Bottleneck"),
            severity=severity,
            description=t(
                f"大量のシャッフルデータ: {shuffle_gb:.2f} GB",
                f"Large shuffle detected: {shuffle_gb:.2f} GB",
            ),
            affected_nodes=[shuffle.shuffle_id],
            recommendation=t(
                "JOIN キーの最適化、BROADCAST ヒントの追加、または "
                "パーティション戦略の見直しを検討してください",
                "Consider optimizing JOIN keys, adding BROADCAST hints, "
                "or reviewing partition strategy",
            ),
        )
        indicators.append(indicator)

        # Check for partition skew
        if shuffle.partition_count > 0 and shuffle.memory_per_partition_bytes > 0:
            mem_per_partition_mb = shuffle.memory_per_partition_bytes / (1024 ** 2)
            if mem_per_partition_mb > 512:
                skew_indicator = BottleneckIndicator(
                    name=t("パーティションスキュー", "Partition Skew"),
                    severity=OptimizationPriority.HIGH,
                    description=t(
                        f"パーティションあたり {mem_per_partition_mb:.0f} MB のデータ",
                        f"High data per partition: {mem_per_partition_mb:.0f} MB",
                    ),
                    affected_nodes=[shuffle.shuffle_id],
                    recommendation=t(
                        "REPARTITION によるデータ再分散、または "
                        "salting によるスキュー解消を検討してください",
                        "Consider REPARTITION for data redistribution or "
                        "salting to reduce skew",
                    ),
                )
                indicators.append(skew_indicator)

    return indicators


def _check_data_skew(metrics: ExtractedMetrics) -> BottleneckIndicator | None:
    """Check for data skew based on node execution times."""
    if len(metrics.node_metrics) < 2:
        return None

    execution_times = [n.execution_time_ms for n in metrics.node_metrics if n.execution_time_ms > 0]
    if not execution_times:
        return None

    avg_time = sum(execution_times) / len(execution_times)
    max_time = max(execution_times)

    if avg_time <= 0:
        return None

    skew_ratio = max_time / avg_time

    if skew_ratio < 3:
        return None

    if skew_ratio > 10:
        severity = OptimizationPriority.HIGH
    elif skew_ratio > 5:
        severity = OptimizationPriority.MEDIUM
    else:
        severity = OptimizationPriority.LOW

    slowest_node = max(metrics.node_metrics, key=lambda n: n.execution_time_ms)

    return BottleneckIndicator(
        name=t("データスキュー", "Data Skew"),
        severity=severity,
        description=t(
            f"実行時間の偏りが検出されました (スキュー比: {skew_ratio:.1f}x)",
            f"Execution time skew detected (skew ratio: {skew_ratio:.1f}x)",
        ),
        affected_nodes=[slowest_node.node_id],
        recommendation=t(
            "JOIN キーの分散確認、salting の適用、または "
            "Adaptive Query Execution の活用を検討してください",
            "Review JOIN key distribution, apply salting, or "
            "leverage Adaptive Query Execution",
        ),
    )


def _check_slow_nodes(metrics: ExtractedMetrics) -> List[BottleneckIndicator]:
    """Identify unusually slow nodes."""
    indicators: List[BottleneckIndicator] = []

    total_time = metrics.query_metrics.execution_time_ms
    if total_time <= 0:
        return indicators

    for node in metrics.top_time_consuming_nodes[:3]:
        node_ratio = node.execution_time_ms / total_time if total_time > 0 else 0

        if node_ratio < 0.3:
            continue

        if node_ratio > 0.7:
            severity = OptimizationPriority.HIGH
        elif node_ratio > 0.5:
            severity = OptimizationPriority.MEDIUM
        else:
            severity = OptimizationPriority.LOW

        indicator = BottleneckIndicator(
            name=t("高負荷ノード", "High-Load Node"),
            severity=severity,
            description=t(
                f"ノード '{node.node_name}' が全体の {node_ratio * 100:.1f}% の時間を消費",
                f"Node '{node.node_name}' consumes {node_ratio * 100:.1f}% of total time",
            ),
            affected_nodes=[node.node_id],
            recommendation=_get_node_specific_recommendation(node),
        )
        indicators.append(indicator)

    return indicators


def _get_node_specific_recommendation(node: NodeMetrics) -> str:
    """Get optimization recommendation based on node type."""
    node_type = node.node_type.lower()
    node_name = node.node_name.lower()

    if "scan" in node_type or "scan" in node_name:
        return t(
            "パーティションプルーニング、Z-ORDER、または Liquid Clustering の適用を検討",
            "Consider partition pruning, Z-ORDER, or Liquid Clustering",
        )
    elif "join" in node_type or "join" in node_name:
        return t(
            "BROADCAST ヒント、JOIN 順序の最適化、または JOIN 条件の見直し",
            "Consider BROADCAST hints, JOIN order optimization, or reviewing JOIN conditions",
        )
    elif "aggregate" in node_type or "agg" in node_name:
        return t(
            "部分集計の活用、または集計前のフィルタリング強化",
            "Consider partial aggregation or stronger pre-aggregation filtering",
        )
    elif "sort" in node_type or "sort" in node_name:
        return t(
            "ソート列のインデックス化、または不要なソートの削除",
            "Consider indexing sort columns or removing unnecessary sorts",
        )
    else:
        return t(
            "ノードの処理ロジックを確認し、最適化の余地を検討してください",
            "Review node processing logic for optimization opportunities",
        )


def format_bottleneck_report(indicators: List[BottleneckIndicator], language: str = "en") -> str:
    """Format bottleneck indicators as a markdown report.

    Args:
        indicators: List of bottleneck indicators
        language: Output language ('ja' or 'en')

    Returns:
        Formatted markdown report
    """
    if not indicators:
        if language == "ja":
            return "## ボトルネック分析\n\n重大なボトルネックは検出されませんでした。"
        return "## Bottleneck Analysis\n\nNo significant bottlenecks detected."

    lines = []

    if language == "ja":
        lines.append("## ボトルネック分析")
        lines.append("")
        lines.append(f"**検出された問題: {len(indicators)} 件**")
    else:
        lines.append("## Bottleneck Analysis")
        lines.append("")
        lines.append(f"**Issues detected: {len(indicators)}**")

    # Group by severity
    for priority in [OptimizationPriority.HIGH, OptimizationPriority.MEDIUM, OptimizationPriority.LOW]:
        priority_indicators = [i for i in indicators if i.severity == priority]
        if not priority_indicators:
            continue

        priority_label = {
            OptimizationPriority.HIGH: "🔴 HIGH" if language == "en" else "🔴 高",
            OptimizationPriority.MEDIUM: "🟡 MEDIUM" if language == "en" else "🟡 中",
            OptimizationPriority.LOW: "🟢 LOW" if language == "en" else "🟢 低",
        }[priority]

        lines.append("")
        lines.append(f"### {priority_label}")
        lines.append("")

        for indicator in priority_indicators:
            lines.append(f"#### {indicator.name}")
            lines.append(f"- {indicator.description}")
            if indicator.affected_nodes:
                if language == "ja":
                    lines.append(f"- 影響ノード: {', '.join(indicator.affected_nodes)}")
                else:
                    lines.append(f"- Affected nodes: {', '.join(indicator.affected_nodes)}")
            if language == "ja":
                lines.append(f"- 💡 推奨: {indicator.recommendation}")
            else:
                lines.append(f"- 💡 Recommendation: {indicator.recommendation}")
            lines.append("")

    return "\n".join(lines)
