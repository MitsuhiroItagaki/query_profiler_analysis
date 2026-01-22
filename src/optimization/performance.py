"""Performance comparison between original and optimized queries."""

from typing import Any, Dict, Optional

from ..models import PerformanceComparison
from ..config import get_config


def compare_performance(
    original_metrics: Dict[str, Any],
    optimized_metrics: Dict[str, Any],
) -> PerformanceComparison:
    """Compare performance between original and optimized query metrics.

    Args:
        original_metrics: Metrics from original query
        optimized_metrics: Metrics from optimized query

    Returns:
        PerformanceComparison with analysis results
    """
    config = get_config()

    # Extract key metrics
    orig_size = _safe_get(original_metrics, "total_size_bytes", 0)
    opt_size = _safe_get(optimized_metrics, "total_size_bytes", 0)
    orig_rows = _safe_get(original_metrics, "row_count", 0)
    opt_rows = _safe_get(optimized_metrics, "row_count", 0)

    # Calculate ratios
    size_ratio = _safe_ratio(opt_size, orig_size)
    row_ratio = _safe_ratio(opt_rows, orig_rows)

    # Determine if improved
    is_improved = size_ratio < 0.95 or row_ratio < 0.95

    # Calculate comprehensive cost ratio
    cost_ratio = _calculate_comprehensive_cost_ratio(
        original_metrics,
        optimized_metrics,
    )

    # Determine recommendation
    if cost_ratio < 0.9:
        recommendation = "use_optimized"
        improvement_level = "SIGNIFICANT"
    elif cost_ratio < 1.0:
        recommendation = "use_optimized"
        improvement_level = "MODERATE"
    elif cost_ratio > 1.1:
        recommendation = "use_original"
        improvement_level = "DEGRADED"
    else:
        recommendation = "use_original"
        improvement_level = "NEUTRAL"

    return PerformanceComparison(
        original_total_size=orig_size,
        optimized_total_size=opt_size,
        original_row_count=orig_rows,
        optimized_row_count=opt_rows,
        size_improvement_ratio=1.0 - size_ratio,
        row_improvement_ratio=1.0 - row_ratio,
        recommendation=recommendation,
        improvement_level=improvement_level,
        comprehensive_cost_ratio=cost_ratio,
        is_improved=is_improved,
        analysis_details={
            "size_ratio": size_ratio,
            "row_ratio": row_ratio,
            "cost_ratio": cost_ratio,
        },
    )


def _safe_get(data: Dict[str, Any], key: str, default: Any = 0) -> Any:
    """Safely get a value from a dict with nested key support."""
    if not data:
        return default

    # Support nested keys like "scan_operations.total_size"
    keys = key.split(".")
    value = data

    for k in keys:
        if isinstance(value, dict):
            value = value.get(k, default)
        else:
            return default

    return value if value is not None else default


def _safe_ratio(optimized: float, original: float) -> float:
    """Calculate ratio safely handling division by zero."""
    if original <= 0:
        return 1.0
    return optimized / original


def _calculate_comprehensive_cost_ratio(
    original_metrics: Dict[str, Any],
    optimized_metrics: Dict[str, Any],
) -> float:
    """Calculate a comprehensive cost ratio considering multiple factors.

    Lower ratio means better optimization.
    """
    ratios = []
    weights = []

    # Size ratio (weight: 0.3)
    orig_size = _safe_get(original_metrics, "total_size_bytes", 0)
    opt_size = _safe_get(optimized_metrics, "total_size_bytes", 0)
    if orig_size > 0:
        ratios.append(_safe_ratio(opt_size, orig_size))
        weights.append(0.3)

    # Row count ratio (weight: 0.2)
    orig_rows = _safe_get(original_metrics, "row_count", 0)
    opt_rows = _safe_get(optimized_metrics, "row_count", 0)
    if orig_rows > 0:
        ratios.append(_safe_ratio(opt_rows, orig_rows))
        weights.append(0.2)

    # Scan operations ratio (weight: 0.2)
    orig_scans = _safe_get(original_metrics, "scan_operations", 0)
    opt_scans = _safe_get(optimized_metrics, "scan_operations", 0)
    if isinstance(orig_scans, dict):
        orig_scans = orig_scans.get("total_size", 0)
    if isinstance(opt_scans, dict):
        opt_scans = opt_scans.get("total_size", 0)
    if orig_scans > 0:
        ratios.append(_safe_ratio(opt_scans, orig_scans))
        weights.append(0.2)

    # Join operations ratio (weight: 0.3)
    orig_joins = _safe_get(original_metrics, "join_operations", 0)
    opt_joins = _safe_get(optimized_metrics, "join_operations", 0)
    if isinstance(orig_joins, dict):
        orig_joins = orig_joins.get("total_cost", 0)
    if isinstance(opt_joins, dict):
        opt_joins = opt_joins.get("total_cost", 0)
    if orig_joins > 0:
        ratios.append(_safe_ratio(opt_joins, orig_joins))
        weights.append(0.3)

    # Calculate weighted average
    if not ratios:
        return 1.0

    total_weight = sum(weights)
    if total_weight <= 0:
        return 1.0

    weighted_sum = sum(r * w for r, w in zip(ratios, weights))
    return weighted_sum / total_weight


def format_performance_comparison(
    comparison: PerformanceComparison,
    language: str = "en",
) -> str:
    """Format performance comparison as a markdown report.

    Args:
        comparison: Performance comparison results
        language: Output language

    Returns:
        Formatted markdown string
    """
    lines = []

    if language == "ja":
        lines.append("## パフォーマンス比較")
        lines.append("")
        lines.append("| 指標 | 元のクエリ | 最適化後 | 改善率 |")
        lines.append("|------|-----------|---------|--------|")
        lines.append(
            f"| データサイズ | {_format_bytes(comparison.original_total_size)} | "
            f"{_format_bytes(comparison.optimized_total_size)} | "
            f"{comparison.size_improvement_ratio * 100:.1f}% |"
        )
        lines.append(
            f"| 行数 | {comparison.original_row_count:,} | "
            f"{comparison.optimized_row_count:,} | "
            f"{comparison.row_improvement_ratio * 100:.1f}% |"
        )
        lines.append("")
        lines.append(f"**総合コスト比率**: {comparison.comprehensive_cost_ratio:.2f}")
        lines.append(f"**改善レベル**: {comparison.improvement_level}")
        lines.append(f"**推奨**: {_translate_recommendation(comparison.recommendation, language)}")
    else:
        lines.append("## Performance Comparison")
        lines.append("")
        lines.append("| Metric | Original | Optimized | Improvement |")
        lines.append("|--------|----------|-----------|-------------|")
        lines.append(
            f"| Data Size | {_format_bytes(comparison.original_total_size)} | "
            f"{_format_bytes(comparison.optimized_total_size)} | "
            f"{comparison.size_improvement_ratio * 100:.1f}% |"
        )
        lines.append(
            f"| Row Count | {comparison.original_row_count:,} | "
            f"{comparison.optimized_row_count:,} | "
            f"{comparison.row_improvement_ratio * 100:.1f}% |"
        )
        lines.append("")
        lines.append(f"**Comprehensive Cost Ratio**: {comparison.comprehensive_cost_ratio:.2f}")
        lines.append(f"**Improvement Level**: {comparison.improvement_level}")
        lines.append(f"**Recommendation**: {_translate_recommendation(comparison.recommendation, language)}")

    return "\n".join(lines)


def _format_bytes(bytes_val: int) -> str:
    """Format bytes as human-readable string."""
    if bytes_val < 1024:
        return f"{bytes_val} B"
    elif bytes_val < 1024 ** 2:
        return f"{bytes_val / 1024:.1f} KB"
    elif bytes_val < 1024 ** 3:
        return f"{bytes_val / (1024 ** 2):.1f} MB"
    else:
        return f"{bytes_val / (1024 ** 3):.2f} GB"


def _translate_recommendation(recommendation: str, language: str) -> str:
    """Translate recommendation to target language."""
    translations = {
        "use_optimized": {
            "ja": "最適化クエリを使用",
            "en": "Use optimized query",
        },
        "use_original": {
            "ja": "元のクエリを使用",
            "en": "Use original query",
        },
    }
    return translations.get(recommendation, {}).get(language, recommendation)
