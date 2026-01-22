"""Iterative optimization with multiple attempts."""

from typing import Any, Dict, List, Optional

from ..config import get_config
from ..models import (
    ExtractedMetrics,
    OptimizationAttempt,
    OptimizationResult,
    PerformanceComparison,
    TrialType,
)
from ..profiler import analyze_bottlenecks, format_bottleneck_report
from .query_generator import (
    generate_optimized_query,
    generate_refined_query,
    generate_error_corrected_query,
)
from .performance import compare_performance


def execute_iterative_optimization(
    original_query: str,
    metrics: ExtractedMetrics,
    execute_explain_fn: callable = None,
) -> OptimizationResult:
    """Execute iterative optimization with multiple attempts.

    Args:
        original_query: Original SQL query
        metrics: Extracted metrics from profiler
        execute_explain_fn: Optional function to execute EXPLAIN and get metrics

    Returns:
        OptimizationResult with all attempts and final result
    """
    config = get_config()
    max_attempts = config.max_optimization_attempts

    # Analyze bottlenecks
    bottleneck_indicators = analyze_bottlenecks(metrics)
    bottleneck_report = format_bottleneck_report(
        bottleneck_indicators,
        config.output_language,
    )

    attempts: List[OptimizationAttempt] = []
    best_attempt: Optional[OptimizationAttempt] = None
    best_performance: Optional[PerformanceComparison] = None

    for attempt_num in range(1, max_attempts + 1):
        print(f"\n🔄 Optimization attempt {attempt_num}/{max_attempts}")

        # Determine trial type
        if attempt_num == 1:
            trial_type = TrialType.INITIAL
        elif attempts and not attempts[-1].is_successful:
            trial_type = TrialType.ERROR_CORRECTION
        else:
            trial_type = TrialType.SINGLE_OPTIMIZATION

        # Generate optimized query
        try:
            if trial_type == TrialType.INITIAL:
                optimized_query = generate_optimized_query(
                    original_query,
                    metrics,
                    bottleneck_report,
                )
            elif trial_type == TrialType.ERROR_CORRECTION:
                last_attempt = attempts[-1]
                optimized_query = generate_error_corrected_query(
                    original_query,
                    last_attempt.query,
                    last_attempt.error_info or "Unknown error",
                )
            else:
                last_successful = _get_last_successful_attempt(attempts)
                if last_successful:
                    optimized_query = generate_refined_query(
                        original_query,
                        metrics,
                        last_successful,
                        bottleneck_report,
                    )
                else:
                    optimized_query = generate_optimized_query(
                        original_query,
                        metrics,
                        bottleneck_report,
                    )

        except Exception as e:
            print(f"❌ Query generation failed: {e}")
            attempts.append(OptimizationAttempt(
                attempt_number=attempt_num,
                trial_type=trial_type,
                query="",
                error_info=str(e),
                is_successful=False,
            ))
            continue

        # Execute EXPLAIN if function provided
        explain_result = ""
        optimized_metrics: Dict[str, Any] = {}
        error_info: Optional[str] = None

        if execute_explain_fn:
            try:
                explain_result, optimized_metrics = execute_explain_fn(optimized_query)
            except Exception as e:
                error_info = str(e)
                print(f"⚠️ EXPLAIN execution failed: {e}")

        # Compare performance
        performance: Optional[PerformanceComparison] = None
        is_successful = error_info is None

        if is_successful and optimized_metrics:
            original_metrics_dict = _metrics_to_dict(metrics)
            performance = compare_performance(original_metrics_dict, optimized_metrics)
            is_successful = performance.is_improved

        # Create attempt record
        attempt = OptimizationAttempt(
            attempt_number=attempt_num,
            trial_type=trial_type,
            query=optimized_query,
            explain_result=explain_result,
            performance=performance,
            error_info=error_info,
            is_successful=is_successful,
        )
        attempts.append(attempt)

        # Track best attempt
        if is_successful:
            if best_performance is None or (
                performance and performance.comprehensive_cost_ratio < best_performance.comprehensive_cost_ratio
            ):
                best_attempt = attempt
                best_performance = performance

        print(f"   {'✅' if is_successful else '❌'} Attempt {attempt_num}: "
              f"{'Success' if is_successful else 'Failed'}")

        # Early termination if significant improvement found
        if performance and performance.comprehensive_cost_ratio < 0.8:
            print("🎯 Significant improvement found, stopping early")
            break

    # Determine final result
    if best_attempt:
        final_query = best_attempt.query
        final_performance = best_performance
        optimization_success = True
        best_attempt_number = best_attempt.attempt_number
    else:
        final_query = original_query
        final_performance = None
        optimization_success = False
        best_attempt_number = 0
        print("⚠️ No successful optimization found, using original query")

    return OptimizationResult(
        original_query=original_query,
        best_optimized_query=final_query,
        best_attempt_number=best_attempt_number,
        attempts=attempts,
        final_performance=final_performance,
        optimization_success=optimization_success,
    )


def _get_last_successful_attempt(
    attempts: List[OptimizationAttempt],
) -> Optional[OptimizationAttempt]:
    """Get the last successful attempt from the list."""
    for attempt in reversed(attempts):
        if attempt.is_successful:
            return attempt
    return None


def _metrics_to_dict(metrics: ExtractedMetrics) -> Dict[str, Any]:
    """Convert ExtractedMetrics to a dictionary for comparison."""
    qm = metrics.query_metrics
    return {
        "total_size_bytes": qm.total_size_bytes,
        "row_count": qm.row_count,
        "execution_time_ms": qm.execution_time_ms,
        "spill_to_disk_bytes": qm.spill_to_disk_bytes,
        "shuffle_bytes": qm.shuffle_bytes,
        "cache_hit_ratio": qm.cache_hit_ratio,
    }


def format_optimization_attempts_summary(
    attempts: List[OptimizationAttempt],
    language: str = "en",
) -> str:
    """Format optimization attempts as a summary report.

    Args:
        attempts: List of optimization attempts
        language: Output language

    Returns:
        Formatted markdown string
    """
    lines = []

    if language == "ja":
        lines.append("## 最適化試行履歴")
        lines.append("")
        lines.append("| 試行 | タイプ | 結果 | コスト比率 |")
        lines.append("|------|--------|------|-----------|")
    else:
        lines.append("## Optimization Attempt History")
        lines.append("")
        lines.append("| Attempt | Type | Result | Cost Ratio |")
        lines.append("|---------|------|--------|------------|")

    for attempt in attempts:
        result = "✅" if attempt.is_successful else "❌"
        cost_ratio = (
            f"{attempt.performance.comprehensive_cost_ratio:.2f}"
            if attempt.performance
            else "N/A"
        )
        lines.append(
            f"| {attempt.attempt_number} | {attempt.trial_type.value} | "
            f"{result} | {cost_ratio} |"
        )

    return "\n".join(lines)
