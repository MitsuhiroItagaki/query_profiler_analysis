"""Data models for the SQL Profiler Analysis Tool."""

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional
from enum import Enum


class OptimizationPriority(Enum):
    """Optimization recommendation priority levels."""
    HIGH = "HIGH"
    MEDIUM = "MEDIUM"
    LOW = "LOW"


class TrialType(Enum):
    """Types of optimization trials."""
    INITIAL = "initial"
    SINGLE_OPTIMIZATION = "single_optimization"
    PERFORMANCE_IMPROVEMENT = "performance_improvement"
    ERROR_CORRECTION = "error_correction"


@dataclass
class QueryMetrics:
    """Basic query execution metrics."""
    query_id: str = ""
    status: str = ""
    execution_time_ms: float = 0.0
    total_size_bytes: int = 0
    row_count: int = 0
    cache_hit_ratio: float = 0.0
    spill_to_disk_bytes: int = 0
    shuffle_bytes: int = 0


@dataclass
class NodeMetrics:
    """Metrics for individual execution plan nodes."""
    node_id: str = ""
    node_name: str = ""
    node_type: str = ""
    execution_time_ms: float = 0.0
    rows_produced: int = 0
    data_size_bytes: int = 0
    is_bottleneck: bool = False
    parallelism: int = 0
    spill_bytes: int = 0
    attributes: Dict[str, Any] = field(default_factory=dict)


@dataclass
class BottleneckIndicator:
    """Bottleneck analysis indicator."""
    name: str = ""
    severity: OptimizationPriority = OptimizationPriority.LOW
    description: str = ""
    affected_nodes: List[str] = field(default_factory=list)
    recommendation: str = ""


@dataclass
class ShuffleMetrics:
    """Shuffle operation metrics."""
    shuffle_id: str = ""
    shuffle_read_bytes: int = 0
    shuffle_write_bytes: int = 0
    partition_count: int = 0
    memory_per_partition_bytes: int = 0
    is_skewed: bool = False
    skew_ratio: float = 0.0


@dataclass
class FilterRateResult:
    """Filter operation rate calculation result."""
    input_rows: int = 0
    output_rows: int = 0
    filter_rate: float = 0.0
    is_effective: bool = False


@dataclass
class LiquidClusteringInfo:
    """Liquid clustering analysis information."""
    table_name: str = ""
    current_clustering_keys: List[str] = field(default_factory=list)
    recommended_clustering_keys: List[str] = field(default_factory=list)
    estimated_improvement: str = ""
    sql_implementation: str = ""


@dataclass
class PerformanceComparison:
    """Performance comparison between original and optimized queries."""
    original_total_size: int = 0
    optimized_total_size: int = 0
    original_row_count: int = 0
    optimized_row_count: int = 0
    size_improvement_ratio: float = 0.0
    row_improvement_ratio: float = 0.0
    recommendation: str = "use_original"
    improvement_level: str = ""
    comprehensive_cost_ratio: float = 1.0
    is_improved: bool = False
    analysis_details: Dict[str, Any] = field(default_factory=dict)


@dataclass
class OptimizationAttempt:
    """Record of a single optimization attempt."""
    attempt_number: int = 0
    trial_type: TrialType = TrialType.INITIAL
    query: str = ""
    explain_result: str = ""
    performance: Optional[PerformanceComparison] = None
    error_info: Optional[str] = None
    is_successful: bool = False


@dataclass
class OptimizationResult:
    """Complete optimization result."""
    original_query: str = ""
    best_optimized_query: str = ""
    best_attempt_number: int = 0
    attempts: List[OptimizationAttempt] = field(default_factory=list)
    final_performance: Optional[PerformanceComparison] = None
    optimization_success: bool = False
    analysis_report: str = ""


@dataclass
class ExtractedMetrics:
    """All extracted metrics from profiler data."""
    query_metrics: QueryMetrics = field(default_factory=QueryMetrics)
    node_metrics: List[NodeMetrics] = field(default_factory=list)
    bottleneck_indicators: List[BottleneckIndicator] = field(default_factory=list)
    shuffle_metrics: List[ShuffleMetrics] = field(default_factory=list)
    top_time_consuming_nodes: List[NodeMetrics] = field(default_factory=list)
    liquid_clustering_info: List[LiquidClusteringInfo] = field(default_factory=list)
    raw_data: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ExplainResult:
    """EXPLAIN statement execution result."""
    query_type: str = ""  # "original" or "optimized"
    explain_output: str = ""
    explain_cost_output: str = ""
    photon_explanation: str = ""
    file_paths: Dict[str, str] = field(default_factory=dict)
    is_successful: bool = False
    error_message: str = ""
