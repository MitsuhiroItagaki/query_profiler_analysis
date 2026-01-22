"""Performance metrics extraction from profiler data."""

from typing import Any, Dict, List, Optional

from .loader import detect_data_format
from ..models import (
    QueryMetrics,
    NodeMetrics,
    ShuffleMetrics,
    ExtractedMetrics,
    FilterRateResult,
)


def extract_metrics(profiler_data: Dict[str, Any]) -> ExtractedMetrics:
    """Extract all metrics from profiler data.

    Args:
        profiler_data: Loaded profiler JSON data

    Returns:
        ExtractedMetrics containing all extracted data
    """
    data_format = detect_data_format(profiler_data)
    print(f"🔍 Detected data format: {data_format}")

    if data_format == "sql_query_summary":
        return _extract_from_query_summary(profiler_data)
    elif data_format == "sql_profiler":
        return _extract_from_sql_profiler(profiler_data)
    else:
        print(f"⚠️ Unknown data format")
        return ExtractedMetrics(raw_data=profiler_data)


def _extract_from_query_summary(profiler_data: Dict[str, Any]) -> ExtractedMetrics:
    """Extract metrics from SQL query summary format."""
    query_data = profiler_data.get("query", {})
    metrics_data = query_data.get("metrics", {})

    if not metrics_data:
        print("⚠️ No metrics data found")
        return ExtractedMetrics(raw_data=profiler_data)

    # Extract query metrics
    query_metrics = QueryMetrics(
        query_id=query_data.get("id", ""),
        status=query_data.get("status", ""),
        execution_time_ms=metrics_data.get("executionTimeMs", 0),
        total_size_bytes=metrics_data.get("readBytes", 0),
        row_count=metrics_data.get("rowsReadCount", 0),
        spill_to_disk_bytes=metrics_data.get("spillToDiskBytes", 0),
        shuffle_bytes=metrics_data.get("networkSentBytes", 0),
    )

    # Calculate cache hit ratio
    read_bytes = metrics_data.get("readBytes", 0)
    cache_bytes = metrics_data.get("readCacheBytes", 0)
    if read_bytes > 0:
        query_metrics.cache_hit_ratio = cache_bytes / read_bytes

    print(f"✅ Extracted metrics from SQL query summary")
    print(f"   - Execution time: {query_metrics.execution_time_ms:,} ms")
    print(f"   - Data read: {query_metrics.total_size_bytes / 1024 / 1024 / 1024:.2f} GB")
    print(f"   - Rows processed: {query_metrics.row_count:,}")

    return ExtractedMetrics(
        query_metrics=query_metrics,
        raw_data=profiler_data,
    )


def _extract_from_sql_profiler(profiler_data: Dict[str, Any]) -> ExtractedMetrics:
    """Extract metrics from SQL profiler detailed format."""
    graphs = profiler_data.get("graphs", [])
    if not graphs:
        print("⚠️ No graphs found in profiler data")
        return ExtractedMetrics(raw_data=profiler_data)

    # Initialize metrics
    query_metrics = QueryMetrics()
    node_metrics_list: List[NodeMetrics] = []
    shuffle_metrics_list: List[ShuffleMetrics] = []

    # Process each graph
    for graph_index, graph in enumerate(graphs):
        # Extract overall metrics from the first graph
        if graph_index == 0:
            query_metrics = _extract_query_metrics_from_graph(graph)

        # Extract node metrics
        nodes = graph.get("nodes", [])
        for node in nodes:
            node_metric = _extract_node_metrics(node, graph_index)
            if node_metric:
                node_metrics_list.append(node_metric)

                # Check for shuffle operations
                if _is_shuffle_node(node):
                    shuffle_metric = _extract_shuffle_metrics(node)
                    if shuffle_metric:
                        shuffle_metrics_list.append(shuffle_metric)

    # Sort nodes by execution time to find top consumers
    sorted_nodes = sorted(
        node_metrics_list,
        key=lambda n: n.execution_time_ms,
        reverse=True,
    )
    top_nodes = sorted_nodes[:10]

    print(f"✅ Extracted metrics from SQL profiler")
    print(f"   - Total nodes: {len(node_metrics_list)}")
    print(f"   - Shuffle operations: {len(shuffle_metrics_list)}")

    return ExtractedMetrics(
        query_metrics=query_metrics,
        node_metrics=node_metrics_list,
        shuffle_metrics=shuffle_metrics_list,
        top_time_consuming_nodes=top_nodes,
        raw_data=profiler_data,
    )


def _extract_query_metrics_from_graph(graph: Dict[str, Any]) -> QueryMetrics:
    """Extract query-level metrics from a graph."""
    metrics = QueryMetrics(
        query_id=graph.get("queryId", ""),
        status=graph.get("status", ""),
    )

    # Try to extract from various possible locations
    stats = graph.get("stats", {})
    if stats:
        metrics.execution_time_ms = stats.get("durationMs", 0)
        metrics.total_size_bytes = stats.get("readBytes", 0)
        metrics.row_count = stats.get("rowsNum", 0)
        metrics.spill_to_disk_bytes = stats.get("spillToDiskBytes", 0)

    return metrics


def _extract_node_metrics(node: Dict[str, Any], graph_index: int) -> Optional[NodeMetrics]:
    """Extract metrics from a single node."""
    node_id = node.get("id", "")
    if not node_id:
        return None

    # Get node name from various sources
    name = _get_node_name(node)
    node_type = node.get("tag", node.get("nodeType", ""))

    # Extract key metrics
    key_metrics = node.get("keyMetrics", {})
    detailed_metrics = node.get("detailedMetrics", {})

    execution_time = key_metrics.get("durationMs", 0)
    if not execution_time and "duration" in detailed_metrics:
        execution_time = detailed_metrics["duration"].get("value", 0)

    rows_produced = key_metrics.get("rowsNum", 0)
    if not rows_produced and "number of output rows" in detailed_metrics:
        rows_produced = detailed_metrics["number of output rows"].get("value", 0)

    data_size = key_metrics.get("dataSize", 0)
    spill_bytes = key_metrics.get("spillToDisk", 0)

    return NodeMetrics(
        node_id=node_id,
        node_name=name,
        node_type=node_type,
        execution_time_ms=execution_time,
        rows_produced=rows_produced,
        data_size_bytes=data_size,
        spill_bytes=spill_bytes,
        attributes=node.get("attributes", {}),
    )


def _get_node_name(node: Dict[str, Any]) -> str:
    """Get the most meaningful name for a node."""
    # Try different name sources in order of preference
    name_sources = [
        node.get("name"),
        node.get("nodeName"),
        node.get("tag"),
        node.get("nodeType"),
        f"Node-{node.get('id', 'unknown')}",
    ]

    for name in name_sources:
        if name and isinstance(name, str) and name.strip():
            return name.strip()

    return "Unknown"


def _is_shuffle_node(node: Dict[str, Any]) -> bool:
    """Check if a node is a shuffle operation."""
    name = _get_node_name(node).lower()
    tag = node.get("tag", "").lower()

    shuffle_indicators = [
        "shuffle",
        "exchange",
        "sort",
        "aggregate",
        "repartition",
    ]

    return any(ind in name or ind in tag for ind in shuffle_indicators)


def _extract_shuffle_metrics(node: Dict[str, Any]) -> Optional[ShuffleMetrics]:
    """Extract shuffle-specific metrics from a node."""
    key_metrics = node.get("keyMetrics", {})
    detailed_metrics = node.get("detailedMetrics", {})

    shuffle_read = key_metrics.get("shuffleReadBytes", 0)
    shuffle_write = key_metrics.get("shuffleWriteBytes", 0)

    # Try detailed metrics if key metrics don't have values
    if not shuffle_read:
        shuffle_read = detailed_metrics.get("shuffle bytes read", {}).get("value", 0)
    if not shuffle_write:
        shuffle_write = detailed_metrics.get("shuffle bytes written", {}).get("value", 0)

    if not shuffle_read and not shuffle_write:
        return None

    return ShuffleMetrics(
        shuffle_id=node.get("id", ""),
        shuffle_read_bytes=shuffle_read,
        shuffle_write_bytes=shuffle_write,
        partition_count=key_metrics.get("numPartitions", 0),
    )


def calculate_filter_rate(input_rows: int, output_rows: int) -> FilterRateResult:
    """Calculate filter effectiveness rate.

    Args:
        input_rows: Number of input rows
        output_rows: Number of output rows

    Returns:
        FilterRateResult with calculated metrics
    """
    if input_rows <= 0:
        return FilterRateResult(
            input_rows=input_rows,
            output_rows=output_rows,
            filter_rate=0.0,
            is_effective=False,
        )

    filter_rate = 1.0 - (output_rows / input_rows)

    return FilterRateResult(
        input_rows=input_rows,
        output_rows=output_rows,
        filter_rate=filter_rate,
        is_effective=filter_rate > 0.5,  # More than 50% filtered is considered effective
    )
