"""Tests for profiler module."""

import pytest

from src.profiler.loader import detect_data_format, extract_query_text, extract_query_id
from src.profiler.metrics import extract_metrics, calculate_filter_rate
from src.profiler.bottleneck import analyze_bottlenecks
from src.models import OptimizationPriority


class TestDataFormatDetection:
    """Tests for data format detection."""

    def test_detect_sql_query_summary_format(self, sample_profiler_data):
        """Test detection of SQL query summary format."""
        result = detect_data_format(sample_profiler_data)
        assert result == "sql_query_summary"

    def test_detect_sql_profiler_format(self, sample_sql_profiler_data):
        """Test detection of SQL profiler format."""
        result = detect_data_format(sample_sql_profiler_data)
        assert result == "sql_profiler"

    def test_detect_unknown_format(self):
        """Test detection of unknown format."""
        result = detect_data_format({"random": "data"})
        assert result == "unknown"


class TestQueryExtraction:
    """Tests for query extraction."""

    def test_extract_query_text_from_summary(self, sample_profiler_data):
        """Test query text extraction from summary format."""
        result = extract_query_text(sample_profiler_data)
        assert "SELECT" in result
        assert "test_table" in result

    def test_extract_query_id_from_summary(self, sample_profiler_data):
        """Test query ID extraction from summary format."""
        result = extract_query_id(sample_profiler_data)
        assert result == "test-query-001"


class TestMetricsExtraction:
    """Tests for metrics extraction."""

    def test_extract_metrics_from_summary(self, sample_profiler_data):
        """Test metrics extraction from summary format."""
        metrics = extract_metrics(sample_profiler_data)

        assert metrics.query_metrics.query_id == "test-query-001"
        assert metrics.query_metrics.execution_time_ms == 4500
        assert metrics.query_metrics.total_size_bytes == 1073741824
        assert metrics.query_metrics.row_count == 1000000
        assert metrics.query_metrics.spill_to_disk_bytes == 104857600

    def test_extract_metrics_from_profiler(self, sample_sql_profiler_data):
        """Test metrics extraction from SQL profiler format."""
        metrics = extract_metrics(sample_sql_profiler_data)

        assert metrics.query_metrics.query_id == "test-query-002"
        assert len(metrics.node_metrics) == 3
        assert len(metrics.top_time_consuming_nodes) <= 10

    def test_cache_hit_ratio_calculation(self, sample_profiler_data):
        """Test cache hit ratio calculation."""
        metrics = extract_metrics(sample_profiler_data)

        # 512 MB cache / 1 GB total = 0.5
        assert abs(metrics.query_metrics.cache_hit_ratio - 0.5) < 0.01


class TestFilterRate:
    """Tests for filter rate calculation."""

    def test_calculate_filter_rate_effective(self):
        """Test effective filter rate calculation."""
        result = calculate_filter_rate(1000000, 100000)

        assert result.input_rows == 1000000
        assert result.output_rows == 100000
        assert abs(result.filter_rate - 0.9) < 0.01
        assert result.is_effective is True

    def test_calculate_filter_rate_ineffective(self):
        """Test ineffective filter rate calculation."""
        result = calculate_filter_rate(1000, 900)

        assert abs(result.filter_rate - 0.1) < 0.01
        assert result.is_effective is False

    def test_calculate_filter_rate_zero_input(self):
        """Test filter rate with zero input."""
        result = calculate_filter_rate(0, 0)

        assert result.filter_rate == 0.0
        assert result.is_effective is False


class TestBottleneckAnalysis:
    """Tests for bottleneck analysis."""

    def test_detect_spill_bottleneck(self, sample_profiler_data):
        """Test spill bottleneck detection."""
        metrics = extract_metrics(sample_profiler_data)
        indicators = analyze_bottlenecks(metrics)

        spill_indicators = [i for i in indicators if "Spill" in i.name or "スピル" in i.name]
        assert len(spill_indicators) > 0

    def test_detect_cache_bottleneck(self, sample_profiler_data):
        """Test cache bottleneck detection."""
        metrics = extract_metrics(sample_profiler_data)
        indicators = analyze_bottlenecks(metrics)

        cache_indicators = [i for i in indicators if "Cache" in i.name or "キャッシュ" in i.name]
        assert len(cache_indicators) > 0

    def test_no_bottlenecks_for_optimal_query(self):
        """Test that optimal query has minimal bottlenecks."""
        optimal_data = {
            "query": {
                "id": "optimal-query",
                "status": "FINISHED",
                "queryText": "SELECT 1",
                "metrics": {
                    "totalTimeMs": 100,
                    "executionTimeMs": 90,
                    "readBytes": 1024,
                    "readCacheBytes": 1024,  # 100% cache hit
                    "spillToDiskBytes": 0,
                    "rowsReadCount": 10,
                    "rowsProducedCount": 10,
                    "networkSentBytes": 0,
                },
            },
            "planMetadatas": [],
        }

        metrics = extract_metrics(optimal_data)
        indicators = analyze_bottlenecks(metrics)

        high_priority = [i for i in indicators if i.severity == OptimizationPriority.HIGH]
        assert len(high_priority) == 0
