"""Pytest fixtures for testing."""

import pytest
from src.config import AnalysisConfig, LLMConfig, set_config


@pytest.fixture(autouse=True)
def reset_config():
    """Reset global config before each test."""
    config = AnalysisConfig(
        json_file_path="",
        output_file_dir="/tmp/test_output",
        output_language="en",
        explain_enabled=False,
        debug_enabled=True,
    )
    set_config(config)
    yield


@pytest.fixture
def sample_profiler_data():
    """Sample profiler data for testing."""
    return {
        "query": {
            "id": "test-query-001",
            "status": "FINISHED",
            "queryText": "SELECT * FROM test_table WHERE id > 100",
            "metrics": {
                "totalTimeMs": 5000,
                "executionTimeMs": 4500,
                "compilationTimeMs": 500,
                "readBytes": 1073741824,  # 1 GB
                "readCacheBytes": 536870912,  # 512 MB
                "spillToDiskBytes": 104857600,  # 100 MB
                "rowsReadCount": 1000000,
                "rowsProducedCount": 10000,
                "networkSentBytes": 52428800,  # 50 MB
            },
        },
        "planMetadatas": [],
    }


@pytest.fixture
def sample_sql_profiler_data():
    """Sample SQL profiler format data."""
    return {
        "graphs": [
            {
                "queryId": "test-query-002",
                "status": "FINISHED",
                "nodes": [
                    {
                        "id": "node-1",
                        "name": "Scan parquet",
                        "tag": "SCAN",
                        "keyMetrics": {
                            "durationMs": 2000,
                            "rowsNum": 1000000,
                            "dataSize": 1073741824,
                        },
                    },
                    {
                        "id": "node-2",
                        "name": "Filter",
                        "tag": "FILTER",
                        "keyMetrics": {
                            "durationMs": 500,
                            "rowsNum": 10000,
                        },
                    },
                    {
                        "id": "node-3",
                        "name": "HashAggregate",
                        "tag": "AGGREGATE",
                        "keyMetrics": {
                            "durationMs": 1500,
                            "rowsNum": 100,
                            "shuffleReadBytes": 52428800,
                            "shuffleWriteBytes": 5242880,
                        },
                    },
                ],
                "stats": {
                    "durationMs": 4000,
                    "readBytes": 1073741824,
                    "rowsNum": 100,
                    "spillToDiskBytes": 0,
                },
            }
        ],
    }


@pytest.fixture
def sample_original_query():
    """Sample original SQL query."""
    return """
SELECT
    customer_id,
    SUM(amount) as total_amount,
    COUNT(*) as transaction_count
FROM transactions t
JOIN customers c ON t.customer_id = c.id
WHERE t.transaction_date >= '2024-01-01'
GROUP BY customer_id
ORDER BY total_amount DESC
LIMIT 100;
"""
