"""Profiler JSON file loader."""

import json
from typing import Any, Dict
from pathlib import Path


def load_profiler_json(file_path: str) -> Dict[str, Any]:
    """Load SQL profiler JSON file.

    Args:
        file_path: JSON file path (DBFS, Workspace, or local path)

    Returns:
        Parsed JSON data

    Raises:
        FileNotFoundError: If file doesn't exist
        json.JSONDecodeError: If file is not valid JSON
    """
    actual_path = _resolve_path(file_path)

    with open(actual_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    print(f"✅ Successfully loaded JSON file: {file_path}")
    print(f"📊 Data size: {len(str(data)):,} characters")

    return data


def _resolve_path(file_path: str) -> str:
    """Resolve file path for different Databricks path formats.

    Args:
        file_path: Original file path

    Returns:
        Resolved local file path
    """
    if file_path.startswith("/dbfs/"):
        return file_path
    elif file_path.startswith("dbfs:/"):
        return file_path.replace("dbfs:", "/dbfs")
    elif file_path.startswith("/FileStore/"):
        return f"/dbfs{file_path}"
    elif file_path.startswith("/Workspace/"):
        return file_path
    else:
        return file_path


def detect_data_format(profiler_data: Dict[str, Any]) -> str:
    """Detect JSON data format.

    Args:
        profiler_data: Loaded profiler data

    Returns:
        Format identifier: 'sql_profiler', 'sql_query_summary', or 'unknown'
    """
    # SQL profiler format detection
    if "graphs" in profiler_data and isinstance(profiler_data["graphs"], list):
        if len(profiler_data["graphs"]) > 0:
            return "sql_profiler"

    # SQL query summary format detection
    if "query" in profiler_data and "planMetadatas" in profiler_data:
        query_data = profiler_data.get("query", {})
        if "metrics" in query_data:
            return "sql_query_summary"

    return "unknown"


def extract_query_text(profiler_data: Dict[str, Any]) -> str:
    """Extract original query text from profiler data.

    Args:
        profiler_data: Loaded profiler data

    Returns:
        Query text or empty string if not found
    """
    data_format = detect_data_format(profiler_data)

    if data_format == "sql_query_summary":
        return profiler_data.get("query", {}).get("queryText", "")

    if data_format == "sql_profiler":
        graphs = profiler_data.get("graphs", [])
        if graphs:
            first_graph = graphs[0]
            if "queryText" in first_graph:
                return first_graph["queryText"]
            # Try to find in plan metadata
            plan_metadatas = profiler_data.get("planMetadatas", [])
            for metadata in plan_metadatas:
                if "queryText" in metadata:
                    return metadata["queryText"]

    return ""


def extract_query_id(profiler_data: Dict[str, Any]) -> str:
    """Extract query ID from profiler data.

    Args:
        profiler_data: Loaded profiler data

    Returns:
        Query ID or empty string if not found
    """
    data_format = detect_data_format(profiler_data)

    if data_format == "sql_query_summary":
        return profiler_data.get("query", {}).get("id", "")

    if data_format == "sql_profiler":
        graphs = profiler_data.get("graphs", [])
        if graphs:
            return graphs[0].get("queryId", "")

    return ""
