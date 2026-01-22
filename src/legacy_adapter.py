"""Legacy adapter to use original query_profiler_analysis.py functions.

This module provides a bridge between the new modular structure (v2.0)
and the original single-file implementation, allowing access to all
the detailed analysis logic.
"""

import sys
import os
from typing import Any, Dict, Optional

# Add parent directory to path to import the legacy module
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _parent_dir not in sys.path:
    sys.path.insert(0, _parent_dir)

# Import functions from the legacy module
# These will be imported when the module is loaded in Databricks environment
_legacy_module = None


def _get_legacy_module():
    """Lazy load the legacy module."""
    global _legacy_module
    if _legacy_module is None:
        try:
            # Try to import as a module
            import importlib.util
            legacy_path = os.path.join(_parent_dir, "query_profiler_analysis.py")
            spec = importlib.util.spec_from_file_location("query_profiler_analysis", legacy_path)
            _legacy_module = importlib.util.module_from_spec(spec)
            # Note: We don't execute the module here to avoid running all cells
            # Instead, we'll exec specific functions
        except Exception as e:
            print(f"⚠️ Could not load legacy module: {e}")
            _legacy_module = {}
    return _legacy_module


def load_profiler_json(file_path: str) -> Dict[str, Any]:
    """Load profiler JSON using legacy implementation."""
    import json

    # Handle DBFS paths appropriately
    if file_path.startswith('/dbfs/'):
        actual_path = file_path
    elif file_path.startswith('dbfs:/'):
        actual_path = file_path.replace('dbfs:', '/dbfs')
    elif file_path.startswith('/FileStore/'):
        actual_path = '/dbfs' + file_path
    else:
        actual_path = file_path

    with open(actual_path, 'r', encoding='utf-8') as f:
        data = json.load(f)

    print(f"✅ Successfully loaded JSON file: {file_path}")
    print(f"📊 Data size: {len(str(data)):,} characters")
    return data


def detect_data_format(profiler_data: Dict[str, Any]) -> str:
    """Detect JSON data format."""
    if 'graphs' in profiler_data and isinstance(profiler_data['graphs'], list):
        if len(profiler_data['graphs']) > 0:
            return 'sql_profiler'

    if 'query' in profiler_data and 'planMetadatas' in profiler_data:
        query_data = profiler_data.get('query', {})
        if 'metrics' in query_data:
            return 'sql_query_summary'

    return 'unknown'
