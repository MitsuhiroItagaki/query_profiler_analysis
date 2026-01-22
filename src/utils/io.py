"""I/O utility functions."""

import os
import json
from datetime import datetime
from typing import Any, Dict, Optional

from ..config import get_config


def get_output_path(filename: str, subdir: Optional[str] = None) -> str:
    """Get full output path for a file.

    Args:
        filename: Name of the file
        subdir: Optional subdirectory within output directory

    Returns:
        Full path to the output file
    """
    config = get_config()
    base_dir = config.output_file_dir

    if subdir:
        output_dir = os.path.join(base_dir, subdir)
        os.makedirs(output_dir, exist_ok=True)
    else:
        output_dir = base_dir

    return os.path.join(output_dir, filename)


def generate_timestamp_filename(prefix: str, extension: str, query_id: str = "") -> str:
    """Generate a filename with timestamp.

    Args:
        prefix: Filename prefix
        extension: File extension (without dot)
        query_id: Optional query ID to include

    Returns:
        Generated filename
    """
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    if query_id:
        # Truncate query_id if too long
        short_id = query_id[:8] if len(query_id) > 8 else query_id
        return f"{prefix}_{short_id}_{timestamp}.{extension}"

    return f"{prefix}_{timestamp}.{extension}"


def save_text_file(content: str, filename: str, subdir: Optional[str] = None) -> str:
    """Save text content to a file.

    Args:
        content: Text content to save
        filename: Name of the file
        subdir: Optional subdirectory

    Returns:
        Full path to the saved file
    """
    filepath = get_output_path(filename, subdir)

    with open(filepath, "w", encoding="utf-8") as f:
        f.write(content)

    print(f"✅ Saved: {filepath}")
    return filepath


def save_json_file(data: Dict[str, Any], filename: str, subdir: Optional[str] = None) -> str:
    """Save JSON data to a file.

    Args:
        data: Dictionary to save as JSON
        filename: Name of the file
        subdir: Optional subdirectory

    Returns:
        Full path to the saved file
    """
    filepath = get_output_path(filename, subdir)

    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2, default=str)

    print(f"✅ Saved: {filepath}")
    return filepath


def save_sql_file(
    sql: str,
    filename: str,
    catalog: str = "",
    database: str = "",
    subdir: Optional[str] = None,
) -> str:
    """Save SQL content to a file with optional USE statements.

    Args:
        sql: SQL content to save
        filename: Name of the file
        catalog: Optional catalog name
        database: Optional database name
        subdir: Optional subdirectory

    Returns:
        Full path to the saved file
    """
    lines = []

    # Add catalog/database settings if provided
    if catalog:
        lines.append(f"USE CATALOG {catalog};")
    if database:
        lines.append(f"USE SCHEMA {database};")
    if lines:
        lines.append("")

    lines.append(sql)

    content = "\n".join(lines)
    return save_text_file(content, filename, subdir)


def read_file(filepath: str) -> str:
    """Read text file content.

    Args:
        filepath: Path to the file

    Returns:
        File content as string
    """
    with open(filepath, "r", encoding="utf-8") as f:
        return f.read()


def find_latest_file(pattern: str, directory: Optional[str] = None) -> Optional[str]:
    """Find the most recently modified file matching a pattern.

    Args:
        pattern: Glob pattern or prefix to match
        directory: Directory to search in (defaults to output directory)

    Returns:
        Path to the most recent matching file, or None
    """
    import glob

    if directory is None:
        config = get_config()
        directory = config.output_file_dir

    search_pattern = os.path.join(directory, f"*{pattern}*")
    files = glob.glob(search_pattern)

    if not files:
        return None

    # Sort by modification time, newest first
    files.sort(key=os.path.getmtime, reverse=True)
    return files[0]


def cleanup_intermediate_files(pattern: str, keep_count: int = 0) -> int:
    """Clean up intermediate files matching a pattern.

    Args:
        pattern: Pattern to match files
        keep_count: Number of most recent files to keep

    Returns:
        Number of files deleted
    """
    import glob

    config = get_config()

    if config.debug_enabled:
        print("⚠️ Debug mode enabled, skipping cleanup")
        return 0

    search_pattern = os.path.join(config.output_file_dir, f"*{pattern}*")
    files = glob.glob(search_pattern)

    if not files:
        return 0

    # Sort by modification time
    files.sort(key=os.path.getmtime, reverse=True)

    # Keep the most recent files
    files_to_delete = files[keep_count:]

    deleted = 0
    for filepath in files_to_delete:
        try:
            os.remove(filepath)
            deleted += 1
        except OSError as e:
            print(f"⚠️ Could not delete {filepath}: {e}")

    if deleted:
        print(f"🗑️ Cleaned up {deleted} intermediate files")

    return deleted
