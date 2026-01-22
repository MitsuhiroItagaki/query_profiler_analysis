"""SQL utility functions."""

import re
from typing import List, Optional, Tuple


def extract_sql_from_llm_response(llm_response: str) -> str:
    """Extract SQL query from LLM response.

    Args:
        llm_response: Raw LLM response text

    Returns:
        Extracted SQL query
    """
    if not llm_response:
        return ""

    # Try to extract from code blocks
    patterns = [
        r"```sql\s*(.*?)\s*```",
        r"```\s*(SELECT.*?)\s*```",
        r"```\s*(WITH.*?)\s*```",
        r"```\s*(CREATE.*?)\s*```",
    ]

    for pattern in patterns:
        match = re.search(pattern, llm_response, re.DOTALL | re.IGNORECASE)
        if match:
            return clean_sql(match.group(1))

    # If no code block found, try to find SQL directly
    sql_patterns = [
        r"(SELECT\s+.*?;)",
        r"(WITH\s+.*?;)",
        r"(CREATE\s+.*?;)",
    ]

    for pattern in sql_patterns:
        match = re.search(pattern, llm_response, re.DOTALL | re.IGNORECASE)
        if match:
            return clean_sql(match.group(1))

    return ""


def clean_sql(sql: str) -> str:
    """Clean and normalize SQL query.

    Args:
        sql: Raw SQL string

    Returns:
        Cleaned SQL string
    """
    if not sql:
        return ""

    # Remove leading/trailing whitespace
    sql = sql.strip()

    # Remove SQL comments
    sql = re.sub(r"--.*$", "", sql, flags=re.MULTILINE)
    sql = re.sub(r"/\*.*?\*/", "", sql, flags=re.DOTALL)

    # Normalize whitespace
    sql = re.sub(r"\s+", " ", sql)
    sql = sql.strip()

    # Ensure single semicolon at end
    sql = sql.rstrip(";") + ";"

    return sql


def extract_select_from_ctas(query: str) -> str:
    """Extract SELECT statement from CREATE TABLE AS SELECT.

    Args:
        query: CTAS query

    Returns:
        SELECT portion of the query
    """
    if not query:
        return ""

    # Match CTAS pattern
    pattern = r"CREATE\s+(?:OR\s+REPLACE\s+)?TABLE\s+\S+\s+(?:USING\s+\S+\s+)?AS\s+(SELECT\s+.+)"
    match = re.search(pattern, query, re.IGNORECASE | re.DOTALL)

    if match:
        return clean_sql(match.group(1))

    return query


def fix_broadcast_hint_placement(sql: str) -> str:
    """Fix BROADCAST hint placement in SQL.

    Args:
        sql: SQL query with potentially misplaced BROADCAST hints

    Returns:
        SQL with correctly placed BROADCAST hints
    """
    if not sql or "/*+" not in sql:
        return sql

    # Pattern for misplaced broadcast hints
    # /*+ BROADCAST(table) */ should be after SELECT or after JOIN keyword
    pattern = r"FROM\s+/\*\+\s*BROADCAST\s*\(\s*(\w+)\s*\)\s*\*/\s*(\w+)"

    def fix_hint(match):
        table = match.group(1)
        actual_table = match.group(2)
        if table.lower() == actual_table.lower():
            return f"FROM {actual_table}"
        return match.group(0)

    return re.sub(pattern, fix_hint, sql, flags=re.IGNORECASE)


def extract_table_names(sql: str) -> List[str]:
    """Extract table names from SQL query.

    Args:
        sql: SQL query

    Returns:
        List of table names found in the query
    """
    if not sql:
        return []

    tables = set()

    # FROM clause tables
    from_pattern = r"FROM\s+([a-zA-Z_][\w.]*)"
    tables.update(re.findall(from_pattern, sql, re.IGNORECASE))

    # JOIN clause tables
    join_pattern = r"JOIN\s+([a-zA-Z_][\w.]*)"
    tables.update(re.findall(join_pattern, sql, re.IGNORECASE))

    # Filter out common SQL keywords that might be matched
    keywords = {"select", "from", "where", "join", "on", "and", "or", "as", "in"}
    tables = {t for t in tables if t.lower() not in keywords}

    return list(tables)


def extract_broadcast_hints(sql: str) -> List[str]:
    """Extract tables with BROADCAST hints from SQL.

    Args:
        sql: SQL query

    Returns:
        List of table names with BROADCAST hints
    """
    if not sql:
        return []

    pattern = r"BROADCAST\s*\(\s*([a-zA-Z_][\w.]*)\s*\)"
    return re.findall(pattern, sql, re.IGNORECASE)


def validate_sql_syntax(sql: str) -> Tuple[bool, Optional[str]]:
    """Basic SQL syntax validation.

    Args:
        sql: SQL query to validate

    Returns:
        Tuple of (is_valid, error_message)
    """
    if not sql or not sql.strip():
        return False, "Empty SQL query"

    sql_upper = sql.upper().strip()

    # Check for basic structure
    if not any(sql_upper.startswith(kw) for kw in ["SELECT", "WITH", "CREATE", "INSERT"]):
        return False, "Query must start with SELECT, WITH, CREATE, or INSERT"

    # Check for balanced parentheses
    if sql.count("(") != sql.count(")"):
        return False, "Unbalanced parentheses"

    # Check for common issues
    if "''" in sql and "ESCAPE" not in sql_upper:
        # This might be intentional empty string, skip check
        pass

    # Check for incomplete clauses
    incomplete_patterns = [
        (r"SELECT\s*$", "Incomplete SELECT clause"),
        (r"FROM\s*$", "Incomplete FROM clause"),
        (r"WHERE\s*$", "Incomplete WHERE clause"),
        (r"JOIN\s*$", "Incomplete JOIN clause"),
        (r"ON\s*$", "Incomplete ON clause"),
    ]

    for pattern, error in incomplete_patterns:
        if re.search(pattern, sql, re.IGNORECASE):
            return False, error

    return True, None


def format_sql(sql: str, indent: int = 2) -> str:
    """Basic SQL formatting.

    Args:
        sql: SQL query to format
        indent: Number of spaces for indentation

    Returns:
        Formatted SQL string
    """
    if not sql:
        return ""

    keywords = [
        "SELECT", "FROM", "WHERE", "AND", "OR", "JOIN",
        "LEFT JOIN", "RIGHT JOIN", "INNER JOIN", "OUTER JOIN",
        "FULL JOIN", "CROSS JOIN", "ON", "GROUP BY", "ORDER BY",
        "HAVING", "LIMIT", "UNION", "UNION ALL", "WITH", "AS",
    ]

    result = sql.strip()

    # Add newlines before major keywords
    for kw in keywords:
        pattern = rf"(\s+)({kw}\s)"
        result = re.sub(pattern, rf"\n{kw} ", result, flags=re.IGNORECASE)

    # Clean up multiple newlines
    result = re.sub(r"\n+", "\n", result)

    return result.strip()
