"""Tests for utility modules."""

import pytest

from src.utils.sql import (
    extract_sql_from_llm_response,
    clean_sql,
    extract_select_from_ctas,
    extract_table_names,
    extract_broadcast_hints,
    validate_sql_syntax,
)


class TestSqlExtraction:
    """Tests for SQL extraction from LLM responses."""

    def test_extract_from_sql_code_block(self):
        """Test extraction from SQL code block."""
        response = """
Here is the optimized query:

```sql
SELECT id, name
FROM users
WHERE active = true;
```

This should improve performance.
"""
        result = extract_sql_from_llm_response(response)
        assert "SELECT id, name" in result
        assert "FROM users" in result

    def test_extract_from_generic_code_block(self):
        """Test extraction from generic code block."""
        response = """
```
SELECT * FROM table1;
```
"""
        result = extract_sql_from_llm_response(response)
        assert "SELECT * FROM table1" in result

    def test_extract_without_code_block(self):
        """Test extraction when no code block present."""
        response = "SELECT id FROM users WHERE id = 1;"
        result = extract_sql_from_llm_response(response)
        assert "SELECT id FROM users" in result

    def test_empty_response(self):
        """Test extraction from empty response."""
        result = extract_sql_from_llm_response("")
        assert result == ""


class TestSqlCleaning:
    """Tests for SQL cleaning."""

    def test_clean_whitespace(self):
        """Test whitespace normalization."""
        sql = "  SELECT   id   FROM   users  "
        result = clean_sql(sql)
        assert result == "SELECT id FROM users;"

    def test_remove_comments(self):
        """Test comment removal."""
        sql = """
-- This is a comment
SELECT id FROM users
/* Multi-line
   comment */
WHERE id = 1
"""
        result = clean_sql(sql)
        assert "--" not in result
        assert "/*" not in result
        assert "SELECT id FROM users WHERE id = 1" in result

    def test_ensure_semicolon(self):
        """Test semicolon handling."""
        sql = "SELECT 1"
        result = clean_sql(sql)
        assert result.endswith(";")

        sql_with_semicolon = "SELECT 1;"
        result = clean_sql(sql_with_semicolon)
        assert result.count(";") == 1


class TestCtasExtraction:
    """Tests for CTAS extraction."""

    def test_extract_select_from_ctas(self):
        """Test SELECT extraction from CTAS."""
        ctas = """
CREATE TABLE new_table AS
SELECT id, name
FROM users
WHERE active = true
"""
        result = extract_select_from_ctas(ctas)
        assert result.startswith("SELECT")
        assert "CREATE TABLE" not in result

    def test_extract_select_from_create_or_replace(self):
        """Test extraction from CREATE OR REPLACE TABLE."""
        ctas = """
CREATE OR REPLACE TABLE new_table AS
SELECT * FROM source
"""
        result = extract_select_from_ctas(ctas)
        assert "SELECT * FROM source" in result

    def test_non_ctas_query(self):
        """Test non-CTAS query passes through."""
        query = "SELECT * FROM users"
        result = extract_select_from_ctas(query)
        assert "SELECT * FROM users" in result


class TestTableExtraction:
    """Tests for table name extraction."""

    def test_extract_from_simple_query(self):
        """Test extraction from simple query."""
        sql = "SELECT * FROM users"
        result = extract_table_names(sql)
        assert "users" in result

    def test_extract_from_join_query(self):
        """Test extraction from JOIN query."""
        sql = """
SELECT u.id, o.amount
FROM users u
JOIN orders o ON u.id = o.user_id
LEFT JOIN products p ON o.product_id = p.id
"""
        result = extract_table_names(sql)
        assert "users" in result
        assert "orders" in result
        assert "products" in result

    def test_extract_qualified_table_names(self):
        """Test extraction of qualified table names."""
        sql = "SELECT * FROM catalog.schema.table_name"
        result = extract_table_names(sql)
        assert "catalog.schema.table_name" in result


class TestBroadcastHints:
    """Tests for broadcast hint extraction."""

    def test_extract_broadcast_hints(self):
        """Test broadcast hint extraction."""
        sql = """
SELECT /*+ BROADCAST(small_table) */
    t1.id, t2.name
FROM large_table t1
JOIN small_table t2 ON t1.id = t2.id
"""
        result = extract_broadcast_hints(sql)
        assert "small_table" in result

    def test_no_broadcast_hints(self):
        """Test query without broadcast hints."""
        sql = "SELECT * FROM users"
        result = extract_broadcast_hints(sql)
        assert len(result) == 0


class TestSqlValidation:
    """Tests for SQL validation."""

    def test_valid_select(self):
        """Test valid SELECT statement."""
        sql = "SELECT id FROM users WHERE id = 1"
        is_valid, error = validate_sql_syntax(sql)
        assert is_valid is True
        assert error is None

    def test_valid_with_clause(self):
        """Test valid WITH clause."""
        sql = "WITH cte AS (SELECT 1) SELECT * FROM cte"
        is_valid, error = validate_sql_syntax(sql)
        assert is_valid is True

    def test_empty_query(self):
        """Test empty query validation."""
        is_valid, error = validate_sql_syntax("")
        assert is_valid is False
        assert "Empty" in error

    def test_unbalanced_parentheses(self):
        """Test unbalanced parentheses detection."""
        sql = "SELECT * FROM (SELECT id FROM users"
        is_valid, error = validate_sql_syntax(sql)
        assert is_valid is False
        assert "parentheses" in error.lower()

    def test_invalid_start(self):
        """Test query with invalid start."""
        sql = "UPDATE users SET name = 'test'"
        is_valid, error = validate_sql_syntax(sql)
        assert is_valid is False
