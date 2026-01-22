"""Optimized query generation using LLM."""

from typing import Any, Dict, Optional

from ..config import get_config, t
from ..llm import call_llm
from ..models import ExtractedMetrics, OptimizationAttempt, TrialType
from ..utils.sql import extract_sql_from_llm_response, clean_sql


def generate_optimized_query(
    original_query: str,
    metrics: ExtractedMetrics,
    bottleneck_analysis: str,
) -> str:
    """Generate an optimized version of the query using LLM.

    Args:
        original_query: Original SQL query
        metrics: Extracted metrics from profiler
        bottleneck_analysis: Bottleneck analysis report

    Returns:
        Optimized SQL query
    """
    config = get_config()
    prompt = _build_optimization_prompt(
        original_query,
        metrics,
        bottleneck_analysis,
        config.output_language,
    )

    response = call_llm(prompt)
    optimized_sql = extract_sql_from_llm_response(response)

    if not optimized_sql:
        print("⚠️ Could not extract SQL from LLM response, using original query")
        return original_query

    return clean_sql(optimized_sql)


def generate_refined_query(
    original_query: str,
    metrics: ExtractedMetrics,
    previous_attempt: OptimizationAttempt,
    bottleneck_analysis: str,
) -> str:
    """Generate a refined query based on previous attempt results.

    Args:
        original_query: Original SQL query
        metrics: Extracted metrics from profiler
        previous_attempt: Previous optimization attempt
        bottleneck_analysis: Bottleneck analysis report

    Returns:
        Refined SQL query
    """
    config = get_config()
    prompt = _build_refinement_prompt(
        original_query,
        metrics,
        previous_attempt,
        bottleneck_analysis,
        config.output_language,
    )

    response = call_llm(prompt)
    refined_sql = extract_sql_from_llm_response(response)

    if not refined_sql:
        print("⚠️ Could not extract SQL from LLM response, using previous query")
        return previous_attempt.query

    return clean_sql(refined_sql)


def generate_error_corrected_query(
    original_query: str,
    failed_query: str,
    error_message: str,
) -> str:
    """Generate a corrected query after an error.

    Args:
        original_query: Original SQL query
        failed_query: Query that produced the error
        error_message: Error message from execution

    Returns:
        Corrected SQL query
    """
    config = get_config()
    prompt = _build_error_correction_prompt(
        original_query,
        failed_query,
        error_message,
        config.output_language,
    )

    response = call_llm(prompt)
    corrected_sql = extract_sql_from_llm_response(response)

    if not corrected_sql:
        print("⚠️ Could not extract SQL from LLM response, using original query")
        return original_query

    return clean_sql(corrected_sql)


def _build_optimization_prompt(
    original_query: str,
    metrics: ExtractedMetrics,
    bottleneck_analysis: str,
    language: str,
) -> str:
    """Build the optimization prompt for LLM."""
    query_metrics = metrics.query_metrics

    if language == "ja":
        return f"""あなたはDatabricks SQLの最適化エキスパートです。
以下のクエリを分析し、パフォーマンスを改善した最適化クエリを生成してください。

## 元のクエリ
```sql
{original_query}
```

## パフォーマンスメトリクス
- 実行時間: {query_metrics.execution_time_ms:,} ms
- データサイズ: {query_metrics.total_size_bytes / (1024**3):.2f} GB
- 処理行数: {query_metrics.row_count:,}
- キャッシュヒット率: {query_metrics.cache_hit_ratio * 100:.1f}%
- ディスクスピル: {query_metrics.spill_to_disk_bytes / (1024**3):.2f} GB

## ボトルネック分析
{bottleneck_analysis}

## 最適化の指針
1. BROADCAST ヒントの追加（小さいテーブルに対して）
2. パーティションプルーニングの活用
3. 不要なカラムの削除
4. JOIN 順序の最適化
5. フィルタの早期適用

## 出力形式
最適化されたSQLを```sql```ブロックで出力してください。
元のクエリと同じ結果を返すことを保証してください。
"""
    else:
        return f"""You are a Databricks SQL optimization expert.
Analyze the following query and generate an optimized version with improved performance.

## Original Query
```sql
{original_query}
```

## Performance Metrics
- Execution time: {query_metrics.execution_time_ms:,} ms
- Data size: {query_metrics.total_size_bytes / (1024**3):.2f} GB
- Rows processed: {query_metrics.row_count:,}
- Cache hit ratio: {query_metrics.cache_hit_ratio * 100:.1f}%
- Disk spill: {query_metrics.spill_to_disk_bytes / (1024**3):.2f} GB

## Bottleneck Analysis
{bottleneck_analysis}

## Optimization Guidelines
1. Add BROADCAST hints for small tables
2. Leverage partition pruning
3. Remove unnecessary columns
4. Optimize JOIN order
5. Apply filters early

## Output Format
Output the optimized SQL in a ```sql``` code block.
Ensure the query returns the same results as the original.
"""


def _build_refinement_prompt(
    original_query: str,
    metrics: ExtractedMetrics,
    previous_attempt: OptimizationAttempt,
    bottleneck_analysis: str,
    language: str,
) -> str:
    """Build the refinement prompt for LLM."""
    if language == "ja":
        return f"""前回の最適化結果を踏まえ、さらに改善したクエリを生成してください。

## 元のクエリ
```sql
{original_query}
```

## 前回の最適化クエリ
```sql
{previous_attempt.query}
```

## 前回の結果
- 試行タイプ: {previous_attempt.trial_type.value}
- 成功: {"はい" if previous_attempt.is_successful else "いいえ"}
- パフォーマンス改善: {"あり" if previous_attempt.performance and previous_attempt.performance.is_improved else "なし"}

## ボトルネック分析
{bottleneck_analysis}

## 指示
前回の最適化で解決されなかった問題に焦点を当て、
さらに改善したSQLを```sql```ブロックで出力してください。
"""
    else:
        return f"""Based on the previous optimization results, generate a further improved query.

## Original Query
```sql
{original_query}
```

## Previous Optimized Query
```sql
{previous_attempt.query}
```

## Previous Results
- Trial type: {previous_attempt.trial_type.value}
- Successful: {"Yes" if previous_attempt.is_successful else "No"}
- Performance improved: {"Yes" if previous_attempt.performance and previous_attempt.performance.is_improved else "No"}

## Bottleneck Analysis
{bottleneck_analysis}

## Instructions
Focus on issues not resolved by the previous optimization,
and output a further improved SQL in a ```sql``` code block.
"""


def _build_error_correction_prompt(
    original_query: str,
    failed_query: str,
    error_message: str,
    language: str,
) -> str:
    """Build the error correction prompt for LLM."""
    if language == "ja":
        return f"""以下のクエリでエラーが発生しました。エラーを修正したクエリを生成してください。

## 元のクエリ（動作する）
```sql
{original_query}
```

## エラーが発生したクエリ
```sql
{failed_query}
```

## エラーメッセージ
{error_message}

## 指示
1. エラーの原因を特定してください
2. 元のクエリの結果を変えずにエラーを修正してください
3. 修正したSQLを```sql```ブロックで出力してください
"""
    else:
        return f"""The following query produced an error. Generate a corrected query.

## Original Query (working)
```sql
{original_query}
```

## Query with Error
```sql
{failed_query}
```

## Error Message
{error_message}

## Instructions
1. Identify the cause of the error
2. Fix the error without changing the result of the original query
3. Output the corrected SQL in a ```sql``` code block
"""
