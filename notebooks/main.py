# Databricks notebook source
# MAGIC %md
# MAGIC # Databricks SQL Profiler Analysis Tool v2.0
# MAGIC
# MAGIC Modular tool to analyze SQL Profiler JSON logs and generate optimization recommendations.
# MAGIC
# MAGIC ## Features
# MAGIC - Multi-provider LLM support (Databricks, OpenAI, Azure OpenAI, Anthropic)
# MAGIC - Bottleneck detection and analysis
# MAGIC - Iterative query optimization
# MAGIC - Comprehensive reporting (Japanese/English)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📁 Configuration

# COMMAND ----------

import sys
sys.path.insert(0, '..')

from src.config import AnalysisConfig, LLMConfig, DatabricksLLMConfig, set_config

# Configure analysis settings
config = AnalysisConfig(
    # Input/Output
    json_file_path='/Workspace/Shared/AutoSQLTuning/query-profile.json',
    output_file_dir='./output',

    # Language: 'ja' for Japanese, 'en' for English
    output_language='en',

    # EXPLAIN execution
    explain_enabled=True,
    catalog='your_catalog',
    database='your_database',

    # Debug mode
    debug_enabled=False,

    # Optimization settings
    max_optimization_attempts=3,
    max_retries=3,

    # LLM Configuration
    llm=LLMConfig(
        provider='databricks',
        databricks=DatabricksLLMConfig(
            endpoint_name='databricks-claude-opus-4-5',
            max_tokens=32000,
            temperature=0.0,
            thinking_enabled=False,
        ),
    ),
)

set_config(config)
print("✅ Configuration loaded")
print(f"📁 Input file: {config.json_file_path}")
print(f"📂 Output directory: {config.output_file_dir}")
print(f"🌐 Language: {config.output_language}")
print(f"🤖 LLM Provider: {config.llm.provider}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📊 Load and Analyze Profiler Data

# COMMAND ----------

from src.profiler import (
    load_profiler_json,
    extract_metrics,
    extract_query_text,
    analyze_bottlenecks,
    format_bottleneck_report,
)

# Load profiler JSON
profiler_data = load_profiler_json(config.json_file_path)

# Extract metrics
metrics = extract_metrics(profiler_data)
print(f"\n📊 Query ID: {metrics.query_metrics.query_id}")
print(f"⏱️ Execution time: {metrics.query_metrics.execution_time_ms:,.0f} ms")
print(f"📦 Data size: {metrics.query_metrics.total_size_bytes / (1024**3):.2f} GB")
print(f"📝 Rows: {metrics.query_metrics.row_count:,}")

# Extract original query
original_query = extract_query_text(profiler_data)
print(f"\n📝 Original query length: {len(original_query):,} characters")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔍 Bottleneck Analysis

# COMMAND ----------

# Analyze bottlenecks
bottleneck_indicators = analyze_bottlenecks(metrics)

print(f"\n🔍 Detected {len(bottleneck_indicators)} bottleneck(s)")

# Generate bottleneck report
bottleneck_report = format_bottleneck_report(bottleneck_indicators, config.output_language)
print(bottleneck_report)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🚀 Query Optimization

# COMMAND ----------

from src.optimization import execute_iterative_optimization

# Run iterative optimization
optimization_result = execute_iterative_optimization(
    original_query=original_query,
    metrics=metrics,
    execute_explain_fn=None,  # Set to actual EXPLAIN function if available
)

print(f"\n🎯 Optimization {'successful' if optimization_result.optimization_success else 'not improved'}")
print(f"📊 Total attempts: {len(optimization_result.attempts)}")
if optimization_result.optimization_success:
    print(f"✅ Best attempt: #{optimization_result.best_attempt_number}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📝 Generate Report

# COMMAND ----------

from src.report import generate_comprehensive_report, save_optimization_files

# Generate comprehensive report
report = generate_comprehensive_report(
    metrics=metrics,
    bottleneck_indicators=bottleneck_indicators,
    optimization_result=optimization_result,
)

# Display report
print(report)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 💾 Save Results

# COMMAND ----------

# Save all files
saved_files = save_optimization_files(
    original_query=original_query,
    optimization_result=optimization_result,
    report_content=report,
    query_id=metrics.query_metrics.query_id,
)

print("\n📁 Saved files:")
for file_type, path in saved_files.items():
    print(f"  - {file_type}: {path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📋 Summary
# MAGIC
# MAGIC Analysis complete. Check the output directory for:
# MAGIC - Original SQL file
# MAGIC - Optimized SQL file (if optimization successful)
# MAGIC - Comprehensive optimization report
