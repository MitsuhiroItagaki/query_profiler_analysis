# Databricks notebook source
# MAGIC %md
# MAGIC # Databricks SQL Profiler Analysis Tool v2.0 (Full Version)
# MAGIC
# MAGIC This notebook uses the complete analysis logic from the original implementation
# MAGIC while benefiting from the modular configuration structure.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📁 Configuration

# COMMAND ----------

import sys
import os

# Set up Databricks credentials as environment variables
try:
    token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
    os.environ["DATABRICKS_TOKEN"] = token

    workspace_url = spark.conf.get("spark.databricks.workspaceUrl")
    os.environ["DATABRICKS_WORKSPACE_URL"] = workspace_url

    print(f"✅ Databricks credentials configured")
    print(f"🔗 Workspace: {workspace_url}")
except Exception as e:
    print(f"⚠️ Could not auto-configure Databricks credentials: {e}")

# COMMAND ----------

# Configuration - Edit these values
JSON_FILE_PATH = '/Workspace/Shared/AutoSQLTuning/query-profile.json'
OUTPUT_FILE_DIR = './output'
OUTPUT_LANGUAGE = 'en'  # 'ja' or 'en'
EXPLAIN_ENABLED = 'Y'
CATALOG = 'your_catalog'
DATABASE = 'your_database'
DEBUG_ENABLED = 'N'
MAX_OPTIMIZATION_ATTEMPTS = 3

# LLM Configuration
LLM_CONFIG = {
    "provider": "databricks",
    "databricks": {
        "endpoint_name": "databricks-claude-opus-4-5",
        "max_tokens": 32000,
        "temperature": 0.0,
        "thinking_enabled": False,
        "thinking_budget_tokens": 10000,
    },
    "openai": {
        "api_key": "",
        "model": "gpt-4o",
        "max_tokens": 16000,
        "temperature": 0.0,
    },
    "azure_openai": {
        "api_key": "",
        "endpoint": "",
        "deployment_name": "",
        "api_version": "2024-02-01",
        "max_tokens": 16000,
        "temperature": 0.0,
    },
    "anthropic": {
        "api_key": "",
        "model": "claude-3-5-sonnet-20241022",
        "max_tokens": 16000,
        "temperature": 0.0,
    },
}

print("✅ Configuration loaded")
print(f"📁 Input: {JSON_FILE_PATH}")
print(f"📂 Output: {OUTPUT_FILE_DIR}")
print(f"🌐 Language: {OUTPUT_LANGUAGE}")
print(f"🤖 LLM: {LLM_CONFIG['provider']} - {LLM_CONFIG[LLM_CONFIG['provider']].get('endpoint_name', LLM_CONFIG[LLM_CONFIG['provider']].get('model', ''))}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📦 Load Original Analysis Functions
# MAGIC
# MAGIC We run the original query_profiler_analysis.py to get all the detailed analysis functions.

# COMMAND ----------

# MAGIC %run ../query_profiler_analysis

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📊 Load and Analyze Profiler Data

# COMMAND ----------

# Load profiler JSON
profiler_data = load_profiler_json(JSON_FILE_PATH)

# Extract metrics using the full implementation
extracted_metrics = extract_performance_metrics(profiler_data)

print(f"\n📊 Data format: {extracted_metrics.get('data_format', 'unknown')}")
print(f"🔍 Query ID: {extracted_metrics.get('query_info', {}).get('query_id', 'N/A')}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔍 Bottleneck Analysis

# COMMAND ----------

# Calculate bottleneck indicators
bottleneck_indicators = extracted_metrics.get('bottleneck_indicators', {})

print("📊 Bottleneck Indicators:")
print(f"  - Cache hit ratio: {bottleneck_indicators.get('cache_hit_ratio', 0) * 100:.1f}%")
print(f"  - Data selectivity: {bottleneck_indicators.get('data_selectivity', 0) * 100:.1f}%")
print(f"  - Photon ratio: {bottleneck_indicators.get('photon_ratio', 0) * 100:.1f}%")
print(f"  - Has spill: {bottleneck_indicators.get('has_spill', False)}")
print(f"  - Spill bytes: {bottleneck_indicators.get('spill_bytes', 0) / (1024**3):.2f} GB")
print(f"  - Shuffle bottleneck: {bottleneck_indicators.get('has_shuffle_bottleneck', False)}")
print(f"  - Shuffle impact ratio: {bottleneck_indicators.get('shuffle_impact_ratio', 0) * 100:.1f}%")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🤖 LLM Bottleneck Analysis

# COMMAND ----------

# Run LLM-powered bottleneck analysis
analysis_result = analyze_bottlenecks_with_llm(extracted_metrics)
print(analysis_result)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📈 Top 10 Time-Consuming Processes

# COMMAND ----------

# Generate Top 10 report
top10_report = generate_top10_time_consuming_processes_report(extracted_metrics, limit_nodes=10, output_language=OUTPUT_LANGUAGE)
print(top10_report)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔄 Enhanced Shuffle Analysis

# COMMAND ----------

# Analyze shuffle operations
if SHUFFLE_ANALYSIS_CONFIG.get('shuffle_analysis_enabled', True):
    node_metrics = extracted_metrics.get('node_metrics', [])
    shuffle_analysis = analyze_enhanced_shuffle_operations(node_metrics, OUTPUT_LANGUAGE)

    if shuffle_analysis.get('shuffle_operations'):
        shuffle_report = generate_enhanced_shuffle_optimization_report(shuffle_analysis, OUTPUT_LANGUAGE)
        print(shuffle_report)
    else:
        print("ℹ️ No significant shuffle operations detected")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🚀 Query Optimization

# COMMAND ----------

# Extract original query
original_query = extract_original_query_from_profiler_data(profiler_data)
print(f"📝 Original query length: {len(original_query):,} characters")

if original_query and EXPLAIN_ENABLED == 'Y':
    # Run iterative optimization
    optimization_result = execute_iterative_optimization_with_degradation_analysis(
        original_query=original_query,
        analysis_result=analysis_result,
        metrics=extracted_metrics,
        max_optimization_attempts=MAX_OPTIMIZATION_ATTEMPTS
    )

    print(f"\n🎯 Optimization {'successful' if optimization_result.get('optimization_success', False) else 'completed'}")
    print(f"📊 Best attempt: #{optimization_result.get('best_attempt_number', 0)}")
else:
    optimization_result = {}
    print("⚠️ Query optimization skipped (no query or EXPLAIN disabled)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📝 Generate Report

# COMMAND ----------

# Generate comprehensive report
if optimization_result:
    query_id = extracted_metrics.get('query_info', {}).get('query_id', '')

    saved_files = save_optimized_sql_files(
        original_query=original_query,
        optimized_result=optimization_result.get('best_optimized_query', ''),
        metrics=extracted_metrics,
        analysis_result=analysis_result,
        performance_comparison=optimization_result.get('final_performance', {}),
        best_attempt_number=optimization_result.get('best_attempt_number', 0),
        optimization_attempts=optimization_result.get('attempts', []),
        optimization_success=optimization_result.get('optimization_success', False)
    )

    print("\n📁 Saved files:")
    for file_type, path in saved_files.items():
        print(f"  - {file_type}: {path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 💡 Liquid Clustering Analysis

# COMMAND ----------

# Display Liquid Clustering recommendations
lc_analysis = extracted_metrics.get('liquid_clustering_analysis', {})
if lc_analysis:
    lc_report = generate_liquid_clustering_markdown_report(lc_analysis)
    print(lc_report)
else:
    print("ℹ️ No Liquid Clustering recommendations available")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📋 Summary
# MAGIC
# MAGIC Analysis complete. Check the output directory for:
# MAGIC - Original SQL file
# MAGIC - Optimized SQL file (if optimization successful)
# MAGIC - Comprehensive optimization report
# MAGIC - Shuffle analysis report
