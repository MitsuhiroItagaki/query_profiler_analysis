# Databricks SQL Profiler Analysis Tool

A single-file tool to analyze Databricks SQL Profiler JSON logs with an LLM (Databricks Model Serving, OpenAI, Azure OpenAI, or Anthropic), identify bottlenecks, and propose concrete optimization steps.

- **Main script**: `query_profiler_analysis.py`
- **Outputs**: optimization reports and SQL files (see Outputs below)

## Features
- **Profiler log ingestion**: Reads Databricks SQL Profiler JSON (parses `graphs` and key metrics)
- **Metrics extraction**: Query basics, execution time, data volume, cache efficiency, stage/node details
- **Bottleneck indicators**: Highlights skew, spill, shuffle, I/O hotspots, Photon utilization and more
- **🔧 Enhanced Shuffle Optimization**: Memory efficiency validation (≤512MB per partition) with actionable recommendations
- **Priority-based recommendations**: HIGH/MEDIUM/LOW optimization priorities with specific parameter suggestions
- **Spark tuning guidance**: Automated Spark parameter recommendations for optimal performance
- **LLM-assisted analysis**: Optional refinement of the report with specific, actionable recommendations
- **Language support**: Output in English or Japanese
- **Safe debug mode**: Keep or clean up intermediate artifacts

## Requirements
- Databricks Runtime (run as a notebook)
- Optional: `requests` for some HTTP interactions

## Quick start

### Run on Databricks
1. Import or create a notebook from `query_profiler_analysis.py`.
2. Open the configuration cell near the top of the file and set key variables:

```python
# File path in your workspace/DBFS
JSON_FILE_PATH = '/Workspace/Shared/AutoSQLTuning/Query1.json'

# Catalog/Database for EXPLAIN (when enabled)
CATALOG = 'tpcds'
DATABASE = 'tpcds_sf1000_delta_lc'

# Output language: 'ja' or 'en'
OUTPUT_LANGUAGE = 'ja'

# Enable/disable EXPLAIN execution on Databricks
EXPLAIN_ENABLED = 'Y'

# Preserve intermediate files if 'Y'
DEBUG_ENABLED = 'Y'

# Max number of iterative optimization attempts
MAX_OPTIMIZATION_ATTEMPTS = 2

# 🔧 Enhanced Shuffle Optimization Settings (New Feature)
SHUFFLE_ANALYSIS_CONFIG = {
    "memory_per_partition_threshold_mb": 512,  # Memory efficiency threshold
    "high_memory_threshold_gb": 100,           # High memory usage threshold
    "shuffle_analysis_enabled": True           # Enable/disable enhanced analysis
}
```

3. Configure LLM provider (required):

```python
LLM_CONFIG = {
    "provider": "databricks",  # 'databricks' | 'openai' | 'azure_openai' | 'anthropic'
    "databricks": {
        "endpoint_name": "databricks-claude-3-7-sonnet",
        "max_tokens": 131072,
        "temperature": 0.0,
    },
    "openai": {
        "api_key": "",           # or set OPENAI_API_KEY in env
        "model": "gpt-4o",
        "max_tokens": 16000,
        "temperature": 0.0,
    },
    "azure_openai": {
        "api_key": "",           # or set AZURE_OPENAI_API_KEY in env
        "endpoint": "",
        "deployment_name": "",
        "api_version": "2024-02-01",
        "max_tokens": 16000,
        "temperature": 0.0,
    },
    "anthropic": {
        "api_key": "",           # or set ANTHROPIC_API_KEY in env
        "model": "claude-3-5-sonnet-20241022",
        "max_tokens": 16000,
        "temperature": 0.0,
    },
}
```

4. Run all cells. The tool will analyze the profiler JSON, generate an optimization report, and optionally refine it via the configured LLM.

## Inputs
- Databricks SQL Profiler JSON produced by SQL Warehouse/DBSQL, containing plan graphs and metrics (expects keys like `graphs`).

## Outputs
The tool writes files to the working directory. Typical names:
- `output_original_query_*.sql` and `output_optimized_query_*.sql`
- `output_optimization_report_en_*.md` or `output_optimization_report_jp_*.md`
- `output_enhanced_shuffle_analysis_en_*.md` or `output_enhanced_shuffle_analysis_jp_*.md` 🔧 **New**
- Final refined report: `output_final_report_en_*.md` or `output_final_report_jp_*.md`

Debug mode controls whether intermediate artifacts are retained.

### 🔧 Enhanced Shuffle Optimization Report
The new Shuffle optimization analysis provides:
- **Memory efficiency validation**: Checks if memory per partition ≤ 512MB
- **Optimization priority assessment**: HIGH/MEDIUM/LOW based on memory usage
- **Actionable recommendations**: Specific partition counts and Spark parameters
- **Performance improvement steps**: 4-stage implementation guidance (emergency/short/medium/long-term)

## Configuration summary
- **JSON_FILE_PATH**: Path to the profiler JSON (DBFS, Workspace, or local)
- **CATALOG / DATABASE**: Used for EXPLAIN when enabled
- **OUTPUT_LANGUAGE**: `'en'` or `'ja'`
- **EXPLAIN_ENABLED**: `'Y'` to run EXPLAIN on Databricks, `'N'` to skip
- **DEBUG_ENABLED**: `'Y'` to keep intermediates, `'N'` to clean them up
- **MAX_OPTIMIZATION_ATTEMPTS**: Iterative improvement attempts
- **LLM_CONFIG**: Provider and parameters for LLM-based report refinement
- **SHUFFLE_ANALYSIS_CONFIG**: Enhanced Shuffle optimization settings 🔧 **New**
  - `memory_per_partition_threshold_mb`: Memory efficiency threshold (default: 512MB)
  - `shuffle_analysis_enabled`: Enable/disable enhanced Shuffle analysis

Environment variables you can use instead of hardcoding keys:
- `OPENAI_API_KEY`, `AZURE_OPENAI_API_KEY`, `ANTHROPIC_API_KEY`

## Troubleshooting
- If no report files are generated, ensure the main analysis cells ran to completion and the input JSON path is correct.
- Increase verbosity by setting `DEBUG_ENABLED='Y'` to keep intermediate artifacts for inspection.

## License
Add your preferred license here if applicable.
