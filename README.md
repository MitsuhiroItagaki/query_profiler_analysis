# Databricks SQL Profiler Analysis Tool

A single-file tool to analyze Databricks SQL Profiler JSON logs, identify bottlenecks, and propose concrete optimization steps. It can optionally refine the generated report using an LLM (Databricks Model Serving, OpenAI, Azure OpenAI, or Anthropic).

- **Main script**: `query_profiler_analysis.py`
- **Outputs**: optimization reports and SQL files (see Outputs below)

## Features
- **Profiler log ingestion**: Reads Databricks SQL Profiler JSON (parses `graphs` and key metrics)
- **Metrics extraction**: Query basics, execution time, data volume, cache efficiency, stage/node details
- **Bottleneck indicators**: Highlights skew, spill, shuffle, I/O hotspots, Photon utilization and more
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
```

3. Configure LLM provider (optional, only if you want AI-based report refinement):

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
- Final refined report: `output_final_report_en_*.md` or `output_final_report_jp_*.md`

Debug mode controls whether intermediate artifacts are retained.

## Configuration summary
- **JSON_FILE_PATH**: Path to the profiler JSON (DBFS, Workspace, or local)
- **CATALOG / DATABASE**: Used for EXPLAIN when enabled
- **OUTPUT_LANGUAGE**: `'en'` or `'ja'`
- **EXPLAIN_ENABLED**: `'Y'` to run EXPLAIN on Databricks, `'N'` to skip
- **DEBUG_ENABLED**: `'Y'` to keep intermediates, `'N'` to clean them up
- **MAX_OPTIMIZATION_ATTEMPTS**: Iterative improvement attempts
- **LLM_CONFIG**: Provider and parameters for LLM-based report refinement

Environment variables you can use instead of hardcoding keys:
- `OPENAI_API_KEY`, `AZURE_OPENAI_API_KEY`, `ANTHROPIC_API_KEY`

## Troubleshooting
- If no report files are generated, ensure the main analysis cells ran to completion and the input JSON path is correct.
- Increase verbosity by setting `DEBUG_ENABLED='Y'` to keep intermediate artifacts for inspection.

## License
Add your preferred license here if applicable.
