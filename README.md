# Databricks SQL Profiler Analysis Tool

A single-file tool to analyze Databricks SQL Profiler JSON logs with an LLM (Databricks Model Serving, OpenAI, Azure OpenAI, or Anthropic), identify bottlenecks, and propose concrete optimization steps.

- **Main script**: `query_profiler_analysis.py`
- **Outputs**: optimization reports and SQL files (see Outputs below)

## Features
- **Profiler log ingestion**: Reads Databricks SQL Profiler JSON (parses `graphs` and key metrics)
- **Metrics extraction**: Query basics, execution time, data volume, cache efficiency, stage/node details
- **Bottleneck indicators**: Highlights skew, spill, shuffle, I/O hotspots, Photon utilization and more
- **Priority-based recommendations**: HIGH/MEDIUM/LOW optimization priorities with specific parameter suggestions
- **Spark tuning guidance**: Automated Spark parameter recommendations for optimal performance
- **Iterative optimization**: Up to 3 optimization attempts for progressive improvement
- **🧠 Thinking Mode**: Advanced reasoning mode support for Databricks Claude 3.7 Sonnet
- **LLM-assisted analysis**: Optional refinement of the report with specific, actionable recommendations
- **Language support**: Output in English or Japanese
- **Safe debug mode**: Keep or clean up intermediate artifacts
- **Liquid Clustering analysis**: Detailed analysis for Delta Lake optimization

## Requirements
- Databricks Runtime (run as a notebook)
- Optional: `requests` for some HTTP interactions

## Quick start

### Run on Databricks
1. Import or create a notebook from `query_profiler_analysis.py`.
2. Download metrics from Query Profiler (output as JSON file).
3. Open the configuration cell near the top of the file and set key variables:

```python
# File path in your workspace/DBFS
JSON_FILE_PATH = '/Workspace/Shared/AutoSQLTuning/Query1.json'

# Output directory
OUTPUT_FILE_DIR = './output'

# Catalog/Database for EXPLAIN (when enabled)
CATALOG = 'tpcds'
DATABASE = 'tpcds_sf1000_delta_lc'

# Output language: 'ja' or 'en'
OUTPUT_LANGUAGE = 'en'

# Enable/disable EXPLAIN execution on Databricks
EXPLAIN_ENABLED = 'Y'

# Preserve intermediate files if 'Y'
DEBUG_ENABLED = 'Y'

# Max number of iterative optimization attempts
MAX_OPTIMIZATION_ATTEMPTS = 3
```

4. Configure LLM provider (required):

```python
LLM_CONFIG = {
    "provider": "databricks",  # 'databricks' | 'openai' | 'azure_openai' | 'anthropic'
    "databricks": {
        "endpoint_name": "databricks-claude-3-7-sonnet",
        "max_tokens": 131072,  # 128K tokens (Claude 3.7 Sonnet maximum limit)
        "temperature": 0.0,
        # 🧠 Extended thinking mode (New Feature)
        # "thinking_enabled": False,  # Default disabled for high-speed execution priority
        # "thinking_budget_tokens": 65536  # Thinking token budget 64K tokens
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

5. Run all cells. The tool will analyze the profiler JSON, generate an optimization report, and optionally refine it via the configured LLM.

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
- **OUTPUT_FILE_DIR**: Output file directory
- **CATALOG / DATABASE**: Used for EXPLAIN when enabled
- **OUTPUT_LANGUAGE**: `'en'` or `'ja'`
- **EXPLAIN_ENABLED**: `'Y'` to run EXPLAIN on Databricks, `'N'` to skip
- **DEBUG_ENABLED**: `'Y'` to keep intermediates, `'N'` to clean them up
- **MAX_OPTIMIZATION_ATTEMPTS**: Iterative improvement attempts (default: 3)
- **LLM_CONFIG**: Provider and parameters for LLM-based report refinement

Environment variables you can use instead of hardcoding keys:
- `OPENAI_API_KEY`, `AZURE_OPENAI_API_KEY`, `ANTHROPIC_API_KEY`

## New Features Details

### 🧠 Thinking Mode (Claude 3.7 Sonnet Only)
- **Advanced reasoning process**: Detailed analysis for complex optimization problems
- **Token budget control**: Allocate up to 64K tokens for thinking process
- **High-speed execution priority**: Disabled by default (enable when needed)

### 🔄 Iterative Optimization
- **Progressive improvement**: Up to 3 optimization attempts for performance enhancement
- **Performance validation**: Effect measurement at each optimization step
- **Automatic fallback**: Use original query when optimization fails

## Troubleshooting
- If no report files are generated, ensure the main analysis cells ran to completion and the input JSON path is correct.
- Increase verbosity by setting `DEBUG_ENABLED='Y'` to keep intermediate artifacts for inspection.

## License
Add your preferred license here if applicable.
