# Databricks SQL Profiler Analysis Tool

A tool that uses LLM (Databricks Model Serving, OpenAI, Azure OpenAI, Anthropic) to analyze Databricks SQL Profiler JSON logs, identify bottlenecks, and provide optimization recommendations.

## Features

- **Profiler JSON Analysis**: Parse SQL Profiler output including `graphs` and metrics
- **Metrics Extraction**: Execution time, data volume, cache efficiency, node details
- **Bottleneck Detection**: Skew, spill, shuffle, I/O hotspots, Photon efficiency
- **Prioritized Recommendations**: HIGH/MEDIUM/LOW optimization suggestions
- **Iterative Optimization**: Up to 3 optimization attempts with progressive improvement
- **EXPLAIN/EXPLAIN COST Analysis**: Execution plan-based optimization verification
- **Multi-language Output**: Japanese/English report generation
- **Multi-LLM Provider**: Databricks, OpenAI, Azure OpenAI, Anthropic

## Requirements

- Python 3.9+
- Databricks Runtime (for notebook execution)
- `requests` library

## Installation

```bash
# Clone repository
git clone https://github.com/MitsuhiroItagaki/query_profiler_analysis.git
cd query_profiler_analysis

# Development setup
pip install -e ".[dev]"
```

## Project Structure

```
query_profiler_analysis/
├── notebooks/
│   └── main_full.py           # Databricks notebook (recommended)
├── src/                       # Modular source code
│   ├── config.py              # Configuration management
│   ├── models.py              # Data models
│   ├── llm/                   # LLM clients
│   ├── profiler/              # Profiler analysis
│   ├── optimization/          # Query optimization
│   ├── report/                # Report generation
│   └── utils/                 # Utilities
├── tests/                     # Test code
├── query_profiler_analysis.py # Full analysis logic
└── pyproject.toml
```

## Quick Start (Recommended)

### Running in Databricks Notebook

1. **Clone repository to Databricks Repos**
   - Repos → Add Repo → `https://github.com/MitsuhiroItagaki/query_profiler_analysis.git`

2. **Open `notebooks/main_full.py`**

3. **Edit the configuration cell**:

```python
# Required settings
JSON_FILE_PATH = '/Volumes/your_catalog/your_schema/your_volume/query-profile.json'
OUTPUT_FILE_DIR = './output'
OUTPUT_LANGUAGE = 'en'  # 'ja' or 'en'
EXPLAIN_ENABLED = 'Y'   # 'Y' = execute EXPLAIN, 'N' = skip
CATALOG = 'your_catalog'
DATABASE = 'your_database'
DEBUG_ENABLED = 'N'     # 'Y' = keep intermediate files, 'N' = final files only

# LLM configuration
LLM_CONFIG = {
    "provider": "databricks",
    "databricks": {
        "endpoint_name": "databricks-claude-opus-4-5",
        "max_tokens": 32000,
        "temperature": 0.0,
    },
}
```

4. **Run All to execute all cells**

## Configuration Options

| Setting | Description | Default |
|---------|-------------|---------|
| `JSON_FILE_PATH` | SQL Profiler JSON file path | Required |
| `OUTPUT_FILE_DIR` | Output directory | `./output` |
| `OUTPUT_LANGUAGE` | Output language (`ja`/`en`) | `en` |
| `EXPLAIN_ENABLED` | Execute EXPLAIN (`Y`/`N`) | `Y` |
| `CATALOG` | Catalog to use | Required (for EXPLAIN) |
| `DATABASE` | Database to use | Required (for EXPLAIN) |
| `DEBUG_ENABLED` | Debug mode (`Y`/`N`) | `N` |
| `MAX_OPTIMIZATION_ATTEMPTS` | Number of optimization attempts | `3` |

## LLM Provider Configuration

### Databricks Model Serving (Recommended)

```python
LLM_CONFIG = {
    "provider": "databricks",
    "databricks": {
        "endpoint_name": "databricks-claude-opus-4-5",
        "max_tokens": 32000,
        "temperature": 0.0,
        "thinking_enabled": False,
    },
}
```

### OpenAI

```python
LLM_CONFIG = {
    "provider": "openai",
    "openai": {
        "api_key": "",  # or use OPENAI_API_KEY environment variable
        "model": "gpt-4o",
        "max_tokens": 16000,
        "temperature": 0.0,
    },
}
```

### Anthropic

```python
LLM_CONFIG = {
    "provider": "anthropic",
    "anthropic": {
        "api_key": "",  # or use ANTHROPIC_API_KEY environment variable
        "model": "claude-3-5-sonnet-20241022",
        "max_tokens": 16000,
        "temperature": 0.0,
    },
}
```

## Output Files

### Final Outputs (when DEBUG_ENABLED='N')

| File | Description |
|------|-------------|
| `output_original_query_*.sql` | Original query |
| `output_optimized_query_*.sql` | Optimized query |
| `output_optimization_report_*.md` | Optimization report |
| `output_final_report_*.md` | LLM-refined final report |

### Debug Files (when DEBUG_ENABLED='Y' only)

- `output_explain_*.txt` - EXPLAIN results
- `output_explain_cost_*.txt` - EXPLAIN COST results
- `output_performance_judgment_log_*.txt` - Performance judgment logs
- Other intermediate files

## Troubleshooting

### EXPLAIN execution fails

1. Verify `CATALOG` and `DATABASE` are correctly configured
2. Check access permissions for tables used in the query
3. Ensure table names use full path (`catalog.schema.table`)

```python
# Test EXPLAIN manually
spark.sql("USE CATALOG your_catalog")
spark.sql("USE DATABASE your_database")
spark.sql("EXPLAIN SELECT * FROM your_table LIMIT 1").show(truncate=False)
```

### Optimized SQL missing SELECT clause

When LLM generates incomplete SQL, it automatically falls back to the original query.
The log will show: "Warning: Generated SQL is missing SELECT clause"

### Intermediate files not being deleted

Ensure `DEBUG_ENABLED = 'N'` is set.
The cleanup cell at the end of the notebook will automatically delete intermediate files.

## Development

```bash
# Run tests
pytest

# Single test
pytest tests/test_profiler.py -v

# Lint
ruff check src/ tests/

# Type check
mypy src/
```

## Change Log

### v2.1.0 - main_full.py Integration
- Added `notebooks/main_full.py` as recommended entry point
- Load full analysis logic via `%run`
- `SKIP_AUTO_CLEANUP` flag for proper cleanup timing control
- Fallback feature for incomplete SQL generation
- Automatic intermediate file deletion when `DEBUG_ENABLED='N'`

### v2.0.0 - Modular Refactoring
- Split 20,000-line single file into 20+ modules
- Type-safe configuration with dataclasses
- Strategy Pattern for LLM client abstraction
- Added pytest unit tests

### v1.x - Initial Release
- Single file implementation
- Basic bottleneck analysis and optimization

## License

MIT
