# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Databricks SQL Profiler Analysis Tool - LLMを使用してSQL Profiler JSONログを分析し、ボトルネック特定と最適化クエリを生成するツール。

## Commands

```bash
# 開発環境セットアップ
pip install -e ".[dev]"

# テスト実行
pytest

# 単一テスト実行
pytest tests/test_profiler.py::TestMetricsExtraction::test_extract_metrics_from_summary -v

# Lint
ruff check src/ tests/

# フォーマット
ruff format src/ tests/

# 型チェック
mypy src/
```

## Architecture

```
src/
├── config.py           # 設定管理（dataclass）- AnalysisConfig, LLMConfig
├── models.py           # データモデル定義 - ExtractedMetrics, OptimizationResult等
├── llm/                # LLMクライアント（Strategy Pattern）
│   ├── base.py         # 抽象基底クラス LLMClient
│   ├── databricks.py   # DatabricksLLMClient
│   ├── openai.py       # OpenAILLMClient
│   ├── anthropic.py    # AnthropicLLMClient
│   ├── azure_openai.py # AzureOpenAILLMClient
│   └── factory.py      # create_llm_client(), call_llm()
├── profiler/           # プロファイラデータ分析
│   ├── loader.py       # JSON読み込み、フォーマット検出
│   ├── metrics.py      # メトリクス抽出
│   └── bottleneck.py   # ボトルネック分析
├── optimization/       # クエリ最適化
│   ├── query_generator.py  # LLMによる最適化クエリ生成
│   ├── performance.py      # パフォーマンス比較
│   └── iterative.py        # 反復最適化ロジック
├── report/             # レポート生成
│   └── generator.py    # Markdownレポート生成、ファイル保存
└── utils/              # ユーティリティ
    ├── sql.py          # SQL操作（抽出、検証、クリーニング）
    └── io.py           # ファイル入出力

notebooks/
└── main.py             # Databricksノートブック形式のエントリポイント

tests/
├── conftest.py         # pytest fixtures
├── test_profiler.py    # profilerモジュールのテスト
└── test_utils.py       # utilsモジュールのテスト
```

## Key Patterns

### 設定管理
```python
from src.config import AnalysisConfig, set_config, get_config

config = AnalysisConfig(
    json_file_path='/path/to/profiler.json',
    output_language='ja',
    llm=LLMConfig(provider='databricks'),
)
set_config(config)
```

### LLM呼び出し
```python
from src.llm import call_llm, create_llm_client

# シンプルな呼び出し
response = call_llm("Analyze this query...")

# カスタムクライアント
client = create_llm_client(custom_config)
response = client.call_with_retry(prompt)
```

### メトリクス抽出とボトルネック分析
```python
from src.profiler import load_profiler_json, extract_metrics, analyze_bottlenecks

data = load_profiler_json(file_path)
metrics = extract_metrics(data)
bottlenecks = analyze_bottlenecks(metrics)
```

## Data Formats

ツールは2種類のJSONフォーマットをサポート:
1. **sql_query_summary**: `query.metrics` にメトリクスを含む形式
2. **sql_profiler**: `graphs[].nodes[]` に詳細実行プランを含む形式

フォーマットは `detect_data_format()` で自動判別。

## Databricks Integration

- ノートブックは `# COMMAND ----------` でセル区切り
- `dbutils` と `spark` はDatabricks環境で自動提供
- Unity Catalogテーブル: `catalog.schema.table`
- Volumeパス: `/Volumes/catalog/schema/volume/path`

## Legacy Code

`query_profiler_analysis.py` は旧バージョンの単一ファイル実装（約20,000行）。
新規開発は `src/` モジュール構造を使用。
