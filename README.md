# Databricks SQL Profiler Analysis Tool

LLM（Databricks Model Serving、OpenAI、Azure OpenAI、Anthropic）を使用してDatabricks SQL ProfilerのJSONログを分析し、ボトルネックを特定して最適化提案を行うツール。

## Features

- **Profiler JSON解析**: SQL Profiler出力の`graphs`とメトリクスを解析
- **メトリクス抽出**: 実行時間、データ量、キャッシュ効率、ノード詳細
- **ボトルネック検出**: スキュー、スピル、シャッフル、I/Oホットスポット、Photon効率
- **優先度付き推奨**: HIGH/MEDIUM/LOWの最適化提案
- **反復最適化**: 最大3回の段階的な最適化試行
- **🧠 Thinking Mode**: Claude Opus 4.5の拡張思考モード対応
- **多言語出力**: 日本語/英語レポート生成
- **マルチLLMプロバイダー**: Databricks、OpenAI、Azure OpenAI、Anthropic

## Requirements

- Python 3.9以上
- Databricks Runtime（ノートブック実行時）
- `requests` ライブラリ

## Installation

```bash
# リポジトリをクローン
git clone https://github.com/MitsuhiroItagaki/query_profiler_analysis.git
cd query_profiler_analysis

# 開発環境セットアップ
pip install -e ".[dev]"
```

## Project Structure

```
query_profiler_analysis/
├── src/                       # モジュール化されたソースコード (v2.0)
│   ├── config.py              # 設定管理
│   ├── models.py              # データモデル
│   ├── llm/                   # LLMクライアント
│   ├── profiler/              # プロファイラ分析
│   ├── optimization/          # クエリ最適化
│   ├── report/                # レポート生成
│   └── utils/                 # ユーティリティ
├── notebooks/
│   └── main.py                # Databricksノートブック (v2.0)
├── tests/                     # テストコード
├── query_profiler_analysis.py # 旧版単一ファイル (v1.x)
└── pyproject.toml
```

## Quick Start

### 方法1: モジュール版 (v2.0 - 推奨)

```python
from src.config import AnalysisConfig, LLMConfig, DatabricksLLMConfig, set_config
from src.profiler import load_profiler_json, extract_metrics, analyze_bottlenecks
from src.optimization import execute_iterative_optimization
from src.report import generate_comprehensive_report

# 設定
config = AnalysisConfig(
    json_file_path='/Workspace/Shared/profiler.json',
    output_file_dir='./output',
    output_language='ja',
    llm=LLMConfig(
        provider='databricks',
        databricks=DatabricksLLMConfig(
            endpoint_name='databricks-claude-3-7-sonnet',
        ),
    ),
)
set_config(config)

# 分析実行
data = load_profiler_json(config.json_file_path)
metrics = extract_metrics(data)
bottlenecks = analyze_bottlenecks(metrics)

# 最適化
result = execute_iterative_optimization(original_query, metrics)

# レポート生成
report = generate_comprehensive_report(metrics, bottlenecks, result)
```

### 方法2: 旧版単一ファイル (v1.x)

1. `query_profiler_analysis.py` をDatabricksにインポート
2. 設定セルで変数を設定:

```python
JSON_FILE_PATH = '/Workspace/Shared/AutoSQLTuning/Query1.json'
OUTPUT_FILE_DIR = './output'
OUTPUT_LANGUAGE = 'ja'  # 'ja' or 'en'
EXPLAIN_ENABLED = 'Y'
CATALOG = 'your_catalog'
DATABASE = 'your_database'
```

3. LLMプロバイダーを設定:

```python
LLM_CONFIG = {
    "provider": "databricks",
    "databricks": {
        "endpoint_name": "databricks-claude-opus-4-5",
        "max_tokens": 32000,
        "temperature": 0.0,
    },
}
```

4. 全セルを実行

## LLM Provider Configuration

### Databricks Model Serving

```python
LLMConfig(
    provider='databricks',
    databricks=DatabricksLLMConfig(
        endpoint_name='databricks-claude-opus-4-5',
        max_tokens=32000,
        thinking_enabled=False,
    ),
)
```

### OpenAI

```python
LLMConfig(
    provider='openai',
    openai=OpenAIConfig(
        api_key='',  # or OPENAI_API_KEY env var
        model='gpt-4o',
    ),
)
```

### Anthropic

```python
LLMConfig(
    provider='anthropic',
    anthropic=AnthropicConfig(
        api_key='',  # or ANTHROPIC_API_KEY env var
        model='claude-3-5-sonnet-20241022',
    ),
)
```

## Outputs

- `original_query_*.sql` - 元のクエリ
- `optimized_query_*.sql` - 最適化クエリ
- `optimization_report_*.md` - 最適化レポート

## Development

```bash
# テスト実行
pytest

# 単一テスト
pytest tests/test_profiler.py -v

# Lint
ruff check src/ tests/

# 型チェック
mypy src/
```

## Change Log

### v2.0.0 - モジュール化リファクタリング
- 20,000行の単一ファイルを20+モジュールに分割
- dataclassによる型安全な設定管理
- Strategy PatternによるLLMクライアント抽象化
- pytestによるユニットテスト追加
- 旧版 `query_profiler_analysis.py` は互換性のため維持

### v1.x - 初期リリース
- 単一ファイル実装
- 基本的なボトルネック分析と最適化

## License

MIT
