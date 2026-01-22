# Databricks SQL プロファイラー分析ツール

LLM（Databricks Model Serving、OpenAI、Azure OpenAI、Anthropic）を使用してDatabricks SQL ProfilerのJSONログを分析し、ボトルネックを特定して最適化提案を行うツール。

## 特長

- **プロファイラーJSON解析**: SQL Profiler出力の`graphs`とメトリクスを解析
- **メトリクス抽出**: 実行時間、データ量、キャッシュ効率、ノード詳細
- **ボトルネック検出**: スキュー、スピル、シャッフル、I/Oホットスポット、Photon効率
- **優先度付き推奨**: HIGH/MEDIUM/LOWの最適化提案
- **反復最適化**: 最大3回の段階的な最適化試行
- **EXPLAIN/EXPLAIN COST分析**: 実行プランに基づく最適化検証
- **多言語出力**: 日本語/英語レポート生成
- **マルチLLMプロバイダー**: Databricks、OpenAI、Azure OpenAI、Anthropic

## 必要要件

- Python 3.9以上
- Databricks Runtime（ノートブック実行時）
- `requests` ライブラリ

## インストール

```bash
# リポジトリをクローン
git clone https://github.com/MitsuhiroItagaki/query_profiler_analysis.git
cd query_profiler_analysis

# 開発環境セットアップ
pip install -e ".[dev]"
```

## プロジェクト構成

```
query_profiler_analysis/
├── notebooks/
│   └── main_full.py           # Databricksノートブック（推奨）
├── src/                       # モジュール化されたソースコード
│   ├── config.py              # 設定管理
│   ├── models.py              # データモデル
│   ├── llm/                   # LLMクライアント
│   ├── profiler/              # プロファイラ分析
│   ├── optimization/          # クエリ最適化
│   ├── report/                # レポート生成
│   └── utils/                 # ユーティリティ
├── tests/                     # テストコード
├── query_profiler_analysis.py # 完全版分析ロジック
└── pyproject.toml
```

## クイックスタート（推奨）

### Databricksノートブックでの実行

1. **リポジトリをDatabricks Reposにクローン**
   - Repos → Add Repo → `https://github.com/MitsuhiroItagaki/query_profiler_analysis.git`

2. **`notebooks/main_full.py` を開く**

3. **設定セルを編集**:

```python
# 必須設定
JSON_FILE_PATH = '/Volumes/your_catalog/your_schema/your_volume/query-profile.json'
OUTPUT_FILE_DIR = './output'
OUTPUT_LANGUAGE = 'ja'  # 'ja' or 'en'
EXPLAIN_ENABLED = 'Y'   # 'Y' = EXPLAIN実行, 'N' = スキップ
CATALOG = 'your_catalog'
DATABASE = 'your_database'
DEBUG_ENABLED = 'N'     # 'Y' = 中間ファイル保持, 'N' = 最終ファイルのみ

# LLM設定
LLM_CONFIG = {
    "provider": "databricks",
    "databricks": {
        "endpoint_name": "databricks-claude-opus-4-5",
        "max_tokens": 32000,
        "temperature": 0.0,
    },
}
```

4. **Run All で全セルを実行**

## 設定オプション

| 設定 | 説明 | デフォルト |
|------|------|-----------|
| `JSON_FILE_PATH` | SQL ProfilerのJSONファイルパス | 必須 |
| `OUTPUT_FILE_DIR` | 出力ディレクトリ | `./output` |
| `OUTPUT_LANGUAGE` | 出力言語 (`ja`/`en`) | `en` |
| `EXPLAIN_ENABLED` | EXPLAIN実行 (`Y`/`N`) | `Y` |
| `CATALOG` | 使用するカタログ | 必須（EXPLAIN時） |
| `DATABASE` | 使用するデータベース | 必須（EXPLAIN時） |
| `DEBUG_ENABLED` | デバッグモード (`Y`/`N`) | `N` |
| `MAX_OPTIMIZATION_ATTEMPTS` | 最適化試行回数 | `3` |

## LLMプロバイダー設定

### Databricks Model Serving（推奨）

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
        "api_key": "",  # または環境変数 OPENAI_API_KEY
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
        "api_key": "",  # または環境変数 ANTHROPIC_API_KEY
        "model": "claude-3-5-sonnet-20241022",
        "max_tokens": 16000,
        "temperature": 0.0,
    },
}
```

## 出力ファイル

### 最終成果物（DEBUG_ENABLED='N' 時）

| ファイル | 説明 |
|---------|------|
| `output_original_query_*.sql` | 元のクエリ |
| `output_optimized_query_*.sql` | 最適化されたクエリ |
| `output_optimization_report_*.md` | 最適化レポート |
| `output_final_report_*.md` | LLMリファイン済み最終レポート |

### デバッグファイル（DEBUG_ENABLED='Y' 時のみ）

- `output_explain_*.txt` - EXPLAIN結果
- `output_explain_cost_*.txt` - EXPLAIN COST結果
- `output_performance_judgment_log_*.txt` - パフォーマンス判定ログ
- その他中間ファイル

## トラブルシューティング

### EXPLAIN実行が失敗する場合

1. `CATALOG` と `DATABASE` が正しく設定されているか確認
2. クエリで使用するテーブルへのアクセス権限があるか確認
3. テーブル名がフルパス（`catalog.schema.table`）で指定されているか確認

```python
# 手動でEXPLAINをテスト
spark.sql("USE CATALOG your_catalog")
spark.sql("USE DATABASE your_database")
spark.sql("EXPLAIN SELECT * FROM your_table LIMIT 1").show(truncate=False)
```

### 最適化されたSQLにSELECT句がない場合

LLMが不完全なSQLを生成した場合、自動的に元のクエリにフォールバックします。
ログに「🚨 警告: 生成されたSQLにSELECT句がありません」と表示されます。

### 中間ファイルが削除されない場合

`DEBUG_ENABLED = 'N'` に設定されていることを確認してください。
ノートブックの最後にクリーンアップセルが実行され、中間ファイルが自動削除されます。

## 開発

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

## 変更履歴

### v2.1.0 - main_full.py 統合版
- `notebooks/main_full.py` を推奨エントリポイントとして追加
- `%run` で完全版分析ロジックを読み込み
- `SKIP_AUTO_CLEANUP` フラグによる適切なクリーンアップタイミング制御
- 不完全なSQL生成時のフォールバック機能追加
- `DEBUG_ENABLED='N'` 時の中間ファイル自動削除

### v2.0.0 - モジュール化リファクタリング
- 20,000行の単一ファイルを20+モジュールに分割
- dataclassによる型安全な設定管理
- Strategy PatternによるLLMクライアント抽象化
- pytestによるユニットテスト追加

### v1.x - 初期リリース
- 単一ファイル実装
- 基本的なボトルネック分析と最適化

## ライセンス

MIT
