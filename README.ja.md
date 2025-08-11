# Databricks SQL プロファイラー分析ツール

Databricks SQL Profiler の JSON ログを解析し、ボトルネックを特定して具体的な最適化手順を提案する単一ファイルのツールです。必要に応じて、LLM（Databricks Model Serving / OpenAI / Azure OpenAI / Anthropic）でレポートを推敲できます。

- **メインスクリプト**: `query_profiler_analysis.py`
- **出力**: 最適化レポートと SQL ファイル（下記参照）

## 特長
- **プロファイラーログ読込**: Databricks SQL Profiler の JSON を解析（`graphs` 等の主要メトリクス）
- **メトリクス抽出**: クエリ基本情報、実行時間、データ量、キャッシュ効率、ステージ/ノードの詳細
- **ボトルネック指標**: スキュー、スピル、シャッフル、I/O ホットスポット、Photon 利用状況などを可視化
- **LLM 推奨**: 具体的なアクションを含むレポート推敲（任意）
- **言語**: 日本語 / 英語の出力に対応
- **安全なデバッグモード**: 中間成果物の保持/削除を制御

## 必要要件
- Python 3.9+（ローカル実行時）。Databricks ではノートブックとして実行
- 任意: 一部の HTTP 通信で `requests` を利用

ローカルでの任意依存関係のインストール:

```bash
pip install requests
```

## クイックスタート

### 方法 A: Databricks 上で実行（推奨）
1. `query_profiler_analysis.py` からノートブックを作成またはインポートします。
2. 冒頭の設定セルで主要変数を設定します:

```python
# 入力 JSON（Workspace/DBFS 上のパス）
JSON_FILE_PATH = '/Workspace/Shared/AutoSQLTuning/Query1.json'

# EXPLAIN を有効化した場合のカタログ/データベース
CATALOG = 'tpcds'
DATABASE = 'tpcds_sf1000_delta_lc'

# 出力言語: 'ja' または 'en'
OUTPUT_LANGUAGE = 'ja'

# Databricks 上での EXPLAIN 実行
EXPLAIN_ENABLED = 'Y'

# 中間ファイルの保持
DEBUG_ENABLED = 'Y'

# 反復的な最適化試行回数
MAX_OPTIMIZATION_ATTEMPTS = 2
```

3. LLM プロバイダーを設定（任意: レポート推敲を行う場合）:

```python
LLM_CONFIG = {
    "provider": "databricks",  # 'databricks' | 'openai' | 'azure_openai' | 'anthropic'
    "databricks": {
        "endpoint_name": "databricks-claude-3-7-sonnet",
        "max_tokens": 131072,
        "temperature": 0.0,
    },
    "openai": {
        "api_key": "",           # 環境変数 OPENAI_API_KEY でも可
        "model": "gpt-4o",
        "max_tokens": 16000,
        "temperature": 0.0,
    },
    "azure_openai": {
        "api_key": "",           # 環境変数 AZURE_OPENAI_API_KEY でも可
        "endpoint": "",
        "deployment_name": "",
        "api_version": "2024-02-01",
        "max_tokens": 16000,
        "temperature": 0.0,
    },
    "anthropic": {
        "api_key": "",           # 環境変数 ANTHROPIC_API_KEY でも可
        "model": "claude-3-5-sonnet-20241022",
        "max_tokens": 16000,
        "temperature": 0.0,
    },
}
```

4. すべてのセルを実行します。プロファイラー JSON を解析し、最適化レポートを生成し、必要に応じて LLM で推敲します。

### 方法 B: ローカルで実行（一部機能限定）
Spark に依存しない範囲のレポート生成/推敲をローカルで実行できます。EXPLAIN を無効化し、ローカルの JSON パスを使用してください。

1. 入力として Databricks SQL Profiler の JSON をローカルに用意します。
2. `query_profiler_analysis.py` の冒頭の設定ブロックを編集します。
   - `JSON_FILE_PATH` にローカルパス（例: `/path/to/profiler.json`）を設定
   - `OUTPUT_LANGUAGE` を `'ja'` または `'en'` に設定
   - `EXPLAIN_ENABLED = 'N'` に設定（Databricks 依存の処理を回避）
   - 必要に応じて OpenAI/Azure/Anthropic の `LLM_CONFIG` を設定
3. 実行:

```bash
python3 query_profiler_analysis.py
```

注: Databricks 専用機能（クラスターに対する EXPLAIN 実行など）はローカルでは利用できません。ローカルでは `EXPLAIN_ENABLED = 'N'` を推奨します。

## 入力
- SQL Warehouse/DBSQL が出力する Databricks SQL Profiler の JSON（`graphs` 等のキーを含む）

## 出力
作業ディレクトリにファイルを出力します。代表例:
- `output_original_query_*.sql` / `output_optimized_query_*.sql`
- `output_optimization_report_en_*.md` または `output_optimization_report_jp_*.md`
- 最終推敲レポート: `output_final_report_en_*.md` または `output_final_report_jp_*.md`

デバッグモードにより中間成果物の保持/削除が切り替わります。

## 設定まとめ
- **JSON_FILE_PATH**: プロファイラー JSON のパス（DBFS / Workspace / ローカル）
- **CATALOG / DATABASE**: EXPLAIN 有効時に使用
- **OUTPUT_LANGUAGE**: `'ja'` または `'en'`
- **EXPLAIN_ENABLED**: `'Y'` で Databricks 上の EXPLAIN 実行、`'N'` で無効化
- **DEBUG_ENABLED**: `'Y'` で中間ファイル保持、`'N'` で削除
- **MAX_OPTIMIZATION_ATTEMPTS**: 反復最適化の試行回数
- **LLM_CONFIG**: LLM 推敲のためのプロバイダー設定

API キーは環境変数でも設定可能:
- `OPENAI_API_KEY`, `AZURE_OPENAI_API_KEY`, `ANTHROPIC_API_KEY`

## トラブルシューティング
- レポートが生成されない場合は、解析セルが最後まで実行されたか、入力 JSON パスが正しいか確認してください。
- ローカル実行時は `EXPLAIN_ENABLED='N'` を推奨し、Databricks 固有の処理を避けてください。
- `DEBUG_ENABLED='Y'` で中間成果物を保持し可視性を上げられます。

## ライセンス
必要に応じてライセンス文を追加してください。