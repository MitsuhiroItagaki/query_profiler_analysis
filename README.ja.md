# Databricks SQL プロファイラー分析ツール

Databricks SQL Profiler の JSON ログをLLM（Databricks Model Serving / OpenAI / Azure OpenAI / Anthropic）で解析し、ボトルネックを特定して具体的な最適化手順を提案する単一ファイルのツールです。

- **メインスクリプト**: `query_profiler_analysis.py`
- **出力**: 最適化レポートと SQL ファイル（下記参照）

## 特長
- **プロファイラーログ読込**: Databricks SQL Profiler の JSON を解析（`graphs` 等の主要メトリクス）
- **メトリクス抽出**: クエリ基本情報、実行時間、データ量、キャッシュ効率、ステージ/ノードの詳細
- **ボトルネック指標**: スキュー、スピル、シャッフル、I/O ホットスポット、Photon 利用状況などを可視化
- **優先度付き推奨事項**: HIGH/MEDIUM/LOW の最適化優先度と具体的パラメータ提案
- **Spark チューニング ガイダンス**: 最適なパフォーマンスのための Spark パラメータ自動推奨
- **反復最適化**: 最大3回の最適化試行による段階的改善
- **🧠 Thinking Mode**: Databricks Claude 3.7 Sonnet用の高度な思考モード対応
- **LLM 推奨**: 具体的なアクションを含むレポート推敲（任意）
- **言語**: 日本語 / 英語の出力に対応
- **安全なデバッグモード**: 中間成果物の保持/削除を制御
- **Liquid Clustering分析**: Delta Lake最適化のための詳細分析

## 必要要件
- Databricks Runtime（ノートブックとして実行）
- 任意: 一部の HTTP 通信で `requests` を利用

## クイックスタート

### Databricks 上で実行
1. `query_profiler_analysis.py` からノートブックを作成またはインポートします。
2. Query Profilerからメトリクスをダウンロードしてください（JSONファイルで出力されます）
3. 冒頭の設定セルで主要変数を設定します:

```python
# 入力 JSON（Workspace/DBFS 上のパス）
JSON_FILE_PATH = '/Workspace/Shared/AutoSQLTuning/Query1.json'

# 出力ディレクトリ
OUTPUT_FILE_DIR = './output'

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
MAX_OPTIMIZATION_ATTEMPTS = 3
```

4. LLM プロバイダーを設定（必須）:

```python
LLM_CONFIG = {
    "provider": "databricks",  # 'databricks' | 'openai' | 'azure_openai' | 'anthropic'
    "databricks": {
        "endpoint_name": "databricks-claude-3-7-sonnet",
        "max_tokens": 131072,  # 128K tokens (Claude 3.7 Sonnet 最大制限)
        "temperature": 0.0,
        # 🧠 拡張思考モード（新機能）
        # "thinking_enabled": False,  # 高速実行優先のためデフォルト無効
        # "thinking_budget_tokens": 65536  # 思考トークン予算 64K
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

5. すべてのセルを実行します。プロファイラー JSON を解析し、最適化レポートを生成し、必要に応じて LLM で推敲します。

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
- **OUTPUT_FILE_DIR**: 出力ファイルディレクトリ
- **CATALOG / DATABASE**: EXPLAIN 有効時に使用
- **OUTPUT_LANGUAGE**: `'ja'` または `'en'`
- **EXPLAIN_ENABLED**: `'Y'` で Databricks 上の EXPLAIN 実行、`'N'` で無効化
- **DEBUG_ENABLED**: `'Y'` で中間ファイル保持、`'N'` で削除
- **MAX_OPTIMIZATION_ATTEMPTS**: 反復最適化の試行回数（デフォルト: 3回）
- **LLM_CONFIG**: LLM 推敲のためのプロバイダー設定

API キーは環境変数でも設定可能:
- `OPENAI_API_KEY`, `AZURE_OPENAI_API_KEY`, `ANTHROPIC_API_KEY`

## 新機能詳細

### 🧠 Thinking Mode（Claude 3.7 Sonnet専用）
- **高度な思考プロセス**: 複雑な最適化問題に対する詳細な分析
- **トークン予算制御**: 思考用に最大64Kトークンを割り当て
- **高速実行優先**: デフォルトでは無効（必要に応じて有効化）

### 🔄 反復最適化
- **段階的改善**: 最大3回の最適化試行による性能向上
- **パフォーマンス検証**: 各最適化ステップでの効果測定
- **自動フォールバック**: 最適化が失敗した場合の元クエリ使用

## トラブルシューティング
- レポートが生成されない場合は、解析セルが最後まで実行されたか、入力 JSON パスが正しいか確認してください。
- `DEBUG_ENABLED='Y'` で中間成果物を保持し可視性を上げられます。

## 変更履歴

### 2024-12-11: EXPLAIN要約ファイル管理の修正
**問題**: `EXPLAIN_ENABLED='Y'` で実行してもレポートに "No EXPLAIN summary files were found" エラーが表示される

**根本原因**:
1. EXPLAIN結果のサイズが200KB未満の場合、要約ファイル（`.md`）が作成されない
2. 要約ファイルの保存条件が `DEBUG_ENABLED='Y'` になっており、通常使用時（`DEBUG_ENABLED='N'`）にファイルが作成されない
3. 最終レポート生成後も中間ファイル（`output_explain_summary_*.md`）が削除されずに残る

**修正内容**:
- **修正1**: サイズが閾値以下（200KB未満）でも、`EXPLAIN_ENABLED='Y'` の場合は要約ファイルを保存するように変更
- **修正2**: 要約ファイルの保存条件を `DEBUG_ENABLED='Y'` から `EXPLAIN_ENABLED='Y'` に変更
- **修正3**: `DEBUG_ENABLED='N'` の場合、最終レポート生成後に中間ファイル（`output_explain_summary_*.md`）を自動削除

**影響範囲**:
- `summarize_explain_results_with_llm` 関数（行10972-11257付近）
- `finalize_report_files` 関数（行19463-19505付近）

**結果**: 通常の使用パターン（`EXPLAIN_ENABLED='Y'`, `DEBUG_ENABLED='N'`）で、EXPLAIN要約が正常にレポートに統合され、不要な中間ファイルが自動削除されるようになりました。

## ライセンス
必要に応じてライセンス文を追加してください。
