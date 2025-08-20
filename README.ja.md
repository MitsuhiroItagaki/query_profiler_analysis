# Databricks SQL プロファイラー分析ツール

Databricks SQL Profiler の JSON ログをLLM（Databricks Model Serving / OpenAI / Azure OpenAI / Anthropic）で解析し、ボトルネックを特定して具体的な最適化手順を提案する単一ファイルのツールです。

- **メインスクリプト**: `query_profiler_analysis.py`
- **出力**: 最適化レポートと SQL ファイル（下記参照）

## 特長
- **プロファイラーログ読込**: Databricks SQL Profiler の JSON を解析（`graphs` 等の主要メトリクス）
- **メトリクス抽出**: クエリ基本情報、実行時間、データ量、キャッシュ効率、ステージ/ノードの詳細
- **ボトルネック指標**: スキュー、スピル、シャッフル、I/O ホットスポット、Photon 利用状況などを可視化
- **🔧 拡張シャッフル最適化**: メモリ効率性検証（≤512MB/パーティション）と具体的推奨事項
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

3. 🔧 **拡張シャッフル最適化設定**（新機能）:

```python
SHUFFLE_ANALYSIS_CONFIG = {
    # メモリ/パーティション閾値 (512MB)
    "memory_per_partition_threshold_mb": 512,
    "memory_per_partition_threshold_bytes": 512 * 1024 * 1024,
    
    # パフォーマンス閾値
    "high_memory_threshold_gb": 100,  # GB
    "long_execution_threshold_sec": 300,  # 5分
    
    # 最適化推奨制御
    "enable_liquid_clustering_advice": True,
    "enable_partition_tuning_advice": True,
    "enable_cluster_sizing_advice": True,
    "shuffle_analysis_enabled": True
}
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
- 🔧 **拡張シャッフル分析レポート**（新機能）: `output_enhanced_shuffle_analysis_en_*.md` または `output_enhanced_shuffle_analysis_jp_*.md`
- 最終推敲レポート: `output_final_report_en_*.md` または `output_final_report_jp_*.md`

デバッグモードにより中間成果物の保持/削除が切り替わります。

### 🔧 拡張シャッフル最適化レポート（新機能）
新しいシャッフル最適化分析では以下を提供：
- **メモリ効率性検証**: メモリ/パーティション ≤ 512MB の確認
- **最適化優先度評価**: メモリ使用量に基づくHIGH/MEDIUM/LOW判定
- **具体的推奨事項**: 特定のパーティション数とSparkパラメータ
- **パフォーマンス改善手順**: 4段階実装ガイダンス（緊急/短期/中期/長期対策）
- **独立動作**: `EXPLAIN_ENABLED = 'Y'` と `EXPLAIN_ENABLED = 'N'` の両方で動作

**注意**: 拡張シャッフル分析はJSONファイルのプロファイラーデータを直接使用し、EXPLAIN実行を必要としません。

## 設定まとめ
- **JSON_FILE_PATH**: プロファイラー JSON のパス（DBFS / Workspace / ローカル）
- **OUTPUT_FILE_DIR**: 出力ファイルディレクトリ
- **CATALOG / DATABASE**: EXPLAIN 有効時に使用
- **OUTPUT_LANGUAGE**: `'ja'` または `'en'`
- **EXPLAIN_ENABLED**: `'Y'` で Databricks 上の EXPLAIN 実行、`'N'` で無効化
- **DEBUG_ENABLED**: `'Y'` で中間ファイル保持、`'N'` で削除
- **MAX_OPTIMIZATION_ATTEMPTS**: 反復最適化の試行回数（デフォルト: 3回）
- **LLM_CONFIG**: LLM 推敲のためのプロバイダー設定
- **SHUFFLE_ANALYSIS_CONFIG**: 🔧 **拡張シャッフル最適化設定**（新機能）
  - `memory_per_partition_threshold_mb`: メモリ効率性閾値（デフォルト: 512MB）
  - `shuffle_analysis_enabled`: 拡張シャッフル分析の有効/無効

API キーは環境変数でも設定可能:
- `OPENAI_API_KEY`, `AZURE_OPENAI_API_KEY`, `ANTHROPIC_API_KEY`

## 新機能詳細

### 🧠 Thinking Mode（Claude 3.7 Sonnet専用）
- **高度な思考プロセス**: 複雑な最適化問題に対する詳細な分析
- **トークン予算制御**: 思考用に最大64Kトークンを割り当て
- **高速実行優先**: デフォルトでは無効（必要に応じて有効化）

### 🔧 拡張シャッフル最適化
- **メモリ効率性分析**: パーティションあたりのメモリ使用量を詳細分析
- **4段階最適化戦略**: 緊急/短期/中期/長期の段階的改善計画
- **Liquid Clustering推奨**: 根本的なシャッフル削減のための戦略提案

### 🔄 反復最適化
- **段階的改善**: 最大3回の最適化試行による性能向上
- **パフォーマンス検証**: 各最適化ステップでの効果測定
- **自動フォールバック**: 最適化が失敗した場合の元クエリ使用

## トラブルシューティング
- レポートが生成されない場合は、解析セルが最後まで実行されたか、入力 JSON パスが正しいか確認してください。
- `DEBUG_ENABLED='Y'` で中間成果物を保持し可視性を上げられます。
- 拡張シャッフル分析が動作しない場合は、`SHUFFLE_ANALYSIS_CONFIG["shuffle_analysis_enabled"]` が `True` に設定されているか確認してください。

## ライセンス
必要に応じてライセンス文を追加してください。
