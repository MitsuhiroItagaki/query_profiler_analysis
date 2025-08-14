# Enhanced query optimization with shuffle analysis
ENHANCED_QUERY_OPTIMIZATION_PROMPT = """
あなたは経験豊富なSpark SQLエキスパートです。
以下のクエリとその実行メトリクスを分析し、Enhanced Shuffle最適化を含む包括的な最適化を行ってください。

元のクエリ: {original_query}
実行メトリクス: {execution_metrics}
クエリプラン: {query_plan}

分析・最適化項目:

1. Enhanced Shuffle操作最適化分析:
   - JOIN/GROUP BY操作でのshuffle最適化機会を特定
   - 必要に応じてREPARTITION/COALESCE提案
   - スキューデータ対策のREPARTITION BY RANGE提案

2. クエリ構造最適化:
   - 実行順序の最適化
   - 不要な操作の除去
   - プレディケートプッシュダウンの改善

3. JOIN戦略最適化:
   - ブロードキャストJOINの適用
   - JOIN順序の最適化
   - JOIN条件の改善

4. 集約操作最適化:
   - GROUP BY最適化
   - ウィンドウ関数の改善
   - DISTINCT操作の最適化

5. パーティション戦略最適化:
   - データ局所性の改善
   - パーティション分散の均等化
   - 動的パーティション調整

6. キャッシュ戦略:
   - 中間結果のキャッシュ
   - 再利用可能データの特定

**重要な指示: 最適化クエリ生成について**

shuffle_optimization_needed が "YES" の場合、必ず以下の手順でREPARTITIONヒントを追加してください：

1. repartition_opportunities で特定された各操作に対して適切なREPARTITION文を追加
2. REPARTITION戦略に基づいて以下の形式を使用：
   - REPARTITION(パーティション数): 均等分散の場合
   - REPARTITION(パーティション数, カラム名): 特定カラムでの分散の場合  
   - COALESCE(パーティション数): パーティション数削減の場合
   - REPARTITION BY RANGE(カラム名): スキューデータ対策の場合

3. REPARTITIONは対象テーブル/サブクエリの直後、JOINやGROUP BY操作の前に配置

**例:**
```sql
-- 元のクエリ
SELECT a.id, b.value, COUNT(*)
FROM table_a a
JOIN table_b b ON a.id = b.id
GROUP BY a.id, b.value

-- REPARTITION最適化後
WITH repartitioned_a AS (
  SELECT /*+ REPARTITION(200, id) */ * FROM table_a
),
repartitioned_b AS (
  SELECT /*+ REPARTITION(200, id) */ * FROM table_b  
)
SELECT a.id, b.value, COUNT(*)
FROM repartitioned_a a
JOIN repartitioned_b b ON a.id = b.id
GROUP BY a.id, b.value
```

最適化結果をJSON形式で出力してください:
{{
    "original_query_analysis": {{
        "execution_time_ms": 実行時間,
        "shuffle_operations_count": shuffle操作数,
        "join_operations_count": JOIN操作数,
        "complexity_score": 複雑度スコア
    }},
    "shuffle_optimization_analysis": {{
        "shuffle_optimization_needed": "YES/NO",
        "shuffle_optimization_details": "詳細説明",
        "repartition_opportunities": [
            {{
                "operation": "対象操作",
                "current_partitions": 現在のパーティション数,
                "recommended_partitions": 推奨パーティション数,
                "repartition_strategy": "REPARTITION/COALESCE/REPARTITION BY RANGE",
                "column": "パーティションキー",
                "expected_improvement": "期待される改善"
            }}
        ]
    }},
    "optimized_queries": {{
        "optimized_query_with_repartition_hints": "shuffle_optimization_neededがYESの場合、上記の指示に従ってREPARTITION最適化を適用したクエリ。NOの場合は元のクエリ",
        "optimized_query_with_broadcast_hints": "ブロードキャストヒント付きクエリ", 
        "optimized_query_comprehensive": "包括的最適化クエリ"
    }},
    "join_optimization": {{
        "broadcast_candidates": [
            {{
                "table": "テーブル名",
                "size_estimate_mb": 推定サイズ,
                "broadcast_eligible": true/false
            }}
        ],
        "join_reordering_needed": true/false,
        "optimized_join_strategy": "推奨JOIN戦略"
    }},
    "aggregation_optimization": {{
        "groupby_optimization_needed": true/false,
        "aggregation_pushdown_opportunities": ["最適化機会"],
        "window_function_optimization": "ウィンドウ関数最適化"
    }},
    "caching_strategy": {{
        "cache_recommendations": [
            {{
                "target": "キャッシュ対象",
                "reason": "キャッシュ理由",
                "expected_benefit": "期待される効果"
            }}
        ]
    }},
    "performance_prediction": {{
        "expected_execution_time_reduction": "実行時間短縮予測",
        "expected_shuffle_reduction": "shuffle削減予測", 
        "overall_improvement_score": "総合改善スコア"
    }},
    "implementation_guidance": {{
        "step_by_step_implementation": [
            "実装手順1",
            "実装手順2"
        ],
        "risk_assessment": "リスク評価",
        "rollback_plan": "ロールバック計画"
    }}
}}
"""

# Advanced comprehensive optimization
ADVANCED_COMPREHENSIVE_OPTIMIZATION_PROMPT = """
あなたは経験豊富なSpark SQLエキスパートです。
以下のクエリとその実行環境を総合的に分析し、最高レベルの最適化を提案してください。

元のクエリ: {original_query}
実行メトリクス: {execution_metrics}
クエリプラン: {query_plan}
クラスタ情報: {cluster_info}
データ特性: {data_characteristics}

高度な分析・最適化項目:

1. アーキテクチャレベル最適化:
   - データレイアウト最適化 (Delta/Parquet)
   - パーティショニング戦略
   - Z-Ordering/Data Skipping

2. Advanced Join最適化:
   - Bucket Join活用
   - Sort-Merge Join vs Hash Join選択
   - Dynamic Partition Pruning活用

3. Enhanced Shuffle操作最適化分析:
   - JOIN/GROUP BY操作でのshuffle最適化機会を特定
   - 必要に応じてREPARTITION/COALESCE提案
   - スキューデータ対策のREPARTITION BY RANGE提案

4. メモリ最適化:
   - カラムナー実行エンジン活用
   - ベクトル化処理最適化
   - オフヒープメモリ活用

5. 並列処理最適化:
   - Adaptive Query Execution調整
   - Dynamic Resource Allocation
   - Speculation実行制御

6. データ型・圧縮最適化:
   - カラム型選択最適化
   - 圧縮アルゴリズム選択
   - エンコーディング最適化

**重要な指示: REPARTITIONヒント生成について**

shuffle_optimization_needed が "YES" の場合、必ず以下の手順でREPARTITIONヒントを追加してください：

1. repartition_recommendations で特定された各操作に対して適切なREPARTITION文を追加
2. repartition_type に基づいて以下の形式を使用：
   - REPARTITION(パーティション数): 均等分散の場合
   - REPARTITION(パーティション数, partition_expression): 特定式での分散の場合  
   - COALESCE(パーティション数): パーティション数削減の場合
   - REPARTITION BY RANGE(partition_expression): スキューデータ対策の場合

3. REPARTITIONは対象操作の直前に配置し、WITH句またはヒントコメントとして記述

**例:**
```sql
-- スキューデータ対策の例
WITH repartitioned_data AS (
  SELECT /*+ REPARTITION BY RANGE(date_column) */ * FROM large_table
)
SELECT date_column, COUNT(*)
FROM repartitioned_data
GROUP BY date_column

-- JOIN最適化の例  
SELECT /*+ REPARTITION(400, a.join_key) */ a.*, b.*
FROM table_a a
JOIN /*+ REPARTITION(400, b.join_key) */ table_b b ON a.join_key = b.join_key
```

最適化結果をJSON形式で出力してください:
{{
    "comprehensive_analysis": {{
        "query_complexity": {{
            "complexity_level": "LOW/MEDIUM/HIGH/VERY_HIGH",
            "bottleneck_operations": ["ボトルネック操作"],
            "optimization_potential": "最適化ポテンシャル"
        }},
        "data_characteristics": {{
            "data_size_gb": データサイズ,
            "table_count": テーブル数,
            "join_complexity": "JOIN複雑度",
            "aggregation_complexity": "集約複雑度"
        }}
    }},
    "architectural_optimizations": {{
        "storage_format_recommendations": [
            {{
                "table": "テーブル名",
                "current_format": "現在のフォーマット",
                "recommended_format": "推奨フォーマット",
                "expected_improvement": "期待される改善"
            }}
        ],
        "partitioning_strategy": {{
            "partition_columns": ["パーティションカラム"],
            "partitioning_scheme": "パーティション方式",
            "z_ordering_candidates": ["Z-Orderingカラム"]
        }}
    }},
    "advanced_join_optimizations": {{
        "bucket_join_opportunities": [
            {{
                "tables": ["対象テーブル"],
                "bucket_column": "バケットカラム",
                "bucket_count": バケット数,
                "expected_benefit": "期待される効果"
            }}
        ],
        "join_strategy_recommendations": [
            {{
                "join_operation": "JOIN操作",
                "recommended_strategy": "推奨戦略",
                "reasoning": "選択理由"
            }}
        ]
    }},
    "shuffle_optimization_analysis": {{
        "shuffle_optimization_needed": "YES/NO",
        "shuffle_optimization_details": "詳細説明",
        "repartition_recommendations": [
            {{
                "target_operation": "対象操作",
                "repartition_type": "REPARTITION/COALESCE/REPARTITION BY RANGE",
                "partition_expression": "パーティション式",
                "expected_improvement": "期待される改善"
            }}
        ]
    }},
    "memory_optimizations": {{
        "columnar_execution": {{
            "vectorization_opportunities": ["ベクトル化機会"],
            "memory_layout_optimization": "メモリレイアウト最適化"
        }},
        "off_heap_memory": {{
            "off_heap_storage_benefit": "オフヒープストレージ効果",
            "recommended_settings": ["推奨設定"]
        }}
    }},
    "parallel_processing_optimizations": {{
        "aqe_recommendations": [
            {{
                "setting": "設定項目",
                "recommended_value": "推奨値",
                "reasoning": "設定理由"
            }}
        ],
        "resource_allocation": {{
            "dynamic_allocation": true/false,
            "executor_scaling_strategy": "Executor スケーリング戦略"
        }}
    }},
    "optimized_implementations": {{
        "optimized_query_with_repartition_hints": "shuffle_optimization_neededがYESの場合、上記の指示に従ってREPARTITION最適化を適用したクエリ。NOの場合は元のクエリ",
        "optimized_query_with_all_hints": "全ヒント付きクエリ",
        "optimized_query_architectural": "アーキテクチャ最適化クエリ",
        "optimized_query_comprehensive": "包括的最適化クエリ"
    }},
    "implementation_roadmap": {{
        "phase1_quick_wins": [
            {{
                "optimization": "最適化内容",
                "implementation_time": "実装時間",
                "expected_improvement": "期待される改善",
                "risk_level": "リスクレベル"
            }}
        ],
        "phase2_structural_changes": [
            {{
                "optimization": "最適化内容", 
                "implementation_time": "実装時間",
                "expected_improvement": "期待される改善",
                "risk_level": "リスクレベル"
            }}
        ],
        "phase3_architectural_changes": [
            {{
                "optimization": "最適化内容",
                "implementation_time": "実装時間", 
                "expected_improvement": "期待される改善",
                "risk_level": "リスクレベル"
            }}
        ]
    }},
    "monitoring_and_validation": {{
        "key_metrics_to_monitor": ["監視すべきメトリクス"],
        "validation_queries": ["検証クエリ"],
        "rollback_criteria": ["ロールバック基準"]
    }}
}}
"""