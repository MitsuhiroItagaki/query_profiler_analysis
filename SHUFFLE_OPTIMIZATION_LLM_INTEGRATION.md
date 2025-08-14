# Enhanced Shuffle操作最適化分析のLLMプロンプト統合

## 概要

セル51のSQL Optimization Executionの前に、Enhanced Shuffle操作最適化分析の結果をクエリ最適化のLLMプロンプトに渡せるように統合しました。

## 統合の詳細

### 1. 分析結果の格納場所

Enhanced Shuffle分析の結果は `extracted_metrics['enhanced_shuffle_analysis']` に格納されています：

```python
# セル内での分析実行（既存）
if SHUFFLE_ANALYSIS_CONFIG.get("shuffle_analysis_enabled", True):
    enhanced_shuffle_analysis = analyze_enhanced_shuffle_operations(
        extracted_metrics.get('node_metrics', []), 
        OUTPUT_LANGUAGE
    )
    extracted_metrics['enhanced_shuffle_analysis'] = enhanced_shuffle_analysis
```

### 2. LLMプロンプトへの統合

`generate_optimized_query_with_llm` 関数内で、以下の処理を追加しました：

#### 2.1 分析結果の抽出・フォーマット
```python
# 🔧 Enhanced Shuffle操作最適化分析の抽出・フォーマット
enhanced_shuffle_summary = ""
enhanced_shuffle_analysis = metrics.get('enhanced_shuffle_analysis', {})

if enhanced_shuffle_analysis and enhanced_shuffle_analysis.get('shuffle_nodes'):
    # 分析結果をプロンプト用に要約
    overall_assessment = enhanced_shuffle_analysis.get('overall_assessment', {})
    shuffle_nodes = enhanced_shuffle_analysis.get('shuffle_nodes', [])
    
    # サマリー情報の生成
    # 個別Shuffle操作の詳細表示
    # 最適化推奨事項の生成
```

#### 2.2 プロンプトテンプレートへの追加
```python
【🔧 Enhanced Shuffle操作最適化分析】
{enhanced_shuffle_summary}
```

## 提供される情報

LLMプロンプトに以下の情報が含まれるようになりました：

### 3.1 全体サマリー
- Shuffle操作数（総数および最適化が必要な操作数）
- 総メモリ使用量
- 平均メモリ/パーティション
- Shuffle効率性スコア
- 最適化必要性の判定

### 3.2 個別Shuffle操作分析
各Shuffle操作について以下の詳細：
- ノードID
- 優先度（HIGH/LOW）
- パーティション数
- ピークメモリ使用量
- メモリ/パーティション比率
- 実行時間

### 3.3 最適化推奨事項
- パーティション数の調整提案
- クラスター拡張の検討
- REPARTITIONヒント適用の推奨（スピル検出時のみ）
- Liquid Clusteringによる根本的改善提案

## 統合によるメリット

### 4.1 より具体的な最適化提案
LLMは以下の具体的な情報を基に最適化を行えます：
- 実際のメモリ/パーティション比率
- 高優先度のShuffle操作の特定
- 効率性スコアに基づく改善の必要性

### 4.2 データドリブンな最適化
- 実際のプロファイラーデータに基づく分析結果
- 閾値（512MB/パーティション）に基づく最適化判定
- 具体的なノードIDと数値による精密な最適化

### 4.3 REPARTITIONヒントの適切な適用
- スピル検出時のみのREPARTITIONヒント適用
- Shuffle分析結果に基づく最適なパーティション数の提案

## 実装ファイル

以下のファイルに統合を実装しました：

### 5.1 メインファイル
- `query_profiler_analysis.py`
- `current_query_profiler_analysis.py`

### 5.2 統合された関数
- `generate_optimized_query_with_llm()` 関数内
- 行番号: 約8020-8080（分析結果抽出）および8170（プロンプト統合）

## 使用例

### 6.1 高優先度Shuffleの場合
```
【🔧 Enhanced Shuffle操作最適化分析】
Shuffle操作数: 3個（最適化必要: 1個）
総メモリ使用量: 445.07GB
平均メモリ/パーティション: 444.2MB
Shuffle効率性スコア: 66.7%
最適化必要性: はい

🔍 個別Shuffle操作分析:
1. Shuffle (Node ID: 13708)
   🚨 優先度: HIGH
   📊 パーティション数: 1
   🧠 ピークメモリ: 4.19GB
   ⚡ メモリ/パーティション: 4288.0MB
   ⏱️ 実行時間: 4.6秒

🎯 Shuffle最適化推奨事項:
- パーティション数の調整（目標: ≤512MB/パーティション）
- 高メモリ使用ノードのクラスター拡張検討
- REPARTITIONヒント適用（スピル検出時のみ）
- Liquid Clusteringによる根本的Shuffle削減
```

### 6.2 効率的なShuffleの場合
```
【🔧 Enhanced Shuffle操作最適化分析】
✅ Shuffle操作は効率的に動作しており、特別な最適化は不要です。
```

## 注意事項

### 7.1 分析データの可用性
- Enhanced Shuffle分析が無効の場合は「分析データが利用できません」と表示
- `SHUFFLE_ANALYSIS_CONFIG.shuffle_analysis_enabled = False` の場合は空の分析結果

### 7.2 プロンプトサイズの考慮
- 最大3つの重要なShuffle操作のみを詳細表示
- 高優先度ノードを優先的に選択
- トークン制限を考慮した簡潔な表示

### 7.3 設定との連携
- `SHUFFLE_ANALYSIS_CONFIG.memory_per_partition_threshold_mb` の閾値を使用
- 既存の設定と統合され、設定変更が分析結果に反映

## 今後の改善点

### 8.1 動的な詳細レベル調整
- プロンプトサイズに応じた詳細レベルの調整
- より多くのShuffle操作の表示（必要に応じて）

### 8.2 言語対応
- 英語での分析結果表示サポート
- `OUTPUT_LANGUAGE` 設定との連携

### 8.3 統計情報との統合
- EXPLAIN COST結果との相関分析
- より精密な最適化効果の予測

## 動作確認

統合の動作は以下のように確認できます：

1. Enhanced Shuffle分析が有効になっている
2. セル51のSQL Optimization Executionを実行
3. LLMプロンプトに「【🔧 Enhanced Shuffle操作最適化分析】」セクションが含まれる
4. 分析結果に基づいた具体的な最適化提案がLLMから返される

この統合により、Enhanced Shuffle操作最適化分析の結果が確実にクエリ最適化のLLMプロンプトに渡され、より効果的な最適化が可能になりました。