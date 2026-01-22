"""Configuration settings for the SQL Profiler Analysis Tool."""

from dataclasses import dataclass, field
from typing import Literal, Optional
import os


@dataclass
class DatabricksLLMConfig:
    """Databricks Model Serving configuration."""
    endpoint_name: str = "databricks-claude-opus-4-5"
    max_tokens: int = 32000
    temperature: float = 0.0
    thinking_enabled: bool = False
    thinking_budget_tokens: int = 10000


@dataclass
class OpenAIConfig:
    """OpenAI API configuration."""
    api_key: str = ""
    model: str = "gpt-4o"
    max_tokens: int = 16000
    temperature: float = 0.0

    def __post_init__(self):
        if not self.api_key:
            self.api_key = os.environ.get("OPENAI_API_KEY", "")


@dataclass
class AzureOpenAIConfig:
    """Azure OpenAI API configuration."""
    api_key: str = ""
    endpoint: str = ""
    deployment_name: str = ""
    api_version: str = "2024-02-01"
    max_tokens: int = 16000
    temperature: float = 0.0

    def __post_init__(self):
        if not self.api_key:
            self.api_key = os.environ.get("AZURE_OPENAI_API_KEY", "")


@dataclass
class AnthropicConfig:
    """Anthropic API configuration."""
    api_key: str = ""
    model: str = "claude-3-5-sonnet-20241022"
    max_tokens: int = 16000
    temperature: float = 0.0

    def __post_init__(self):
        if not self.api_key:
            self.api_key = os.environ.get("ANTHROPIC_API_KEY", "")


@dataclass
class LLMConfig:
    """LLM provider configuration."""
    provider: Literal["databricks", "openai", "azure_openai", "anthropic"] = "databricks"
    databricks: DatabricksLLMConfig = field(default_factory=DatabricksLLMConfig)
    openai: OpenAIConfig = field(default_factory=OpenAIConfig)
    azure_openai: AzureOpenAIConfig = field(default_factory=AzureOpenAIConfig)
    anthropic: AnthropicConfig = field(default_factory=AnthropicConfig)


@dataclass
class ShuffleAnalysisConfig:
    """Enhanced shuffle optimization settings."""
    memory_per_partition_threshold_mb: int = 512
    high_memory_threshold_gb: int = 100
    long_execution_threshold_sec: int = 300
    enable_liquid_clustering_advice: bool = True
    enable_partition_tuning_advice: bool = True
    enable_cluster_sizing_advice: bool = True
    shuffle_analysis_enabled: bool = True

    @property
    def memory_per_partition_threshold_bytes(self) -> int:
        return self.memory_per_partition_threshold_mb * 1024 * 1024


@dataclass
class AnalysisConfig:
    """Main analysis configuration."""
    # Input/Output
    json_file_path: str = ""
    output_file_dir: str = "./output"

    # Language and features
    output_language: Literal["ja", "en"] = "en"
    explain_enabled: bool = True
    debug_enabled: bool = False

    # Database settings (for EXPLAIN)
    catalog: str = ""
    database: str = ""

    # Optimization settings
    max_optimization_attempts: int = 3
    max_retries: int = 3

    # Feature flags
    structured_extraction_enabled: bool = True
    enhanced_error_handling: bool = True
    save_intermediate_results: bool = False
    staged_judgment_mode: bool = True
    strict_validation_mode: bool = False
    debug_json_enabled: bool = False

    # LLM configuration
    llm: LLMConfig = field(default_factory=LLMConfig)

    # Shuffle analysis configuration
    shuffle_analysis: ShuffleAnalysisConfig = field(default_factory=ShuffleAnalysisConfig)

    def __post_init__(self):
        if self.output_file_dir and not os.path.exists(self.output_file_dir):
            os.makedirs(self.output_file_dir, exist_ok=True)


# Global configuration instance
_config: Optional[AnalysisConfig] = None


def get_config() -> AnalysisConfig:
    """Get the global configuration instance."""
    global _config
    if _config is None:
        _config = AnalysisConfig()
    return _config


def set_config(config: AnalysisConfig) -> None:
    """Set the global configuration instance."""
    global _config
    _config = config


def t(ja: str, en: str) -> str:
    """Language toggle helper for inline strings.

    Args:
        ja: Japanese text
        en: English text

    Returns:
        Text in the configured language
    """
    config = get_config()
    return en if config.output_language == "en" else ja
