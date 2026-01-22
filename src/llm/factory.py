"""LLM client factory."""

from typing import Union

from .base import LLMClient
from .databricks import DatabricksLLMClient
from .openai import OpenAILLMClient
from .azure_openai import AzureOpenAILLMClient
from .anthropic import AnthropicLLMClient
from ..config import LLMConfig, get_config


def create_llm_client(config: LLMConfig = None) -> LLMClient:
    """Create an LLM client based on configuration.

    Args:
        config: LLM configuration. If None, uses global config.

    Returns:
        An LLM client instance
    """
    if config is None:
        config = get_config().llm

    provider = config.provider

    if provider == "databricks":
        return DatabricksLLMClient(config=config.databricks)
    elif provider == "openai":
        return OpenAILLMClient(config=config.openai)
    elif provider == "azure_openai":
        return AzureOpenAILLMClient(config=config.azure_openai)
    elif provider == "anthropic":
        return AnthropicLLMClient(config=config.anthropic)
    else:
        raise ValueError(f"Unknown LLM provider: {provider}")


# Module-level client instance (lazy initialization)
_client: Union[LLMClient, None] = None


def get_llm_client() -> LLMClient:
    """Get or create the global LLM client instance."""
    global _client
    if _client is None:
        _client = create_llm_client()
    return _client


def reset_llm_client() -> None:
    """Reset the global LLM client instance."""
    global _client
    _client = None


def call_llm(prompt: str) -> str:
    """Convenience function to call the LLM.

    Args:
        prompt: The prompt to send

    Returns:
        The LLM response
    """
    client = get_llm_client()
    return client.call_with_retry(prompt)
