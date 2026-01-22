"""Anthropic LLM client."""

import os
from typing import Any, Dict

try:
    import requests
except ImportError:
    requests = None

from .base import LLMClient
from ..config import AnthropicConfig


class AnthropicLLMClient(LLMClient):
    """Client for Anthropic API."""

    API_URL = "https://api.anthropic.com/v1/messages"

    def __init__(
        self,
        config: AnthropicConfig,
        max_retries: int = 3,
        timeout: int = 300,
    ):
        super().__init__(max_retries=max_retries, timeout=timeout)
        self.config = config

    @property
    def provider_name(self) -> str:
        return "Anthropic"

    def _get_api_key(self) -> str:
        """Get Anthropic API key."""
        api_key = self.config.api_key or os.environ.get("ANTHROPIC_API_KEY")
        if not api_key:
            raise ValueError(
                "Anthropic API key is not configured. "
                "Please set api_key or environment variable ANTHROPIC_API_KEY."
            )
        return api_key

    def _build_payload(self, prompt: str) -> Dict[str, Any]:
        """Build the API request payload."""
        return {
            "model": self.config.model,
            "max_tokens": self.config.max_tokens,
            "temperature": self.config.temperature,
            "messages": [{"role": "user", "content": prompt}],
        }

    def call(self, prompt: str) -> str:
        """Call Anthropic API."""
        if requests is None:
            raise ImportError("requests library is required for Anthropic LLM client")

        api_key = self._get_api_key()

        headers = {
            "x-api-key": api_key,
            "Content-Type": "application/json",
            "anthropic-version": "2023-06-01",
        }

        payload = self._build_payload(prompt)

        try:
            response = requests.post(
                self.API_URL,
                headers=headers,
                json=payload,
                timeout=self.timeout,
            )

            if response.status_code == 200:
                result = response.json()
                content = result["content"][0]["text"]
                print("✅ Anthropic analysis completed")
                return content

            raise APIError(
                f"Anthropic API Error: Status code {response.status_code}\n{response.text}"
            )

        except requests.exceptions.Timeout:
            raise TimeoutError(
                f"Request timed out after {self.timeout} seconds."
            )


class APIError(Exception):
    """Raised for API errors."""
    pass
