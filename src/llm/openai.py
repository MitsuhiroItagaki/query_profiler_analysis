"""OpenAI LLM client."""

import os
from typing import Any, Dict

try:
    import requests
except ImportError:
    requests = None

from .base import LLMClient
from ..config import OpenAIConfig


class OpenAILLMClient(LLMClient):
    """Client for OpenAI API."""

    API_URL = "https://api.openai.com/v1/chat/completions"

    def __init__(
        self,
        config: OpenAIConfig,
        max_retries: int = 3,
        timeout: int = 300,
    ):
        super().__init__(max_retries=max_retries, timeout=timeout)
        self.config = config

    @property
    def provider_name(self) -> str:
        return "OpenAI"

    def _get_api_key(self) -> str:
        """Get OpenAI API key."""
        api_key = self.config.api_key or os.environ.get("OPENAI_API_KEY")
        if not api_key:
            raise ValueError(
                "OpenAI API key is not configured. "
                "Please set api_key or environment variable OPENAI_API_KEY."
            )
        return api_key

    def _build_payload(self, prompt: str) -> Dict[str, Any]:
        """Build the API request payload."""
        return {
            "model": self.config.model,
            "messages": [{"role": "user", "content": prompt}],
            "max_tokens": self.config.max_tokens,
            "temperature": self.config.temperature,
        }

    def call(self, prompt: str) -> str:
        """Call OpenAI API."""
        if requests is None:
            raise ImportError("requests library is required for OpenAI LLM client")

        api_key = self._get_api_key()

        headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
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
                content = result["choices"][0]["message"]["content"]
                print("✅ OpenAI analysis completed")
                return content

            raise APIError(
                f"OpenAI API Error: Status code {response.status_code}\n{response.text}"
            )

        except requests.exceptions.Timeout:
            raise TimeoutError(
                f"Request timed out after {self.timeout} seconds."
            )


class APIError(Exception):
    """Raised for API errors."""
    pass
