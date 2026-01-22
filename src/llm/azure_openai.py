"""Azure OpenAI LLM client."""

import os
from typing import Any, Dict

try:
    import requests
except ImportError:
    requests = None

from .base import LLMClient
from ..config import AzureOpenAIConfig


class AzureOpenAILLMClient(LLMClient):
    """Client for Azure OpenAI API."""

    def __init__(
        self,
        config: AzureOpenAIConfig,
        max_retries: int = 3,
        timeout: int = 300,
    ):
        super().__init__(max_retries=max_retries, timeout=timeout)
        self.config = config

    @property
    def provider_name(self) -> str:
        return "Azure OpenAI"

    def _get_api_key(self) -> str:
        """Get Azure OpenAI API key."""
        api_key = self.config.api_key or os.environ.get("AZURE_OPENAI_API_KEY")
        if not api_key:
            raise ValueError(
                "Azure OpenAI API key is not configured. "
                "Please set api_key or environment variable AZURE_OPENAI_API_KEY."
            )
        return api_key

    def _validate_config(self) -> None:
        """Validate configuration is complete."""
        if not self.config.endpoint:
            raise ValueError("Azure OpenAI endpoint is not configured.")
        if not self.config.deployment_name:
            raise ValueError("Azure OpenAI deployment_name is not configured.")

    def _get_endpoint_url(self) -> str:
        """Build the API endpoint URL."""
        return (
            f"{self.config.endpoint}/openai/deployments/"
            f"{self.config.deployment_name}/chat/completions"
            f"?api-version={self.config.api_version}"
        )

    def _build_payload(self, prompt: str) -> Dict[str, Any]:
        """Build the API request payload."""
        return {
            "messages": [{"role": "user", "content": prompt}],
            "max_tokens": self.config.max_tokens,
            "temperature": self.config.temperature,
        }

    def call(self, prompt: str) -> str:
        """Call Azure OpenAI API."""
        if requests is None:
            raise ImportError("requests library is required for Azure OpenAI LLM client")

        self._validate_config()
        api_key = self._get_api_key()

        headers = {
            "api-key": api_key,
            "Content-Type": "application/json",
        }

        payload = self._build_payload(prompt)
        endpoint_url = self._get_endpoint_url()

        try:
            response = requests.post(
                endpoint_url,
                headers=headers,
                json=payload,
                timeout=self.timeout,
            )

            if response.status_code == 200:
                result = response.json()
                content = result["choices"][0]["message"]["content"]
                print("✅ Azure OpenAI analysis completed")
                return content

            raise APIError(
                f"Azure OpenAI API Error: Status code {response.status_code}\n{response.text}"
            )

        except requests.exceptions.Timeout:
            raise TimeoutError(
                f"Request timed out after {self.timeout} seconds."
            )


class APIError(Exception):
    """Raised for API errors."""
    pass
