"""Databricks Model Serving LLM client."""

import os
from typing import Any, Dict, Optional

try:
    import requests
except ImportError:
    requests = None

from .base import LLMClient
from ..config import DatabricksLLMConfig


class DatabricksLLMClient(LLMClient):
    """Client for Databricks Model Serving endpoints."""

    def __init__(
        self,
        config: DatabricksLLMConfig,
        workspace_url: Optional[str] = None,
        token: Optional[str] = None,
        max_retries: int = 3,
        timeout: int = 300,
    ):
        super().__init__(max_retries=max_retries, timeout=timeout)
        self.config = config
        self._workspace_url = workspace_url
        self._token = token

    @property
    def provider_name(self) -> str:
        return "Databricks"

    def _get_token(self) -> str:
        """Get Databricks API token."""
        if self._token:
            return self._token

        # Try to get from dbutils (Databricks notebook environment)
        try:
            # pylint: disable=undefined-variable
            token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()  # noqa: F821
            return token
        except Exception:
            pass

        # Fall back to environment variable
        token = os.environ.get("DATABRICKS_TOKEN")
        if token:
            return token

        raise ValueError(
            "Failed to obtain Databricks token. "
            "Please set the environment variable DATABRICKS_TOKEN."
        )

    def _get_workspace_url(self) -> str:
        """Get Databricks workspace URL."""
        if self._workspace_url:
            return self._workspace_url

        # Try to get from spark config
        try:
            # pylint: disable=undefined-variable
            return spark.conf.get("spark.databricks.workspaceUrl")  # noqa: F821
        except Exception:
            pass

        # Try to get from dbutils
        try:
            # pylint: disable=undefined-variable
            return dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags().get("browserHostName").get()  # noqa: F821
        except Exception:
            pass

        # Fall back to environment variable
        workspace_url = os.environ.get("DATABRICKS_WORKSPACE_URL")
        if workspace_url:
            return workspace_url

        raise ValueError(
            "Failed to obtain Databricks workspace URL. "
            "Please set the environment variable DATABRICKS_WORKSPACE_URL."
        )

    def _build_payload(self, prompt: str) -> Dict[str, Any]:
        """Build the API request payload."""
        payload = {
            "messages": [{"role": "user", "content": prompt}],
            "max_tokens": self.config.max_tokens,
            "temperature": self.config.temperature,
        }

        # Add thinking mode if enabled
        if self.config.thinking_enabled:
            payload["thinking"] = {
                "type": "enabled",
                "budget_tokens": self.config.thinking_budget_tokens,
            }

        return payload

    def call(self, prompt: str) -> str:
        """Call Databricks Model Serving API."""
        if requests is None:
            raise ImportError("requests library is required for Databricks LLM client")

        token = self._get_token()
        workspace_url = self._get_workspace_url()

        endpoint_url = (
            f"https://{workspace_url}/serving-endpoints/"
            f"{self.config.endpoint_name}/invocations"
        )

        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        }

        payload = self._build_payload(prompt)

        try:
            response = requests.post(
                endpoint_url,
                headers=headers,
                json=payload,
                timeout=self.timeout,
            )

            if response.status_code == 200:
                result = response.json()
                content = (
                    result.get("choices", [{}])[0]
                    .get("message", {})
                    .get("content", "")
                )
                print("✅ Databricks analysis completed")
                return content

            # Handle specific error cases
            if response.status_code == 400 and "maximum tokens" in response.text.lower():
                raise TokenLimitError(
                    f"Token limit exceeded. Consider reducing max_tokens. "
                    f"Current: {self.config.max_tokens}"
                )

            raise APIError(
                f"API Error: Status code {response.status_code}\n{response.text}"
            )

        except requests.exceptions.Timeout:
            raise TimeoutError(
                f"Request timed out after {self.timeout} seconds. "
                "Consider checking endpoint status or reducing prompt size."
            )


class TokenLimitError(Exception):
    """Raised when token limit is exceeded."""
    pass


class APIError(Exception):
    """Raised for API errors."""
    pass
