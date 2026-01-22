"""Base LLM client interface."""

from abc import ABC, abstractmethod
from typing import Optional
import time


class LLMClient(ABC):
    """Abstract base class for LLM clients."""

    def __init__(self, max_retries: int = 3, timeout: int = 300):
        self.max_retries = max_retries
        self.timeout = timeout

    @abstractmethod
    def call(self, prompt: str) -> str:
        """Send a prompt to the LLM and return the response.

        Args:
            prompt: The prompt text to send

        Returns:
            The LLM response text
        """
        pass

    def call_with_retry(self, prompt: str) -> str:
        """Call the LLM with retry logic.

        Args:
            prompt: The prompt text to send

        Returns:
            The LLM response text
        """
        last_error: Optional[Exception] = None

        for attempt in range(self.max_retries):
            try:
                if attempt > 0:
                    print(f"🔄 Retrying... (attempt {attempt + 1}/{self.max_retries})")
                    time.sleep(2 ** attempt)  # Exponential backoff

                return self.call(prompt)

            except Exception as e:
                last_error = e
                if attempt < self.max_retries - 1:
                    print(f"⚠️ Error occurred: {str(e)} - Retrying...")
                    continue

        raise last_error if last_error else RuntimeError("Unknown error")

    @property
    @abstractmethod
    def provider_name(self) -> str:
        """Return the provider name for logging."""
        pass
