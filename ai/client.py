"""
Anthropic Claude API wrapper.

Single point of entry for every AI module in this package. Handles:
  - API-key resolution (env var > Prefect Block > Settings).
  - Model selection (sonnet default; haiku for cheap calls).
  - Retry on transient API errors.
  - Token-budget guardrails (every call carries max_tokens).
  - Structured prompts via a small DSL: ``Prompt.system + user``.

NOT a generic LLM router. Claude-only. If the team wants OpenAI / Gemini
later, this module is the seam (add a second adapter implementing the
same `complete()` signature).
"""

from __future__ import annotations

import logging
import os
import time
from dataclasses import dataclass, field
from typing import Optional

log = logging.getLogger(__name__)

DEFAULT_MODEL = os.environ.get("PT_AI_MODEL", "claude-sonnet-4-5")
DEFAULT_MAX_TOKENS = int(os.environ.get("PT_AI_MAX_TOKENS", "4096"))


@dataclass
class Prompt:
    """One AI call's input. Keep the contract small."""

    user: str
    system: str = ""
    max_tokens: int = DEFAULT_MAX_TOKENS
    model: str = DEFAULT_MODEL
    temperature: float = 0.0   # Default deterministic; override for creative tasks.
    metadata: dict = field(default_factory=dict)


@dataclass
class Completion:
    """One AI call's output."""

    text: str
    model: str
    input_tokens: int
    output_tokens: int
    stop_reason: str = ""

    @property
    def total_tokens(self) -> int:
        return self.input_tokens + self.output_tokens

    @property
    def estimated_cost_usd(self) -> float:
        """Approximate per Sonnet 4.5 pricing (cents-per-million tokens)."""
        # Sonnet 4.5: ~$3/M input, $15/M output.
        return (self.input_tokens * 3.0 + self.output_tokens * 15.0) / 1_000_000.0


def _resolve_api_key() -> str:
    """Resolve the Anthropic API key in priority order."""
    # 1. Explicit env var.
    key = os.environ.get("ANTHROPIC_API_KEY")
    if key:
        return key
    # 2. config.py Settings (loads from .env via pydantic).
    try:
        import sys
        sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
        from config import settings

        key = getattr(settings, "anthropic_api_key", None)
        if key:
            return key
    except Exception:  # noqa: BLE001
        pass
    raise RuntimeError(
        "ANTHROPIC_API_KEY not set. "
        "Set via env var, .env file, or Prefect Block."
    )


def complete(prompt: Prompt) -> Completion:
    """Send a prompt to Claude and return the completion.

    Args:
        prompt: Prompt dataclass with user + system + model + max_tokens.

    Returns:
        Completion with the assistant's text and token usage.

    Raises:
        RuntimeError: API key not configured.
        anthropic.APIError on any unrecoverable API failure (after retries).
    """
    try:
        import anthropic
    except ImportError as exc:
        raise RuntimeError(
            "anthropic SDK not installed — pip install anthropic"
        ) from exc

    api_key = _resolve_api_key()
    client = anthropic.Anthropic(api_key=api_key)

    # Build messages.
    messages = [{"role": "user", "content": prompt.user}]

    # Retry loop for transient errors.
    last_exc: Optional[Exception] = None
    for attempt in range(3):
        try:
            resp = client.messages.create(
                model=prompt.model,
                max_tokens=prompt.max_tokens,
                temperature=prompt.temperature,
                system=prompt.system if prompt.system else anthropic.NOT_GIVEN,
                messages=messages,
            )
            return Completion(
                text="".join(
                    block.text for block in resp.content if hasattr(block, "text")
                ),
                model=resp.model,
                input_tokens=resp.usage.input_tokens,
                output_tokens=resp.usage.output_tokens,
                stop_reason=resp.stop_reason or "",
            )
        except Exception as exc:  # noqa: BLE001
            last_exc = exc
            wait = 2 ** attempt
            log.warning(
                "Claude API attempt %d failed: %s — retrying in %ds",
                attempt + 1, exc, wait,
            )
            time.sleep(wait)

    raise RuntimeError(f"Claude API failed after 3 attempts: {last_exc}") from last_exc


def complete_text(user: str, system: str = "", **kwargs) -> str:
    """Convenience: send a prompt and return just the text."""
    return complete(Prompt(user=user, system=system, **kwargs)).text
