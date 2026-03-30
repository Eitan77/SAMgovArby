"""Provider abstractions for Headroom SDK.

Providers encapsulate model-specific behavior like tokenization,
context limits, and cost estimation.

Supported Providers:
- OpenAIProvider: Native OpenAI models (GPT-4o, o1, etc.)
- AnthropicProvider: Claude models
- GoogleProvider: Google Gemini models
- CohereProvider: Cohere Command models
- OpenAICompatibleProvider: Universal provider for any OpenAI-compatible API
  (Ollama, vLLM, Together, Groq, Fireworks, LM Studio, etc.)
- LiteLLMProvider: Universal provider via LiteLLM (100+ providers)
"""

from .anthropic import AnthropicProvider
from .base import Provider, TokenCounter
from .cohere import CohereProvider
from .google import GoogleProvider
from .litellm import (
    LiteLLMProvider,
    create_litellm_provider,
    is_litellm_available,
)
from .openai import OpenAIProvider
from .openai_compatible import (
    ModelCapabilities,
    OpenAICompatibleProvider,
    create_anyscale_provider,
    create_fireworks_provider,
    create_groq_provider,
    create_lmstudio_provider,
    create_ollama_provider,
    create_together_provider,
    create_vllm_provider,
)

__all__ = [
    # Base
    "Provider",
    "TokenCounter",
    # Native providers
    "OpenAIProvider",
    "AnthropicProvider",
    "GoogleProvider",
    "CohereProvider",
    # Universal providers
    "OpenAICompatibleProvider",
    "ModelCapabilities",
    "LiteLLMProvider",
    "is_litellm_available",
    # Factory functions
    "create_ollama_provider",
    "create_together_provider",
    "create_groq_provider",
    "create_fireworks_provider",
    "create_anyscale_provider",
    "create_vllm_provider",
    "create_lmstudio_provider",
    "create_litellm_provider",
]
