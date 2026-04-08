"""vLLM profile-based client routing.

Two logical serving profiles — do NOT restart vLLM between them.
Route by workload type instead:

  interactive   Low-concurrency, tunable temperature, thinking optionally on.
                Used during schema design and sample-corpus preview (≤20 rows).
                Latency matters more than throughput.

  batch         Zero temperature, thinking off, max concurrency.
                Used for full-corpus extraction (600+ rows).
                Throughput matters; time-to-first-token per row is irrelevant.

vLLM already handles mixed workloads via continuous batching + chunked prefill.
The router here just controls request-level knobs (temperature, concurrency,
chat_template_kwargs) so you get behaviorally different outputs without ever
touching the serving process.
"""
from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from typing import Any, Literal

from openai import AsyncOpenAI, OpenAI

from tariff_agent.utils.config import Settings, get_settings

logger = logging.getLogger(__name__)

ProfileName = Literal["interactive", "batch"]


@dataclass
class VLLMProfile:
    name: ProfileName
    temperature: float
    max_tokens: int
    max_concurrent_requests: int
    enable_thinking: bool
    top_p: float = 1.0
    extra_kwargs: dict[str, Any] = field(default_factory=dict)

    def chat_template_kwargs(self) -> dict[str, Any]:
        return {"enable_thinking": self.enable_thinking}

    def extra_body(self, guided_json: dict | None = None) -> dict[str, Any]:
        body: dict[str, Any] = {"chat_template_kwargs": self.chat_template_kwargs()}
        if guided_json is not None:
            body["guided_json"] = guided_json
        body.update(self.extra_kwargs)
        return body


def _build_profiles(cfg: Settings) -> dict[ProfileName, VLLMProfile]:
    """Construct interactive and batch profiles from Settings.

    Both share the same vLLM endpoint; only request parameters differ.
    Add VLLM_INTERACTIVE_TEMPERATURE, VLLM_INTERACTIVE_MAX_TOKENS etc. to
    .env to override without code changes.
    """
    import os

    def _float(key: str, default: float) -> float:
        return float(os.environ.get(key, default))

    def _int(key: str, default: int) -> int:
        return int(os.environ.get(key, default))

    def _bool(key: str, default: bool) -> bool:
        v = os.environ.get(key, "").strip().lower()
        if not v:
            return default
        return v in ("1", "true", "yes", "on")

    interactive = VLLMProfile(
        name="interactive",
        temperature=_float("VLLM_INTERACTIVE_TEMPERATURE", 0.2),
        max_tokens=_int("VLLM_INTERACTIVE_MAX_TOKENS", 2000),
        max_concurrent_requests=_int("VLLM_INTERACTIVE_CONCURRENCY", 4),
        enable_thinking=_bool("VLLM_INTERACTIVE_THINKING", False),
        top_p=_float("VLLM_INTERACTIVE_TOP_P", 1.0),
    )

    batch = VLLMProfile(
        name="batch",
        temperature=0.0,
        max_tokens=cfg.vllm_max_tokens,
        max_concurrent_requests=cfg.vllm_max_concurrent_requests,
        enable_thinking=False,
        top_p=1.0,
    )

    return {"interactive": interactive, "batch": batch}


# Module-level cache — profiles are created once per process
_profiles: dict[ProfileName, VLLMProfile] | None = None


def get_profile(name: ProfileName = "batch", cfg: Settings | None = None) -> VLLMProfile:
    global _profiles
    if _profiles is None:
        _profiles = _build_profiles(cfg or get_settings())
    return _profiles[name]


def make_sync_client(profile: VLLMProfile, cfg: Settings | None = None) -> OpenAI:
    s = cfg or get_settings()
    return OpenAI(base_url=s.vllm_base_url, api_key=s.vllm_api_key, timeout=120)


def make_async_client(profile: VLLMProfile, cfg: Settings | None = None) -> AsyncOpenAI:
    s = cfg or get_settings()
    return AsyncOpenAI(
        base_url=s.vllm_base_url,
        api_key=s.vllm_api_key,
        timeout=s.vllm_timeout_sec,
    )


def profile_for_workload(n_rows: int, *, interactive_threshold: int = 25) -> ProfileName:
    """Auto-select profile based on how many rows need processing."""
    return "interactive" if n_rows <= interactive_threshold else "batch"
