"""Critique node: samples extraction rows and asks the LLM to assess quality."""
from __future__ import annotations

import json
import logging
import random
import re
from typing import Any

from openai import OpenAI

from tariff_agent.dataset_graph.state import DatasetState
from tariff_agent.prompts.dataset_prompt import CRITIQUE_SYSTEM_PROMPT, build_critique_user_prompt
from tariff_agent.utils.config import get_settings

logger = logging.getLogger(__name__)

_THINK_RE = re.compile(r"<think>.*?</think>|<redacted_thinking>.*?</redacted_thinking>", re.I | re.DOTALL)
_FENCE_RE = re.compile(r"^\s*```(?:json)?\s*|\s*```\s*$", re.I | re.M)
_JSON_OBJ_RE = re.compile(r"\{.*\}", re.DOTALL)

_INTERNAL_COLS = {"filing_id", "ticker", "issuer_name", "profile_number",
                  "filing_date", "naics_sector", "mechanism",
                  "_extraction_error", "_all_chunks", "_keyword_hits",
                  "_pass1_positive", "_evidence_blocks_used"}


def _strip_internals(row: dict) -> dict:
    return {k: v for k, v in row.items() if k not in _INTERNAL_COLS}


def _parse_json_block(text: str) -> dict[str, Any]:
    raw = _THINK_RE.sub("", text).strip()
    raw = _FENCE_RE.sub("", raw).strip()
    m = _JSON_OBJ_RE.search(raw)
    if m:
        try:
            return json.loads(m.group())
        except Exception:
            pass
    return {}


def critique_node(state: DatasetState) -> DatasetState:
    """Sample up to 15 rows and ask the LLM for a quality critique."""
    rows = state.get("rows", [])
    columns = state.get("proposed_columns", [])
    dataset_name = state.get("dataset_name", "dataset")

    if not rows:
        return {**state, "critique_text": "No rows extracted yet.", "critique_quality": "needs_work"}

    cfg = get_settings()
    client = OpenAI(base_url=cfg.vllm_base_url, api_key=cfg.vllm_api_key, timeout=120)

    sample = random.sample(rows, min(15, len(rows)))
    sample_clean = [_strip_internals(r) for r in sample]

    user_prompt = build_critique_user_prompt(dataset_name, columns, sample_clean)

    try:
        resp = client.chat.completions.create(
            model=cfg.vllm_model_name,
            messages=[
                {"role": "system", "content": CRITIQUE_SYSTEM_PROMPT},
                {"role": "user", "content": user_prompt},
            ],
            temperature=0.3,
            max_tokens=800,
            extra_body={"chat_template_kwargs": {"enable_thinking": False}},
        )
        content = resp.choices[0].message.content or ""
    except Exception as exc:
        logger.error("critique_node LLM error: %s", exc)
        return {**state, "critique_text": f"Critique failed: {exc}", "critique_quality": "needs_work"}

    meta = _parse_json_block(content)
    suggestions = meta.get("suggested_changes", [])
    quality = str(meta.get("overall_quality", "ok")).lower()
    if quality not in ("good", "ok", "needs_work"):
        quality = "ok"

    # Strip the trailing JSON block from the human-readable text
    critique_text = _JSON_OBJ_RE.sub("", _THINK_RE.sub("", content)).strip()

    return {
        **state,
        "critique_text": critique_text,
        "critique_suggestions": suggestions if isinstance(suggestions, list) else [],
        "critique_quality": quality,
    }
