"""Schema designer node: converts a user query into structured column definitions."""
from __future__ import annotations

import json
import logging
import re

from openai import OpenAI

from tariff_agent.dataset_graph.state import DatasetState
from tariff_agent.prompts.dataset_prompt import (
    SCHEMA_DESIGNER_SYSTEM_PROMPT,
    build_schema_design_user_prompt,
)
from tariff_agent.utils.config import get_settings

logger = logging.getLogger(__name__)

_FENCE_RE = re.compile(r"^\s*```(?:json)?\s*|\s*```\s*$", re.I | re.M)
_THINK_RE = re.compile(r"<think>.*?</think>|<redacted_thinking>.*?</redacted_thinking>", re.I | re.DOTALL)
_JSON_OBJ_RE = re.compile(r"\{.*\}", re.DOTALL)

_DEFAULT_NAICS_SECTORS = [
    "manufacturing", "mining_oil_gas", "financial_services",
    "utilities", "retail_trade", "transportation", "agriculture",
]


def _strip(s: str) -> str:
    s = _THINK_RE.sub("", s).strip()
    s = _FENCE_RE.sub("", s).strip()
    return s


def _parse_json(content: str) -> dict:
    raw = _strip(content)
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        m = _JSON_OBJ_RE.search(raw)
        if m:
            return json.loads(m.group())
        raise


def schema_node(state: DatasetState) -> DatasetState:
    """Call the LLM to design (or refine) an extraction schema from the user query."""
    cfg = get_settings()
    client = OpenAI(base_url=cfg.vllm_base_url, api_key=cfg.vllm_api_key, timeout=120)

    # Collect sector-like context from the active index (SEDAR NAICS or generic)
    try:
        import pandas as pd
        idx_path = state.get("corpus_index_csv") or str(cfg.resolve(cfg.filings_index_path))
        idx = pd.read_csv(idx_path, dtype=str)
        if "naics_sector" in idx.columns:
            naics_sectors = [s for s in idx["naics_sector"].dropna().unique().tolist() if s]
        else:
            naics_sectors = _DEFAULT_NAICS_SECTORS
    except Exception:
        naics_sectors = _DEFAULT_NAICS_SECTORS

    user_prompt = build_schema_design_user_prompt(
        user_query=state.get("user_query", ""),
        naics_sectors=naics_sectors,
        schema_feedback=state.get("schema_feedback", ""),
    )

    logger.info("schema_node: calling LLM (iteration %d)", state.get("schema_iteration", 0) + 1)
    resp = client.chat.completions.create(
        model=cfg.vllm_model_name,
        messages=[
            {"role": "system", "content": SCHEMA_DESIGNER_SYSTEM_PROMPT},
            {"role": "user", "content": user_prompt},
        ],
        temperature=0.2,
        max_tokens=1500,
        extra_body={"chat_template_kwargs": {"enable_thinking": False}},
    )
    raw_content = resp.choices[0].message.content or ""

    try:
        data = _parse_json(raw_content)
    except Exception as exc:
        logger.error("schema_node: failed to parse LLM response: %s\nRaw: %s", exc, raw_content[:500])
        return {**state, "error": f"Schema LLM parse error: {exc}"}

    columns = data.get("columns", [])
    if not columns:
        return {**state, "error": "LLM returned empty columns list"}

    return {
        **state,
        "dataset_name": data.get("dataset_name", "custom_extraction"),
        "dataset_description": data.get("description", ""),
        "proposed_columns": columns,
        "schema_approved": False,
        "schema_feedback": "",
        "schema_iteration": state.get("schema_iteration", 0) + 1,
        "error": "",
    }
