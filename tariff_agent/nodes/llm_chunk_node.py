from __future__ import annotations

from tariff_agent.state import PipelineState
from tariff_agent.utils.llm_client import run_llm_on_chunks


def llm_chunk_node(state: PipelineState) -> dict:
    force = bool(state.get("meta", {}).get("force", False))
    run_llm_on_chunks(force=force)
    return {"messages": ["stage=llm_chunk complete"]}
