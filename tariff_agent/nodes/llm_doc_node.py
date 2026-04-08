from __future__ import annotations

from tariff_agent.state import PipelineState
from tariff_agent.utils.doc_level import run_doc_level


def llm_doc_node(state: PipelineState) -> dict:
    force = bool(state.get("meta", {}).get("force", False))
    run_doc_level(force=force)
    return {"messages": ["stage=llm_doc complete"]}
