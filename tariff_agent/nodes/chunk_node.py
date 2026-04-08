from __future__ import annotations

from tariff_agent.state import PipelineState
from tariff_agent.utils.chunking import run_chunking


def chunk_node(state: PipelineState) -> dict:
    force = bool(state.get("meta", {}).get("force", False))
    run_chunking(force=force)
    return {"messages": ["stage=chunk complete"]}
