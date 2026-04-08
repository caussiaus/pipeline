from __future__ import annotations

from tariff_agent.state import PipelineState
from tariff_agent.utils.docling_pipeline import run_docling_on_filings
from tariff_agent.utils.vllm_lifecycle import maybe_start_vllm_after_parse


def parse_node(state: PipelineState) -> dict:
    force = bool(state.get("meta", {}).get("force", False))
    run_docling_on_filings(force=force)
    maybe_start_vllm_after_parse()
    return {"messages": ["stage=parse complete"]}
