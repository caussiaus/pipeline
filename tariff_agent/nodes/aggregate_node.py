from __future__ import annotations

from tariff_agent.state import PipelineState
from tariff_agent.utils.aggregate import build_issuer_year_table


def aggregate_node(state: PipelineState) -> dict:
    force = bool(state.get("meta", {}).get("force", False))
    build_issuer_year_table(force=force)
    return {"messages": ["stage=aggregate complete"]}
