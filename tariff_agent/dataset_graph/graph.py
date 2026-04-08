"""LangGraph definition for the interactive dataset generation pipeline.

Flow:
  schema_design → [human: approve / give feedback]
      ├── feedback → schema_design (loop)
      └── approved → extraction → critique
                         → [human: export / refine schema]
                               ├── refine → schema_design (loop)
                               └── export → END
"""
from __future__ import annotations

import logging
from typing import Literal

from langgraph.graph import END, StateGraph

from tariff_agent.dataset_graph.critique_node import critique_node
from tariff_agent.dataset_graph.extraction_node import extraction_node
from tariff_agent.dataset_graph.schema_node import schema_node
from tariff_agent.dataset_graph.state import DatasetState

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Routing helpers
# ---------------------------------------------------------------------------

def _route_after_schema(state: DatasetState) -> Literal["extraction", "schema_design"]:
    if state.get("schema_approved"):
        return "extraction"
    return "schema_design"


def _route_after_critique(state: DatasetState) -> Literal["export", "schema_design"]:
    if state.get("export_approved"):
        return "export"
    return "schema_design"


# ---------------------------------------------------------------------------
# Export node (saves the CSV)
# ---------------------------------------------------------------------------

def export_node(state: DatasetState) -> DatasetState:
    """Persist the extracted rows to a timestamped CSV file."""
    import datetime
    from pathlib import Path

    import pandas as pd

    from tariff_agent.utils.config import get_settings

    rows = state.get("rows", [])
    if not rows:
        return {**state, "error": "No rows to export"}

    cfg = get_settings()
    if state.get("datasets_export_dir"):
        datasets_dir = Path(state["datasets_export_dir"])
    else:
        datasets_dir = cfg.resolve(getattr(cfg, "datasets_dir", "output/datasets"))
    datasets_dir.mkdir(parents=True, exist_ok=True)

    name = state.get("dataset_name", "custom_extraction")
    ts = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    path = datasets_dir / f"{name}_{ts}.csv"

    df = pd.DataFrame(rows)

    # Move identity columns to front
    id_cols = ["filing_id", "ticker", "issuer_name", "profile_number",
               "filing_date", "naics_sector", "mechanism"]
    ordered = [c for c in id_cols if c in df.columns]
    rest = [c for c in df.columns if c not in ordered]
    df = df[ordered + rest]

    df.to_csv(path, index=False)
    logger.info("export_node: saved %d rows to %s", len(df), path)

    return {**state, "dataset_path": str(path), "error": ""}


# ---------------------------------------------------------------------------
# Graph builder
# ---------------------------------------------------------------------------

def build_dataset_graph() -> StateGraph:
    g = StateGraph(DatasetState)

    g.add_node("schema_design", schema_node)
    g.add_node("extraction", extraction_node)
    g.add_node("critique", critique_node)
    g.add_node("export", export_node)

    g.set_entry_point("schema_design")

    # After schema design: human checkpoint decides whether to loop or proceed
    g.add_conditional_edges(
        "schema_design",
        _route_after_schema,
        {"extraction": "extraction", "schema_design": "schema_design"},
    )

    g.add_edge("extraction", "critique")

    # After critique: human checkpoint decides export vs schema redo
    g.add_conditional_edges(
        "critique",
        _route_after_critique,
        {"export": "export", "schema_design": "schema_design"},
    )

    g.add_edge("export", END)

    return g
