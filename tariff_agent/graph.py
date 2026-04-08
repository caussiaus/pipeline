from __future__ import annotations

"""
Pipeline graph: parse → chunk → llm_chunk → llm_doc → aggregate → export
                                                                       ↓
                                              refine (conditional) ←──┘
                                                       ↓
                                                    aggregate
                                                       ↓
                                                    export → END

Refinement loop
---------------
Set ``meta["refine"] = True`` in initial state to enable the post-export refine
pass.  The loop runs at most ``meta["max_refine_iters"]`` times (default 3) and
only targets LLM-fixable QC errors.  Without ``meta["refine"]`` the graph exits
at END immediately after export.

Sync ``SqliteSaver`` + ``invoke`` matches a sync CLI entrypoint; async LLM work
runs inside nodes via ``asyncio.run`` at the client boundary.
"""

from typing import Any

from langgraph.checkpoint.memory import InMemorySaver
from langgraph.checkpoint.sqlite import SqliteSaver
from langgraph.graph import END, StateGraph

from tariff_agent.nodes.aggregate_node import aggregate_node
from tariff_agent.nodes.chunk_node import chunk_node
from tariff_agent.nodes.export_node import export_node
from tariff_agent.nodes.llm_chunk_node import llm_chunk_node
from tariff_agent.nodes.llm_doc_node import llm_doc_node
from tariff_agent.nodes.parse_node import parse_node
from tariff_agent.nodes.refine_node import refine_node
from tariff_agent.state import PipelineState
from tariff_agent.utils.config import get_settings


def _should_refine(state: PipelineState) -> str:
    """Conditional edge after export: loop into refine or exit."""
    meta = state.get("meta", {})
    if not meta.get("refine", False):
        return "end"
    iteration = int(meta.get("refine_iter", 0) or 0)
    max_iters = int(meta.get("max_refine_iters", 3) or 3)
    if iteration >= max_iters:
        return "end"
    return "refine"


def build_graph(*, checkpointer: InMemorySaver | SqliteSaver | None = None):
    g = StateGraph(PipelineState)

    # ── Core stages ────────────────────────────────────────────────────────
    g.add_node("parse",     parse_node)
    g.add_node("chunk",     chunk_node)
    g.add_node("llm_chunk", llm_chunk_node)
    g.add_node("llm_doc",   llm_doc_node)
    g.add_node("aggregate", aggregate_node)
    g.add_node("export",    export_node)

    # ── Refinement loop ────────────────────────────────────────────────────
    g.add_node("refine",    refine_node)

    g.set_entry_point("parse")
    g.add_edge("parse",     "chunk")
    g.add_edge("chunk",     "llm_chunk")
    g.add_edge("llm_chunk", "llm_doc")
    g.add_edge("llm_doc",   "aggregate")
    g.add_edge("aggregate", "export")

    # After export: either exit or loop through refine → aggregate → export
    g.add_conditional_edges("export", _should_refine, {"refine": "refine", "end": END})
    g.add_edge("refine",    "aggregate")

    if checkpointer is None:
        checkpointer = InMemorySaver()
    return g.compile(checkpointer=checkpointer)


def run_full_pipeline(
    *,
    thread_id: str = "default",
    force: bool = False,
    refine: bool = False,
    max_refine_iters: int = 3,
    refine_rules: str = "",
    refine_severity: str = "error",
) -> None:
    settings = get_settings()
    cfg: dict[str, Any] = {"configurable": {"thread_id": thread_id}}
    state: PipelineState = {
        "messages": [],
        "meta": {
            "force": force,
            "refine": refine,
            "max_refine_iters": max_refine_iters,
            "refine_iter": 0,
            "refine_rules": refine_rules,
            "refine_severity": refine_severity,
        },
    }
    path = settings.checkpoint_sqlite_path.strip()
    if path:
        conn = str(settings.resolve(path))
        with SqliteSaver.from_conn_string(conn) as saver:
            build_graph(checkpointer=saver).invoke(state, config=cfg)
    else:
        build_graph().invoke(state, config=cfg)
