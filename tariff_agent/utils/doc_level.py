from __future__ import annotations

import pandas as pd

from tariff_agent.utils.config import Settings, get_settings
from tariff_agent.utils.llm_client import run_llm_on_docs


def run_doc_level(
    settings: Settings | None = None,
    *,
    force: bool = False,
    update_filing_ids: set[str] | None = None,
) -> pd.DataFrame:
    """Pass 2: filing-level consolidation via OpenAI-compatible vLLM."""
    return run_llm_on_docs(settings, force=force, update_filing_ids=update_filing_ids)
