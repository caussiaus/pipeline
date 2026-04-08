from __future__ import annotations

import asyncio
import logging
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import orjson
import pandas as pd
from openai import APIConnectionError, APIStatusError, AsyncOpenAI, RateLimitError
from pydantic import ValidationError

from tariff_agent.prompts.chunk_prompt import (
    CHUNK_OUTPUT_JSON_SCHEMA,
    CHUNK_SYSTEM_PROMPT,
    build_chunk_user_prompt,
    keyword_hit,
    keyword_terms,
)
from tariff_agent.prompts.doc_prompt import (
    DOC_OUTPUT_JSON_SCHEMA,
    DOC_SYSTEM_PROMPT,
    build_doc_user_prompt,
)
from tariff_agent.state import (
    ChunkLLMOutput,
    ChunkRecord,
    EarningsEvidenceItem,
    FilingLLMOutput,
    KeyQuoteItem,
    MacroEvidenceItem,
    SupplyChainEvidenceItem,
    normalize_disclosure_quality,
    normalize_earnings_signal_type,
    normalize_filing_tariff_direction,
    normalize_filing_type,
    normalize_macro_signal_type,
    normalize_supply_chain_signal_type,
)
from tariff_agent.utils.chunking import estimate_tokens, truncate_text_to_estimated_tokens
from tariff_agent.utils.company_outputs import df_filings_for_csv, write_company_filing_json_artifacts
from tariff_agent.utils.config import Settings, get_settings
from tariff_agent.utils.meta_normalize import clean_meta_str
from tariff_agent.utils.vllm_lifecycle import wait_for_vllm_http

logger = logging.getLogger(__name__)

_FENCE_RE = re.compile(r"^\s*```(?:json)?\s*|\s*```\s*$", re.I | re.M)
# Qwen3 emits <think>…</think> (standard mode) OR <redacted_thinking>…</redacted_thinking>
# (server-side suppression). Both must be stripped before JSON parsing.
_THINKING_RE = re.compile(
    r"<think>.*?</think>|<redacted_thinking>.*?</redacted_thinking>",
    re.I | re.DOTALL,
)
# If the model ONLY outputs a think block and nothing else (shouldn't happen with guided decoding,
# but if max_tokens is very tight), capture the last JSON object after the block.
_JSON_OBJ_RE = re.compile(r"\{.*\}", re.DOTALL)


def _strip_fences(s: str) -> str:
    s = s.strip()
    s = _FENCE_RE.sub("", s)
    return s.strip()


def _strip_thinking_blocks(s: str) -> str:
    """Remove Qwen3-style reasoning wrappers (<think> or <redacted_thinking>) the server may emit."""
    return _THINKING_RE.sub("", s).strip()


def _parse_json_object(content: str) -> dict[str, Any]:
    """Parse the first JSON object from a completion, stripping thinking blocks and fences."""
    raw = _strip_thinking_blocks(_strip_fences(content))
    try:
        return orjson.loads(raw)
    except orjson.JSONDecodeError:
        # Last resort: extract the outermost {...} in case stray text surrounds it
        m = _JSON_OBJ_RE.search(raw)
        if m:
            return orjson.loads(m.group())
        raise


def _parse_earnings_evidence(raw: Any) -> list[EarningsEvidenceItem]:
    if not isinstance(raw, list):
        return []
    out: list[EarningsEvidenceItem] = []
    for item in raw:
        if isinstance(item, str) and item.strip():
            out.append(EarningsEvidenceItem(quote=item, signal_type="UNCLEAR", magnitude_text=None))
        elif isinstance(item, dict):
            out.append(
                EarningsEvidenceItem(
                    quote=str(item.get("quote", "")),
                    signal_type=normalize_earnings_signal_type(item.get("signal_type", "UNCLEAR")),
                    magnitude_text=item.get("magnitude_text"),
                )
            )
    return out


def _parse_supply_evidence(raw: Any) -> list[SupplyChainEvidenceItem]:
    if not isinstance(raw, list):
        return []
    out: list[SupplyChainEvidenceItem] = []
    for item in raw:
        if isinstance(item, str) and item.strip():
            out.append(SupplyChainEvidenceItem(quote=item, chain_type="OTHER", magnitude_text=None))
        elif isinstance(item, dict):
            out.append(
                SupplyChainEvidenceItem(
                    quote=str(item.get("quote", "")),
                    chain_type=normalize_supply_chain_signal_type(item.get("chain_type", "OTHER")),
                    magnitude_text=item.get("magnitude_text"),
                )
            )
    return out


def _parse_macro_evidence(raw: Any) -> list[MacroEvidenceItem]:
    if not isinstance(raw, list):
        return []
    out: list[MacroEvidenceItem] = []
    for item in raw:
        if isinstance(item, str) and item.strip():
            out.append(MacroEvidenceItem(quote=item, macro_type="OTHER", magnitude_text=None))
        elif isinstance(item, dict):
            out.append(
                MacroEvidenceItem(
                    quote=str(item.get("quote", "")),
                    macro_type=normalize_macro_signal_type(item.get("macro_type", "OTHER")),
                    magnitude_text=item.get("magnitude_text"),
                )
            )
    return out


def _parse_key_quotes(raw: Any) -> list[KeyQuoteItem]:
    if not isinstance(raw, list):
        return []
    out: list[KeyQuoteItem] = []
    for item in raw:
        if isinstance(item, str) and item.strip():
            out.append(KeyQuoteItem(quote=item))
        elif isinstance(item, dict):
            ps = item.get("page_start", 0)
            pe = item.get("page_end", 0)
            try:
                ps_i = int(ps) if ps is not None and str(ps).strip() != "" else 0
            except (TypeError, ValueError):
                ps_i = 0
            try:
                pe_i = int(pe) if pe is not None and str(pe).strip() != "" else 0
            except (TypeError, ValueError):
                pe_i = 0
            out.append(
                KeyQuoteItem(
                    section_path=str(item.get("section_path", "")),
                    quote=str(item.get("quote", "")),
                    signal_type=str(item.get("signal_type", "")),
                    chunk_id=str(item.get("chunk_id", "")),
                    page_start=ps_i,
                    page_end=pe_i,
                )
            )
    return out


def _enrich_key_quotes_provenance(data: dict[str, Any], chunk_rows: list[dict[str, Any]]) -> None:
    """Attach chunk_id + PDF pages to filing-level quotes when the model omits them; enables dashboard drill-down."""
    kq = data.get("key_quotes")
    if not isinstance(kq, list):
        return

    def _pages(r: dict[str, Any]) -> tuple[int, int]:
        try:
            p0 = int(r.get("page_start") or 0)
        except (TypeError, ValueError):
            p0 = 0
        try:
            p1 = int(r.get("page_end") or 0)
        except (TypeError, ValueError):
            p1 = 0
        return p0, p1

    for item in kq:
        if not isinstance(item, dict):
            continue
        q = str(item.get("quote", "")).strip()
        cid = str(item.get("chunk_id", "")).strip()
        by_id = {str(r.get("chunk_id", "")): r for r in chunk_rows if r.get("chunk_id")}
        if cid and cid in by_id:
            r = by_id[cid]
            p0, p1 = _pages(r)
            if not item.get("page_start") and not item.get("page_end"):
                item["page_start"], item["page_end"] = p0, p1
            continue
        if not q:
            continue
        needle = q[:120] if len(q) > 120 else q
        for r in chunk_rows:
            text = str(r.get("text", ""))
            if needle and needle in text:
                item["chunk_id"] = str(r.get("chunk_id", ""))
                p0, p1 = _pages(r)
                item["page_start"], item["page_end"] = p0, p1
                if not str(item.get("section_path", "")).strip():
                    item["section_path"] = str(r.get("section_path", ""))
                break


def _coerce_bool(v: Any) -> bool:
    if isinstance(v, bool):
        return v
    if v is None:
        return False
    if isinstance(v, (int, float)) and not isinstance(v, bool):
        return bool(v)
    if isinstance(v, str):
        return v.strip().lower() in ("true", "1", "yes", "y")
    return bool(v)


def _json_listish(raw: Any) -> list[Any]:
    """Qwen sometimes returns one evidence object as a bare string or dict instead of a list."""
    if raw is None:
        return []
    if isinstance(raw, list):
        return raw
    return [raw]


def _normalize_chunk_llm_payload(p: dict[str, Any]) -> dict[str, Any]:
    """Repair shapes Qwen sometimes returns under ``json_object`` (strings vs evidence dicts, list vs bool)."""
    out = dict(p)
    out["earnings_evidence"] = [e.model_dump() for e in _parse_earnings_evidence(_json_listish(out.get("earnings_evidence")))]
    out["macro_evidence"] = [e.model_dump() for e in _parse_macro_evidence(_json_listish(out.get("macro_evidence")))]
    out["supply_chain_evidence"] = [
        e.model_dump() for e in _parse_supply_evidence(_json_listish(out.get("supply_chain_evidence")))
    ]

    otm = out.get("other_tariff_mentions")
    if not isinstance(otm, list):
        otm = [str(otm)] if otm not in (None, "") else []
    mentions_list = [str(x).strip() for x in otm if str(x).strip()]

    ul = out.get("uncertainty_language")
    if isinstance(ul, list):
        any_signal = False
        for x in ul:
            if isinstance(x, str) and x.strip():
                any_signal = True
                s = x.strip()
                if s not in mentions_list:
                    mentions_list.append(s)
            elif isinstance(x, bool) and x:
                any_signal = True
        out["uncertainty_language"] = any_signal
    else:
        out["uncertainty_language"] = _coerce_bool(ul)

    out["other_tariff_mentions"] = mentions_list

    spp = out.get("specific_tariff_programs")
    if not isinstance(spp, list):
        spp = [str(spp)] if spp not in (None, "") else []
    out["specific_tariff_programs"] = [str(x).strip() for x in spp if str(x).strip()]

    for key in (
        "mentions_tariffs",
        "earnings_impact_present",
        "macro_risk_present",
        "supply_chain_risk_present",
        "pass_through_mentioned",
        "mitigation_mentioned",
        "llm_skipped",
    ):
        if key in out:
            out[key] = _coerce_bool(out[key])
    return out


def _null_chunk_output_ids(
    chunk_id: str,
    filing_id: str,
    *,
    model_version: str,
    skipped: bool,
) -> ChunkLLMOutput:
    now = datetime.now(timezone.utc).isoformat()
    return ChunkLLMOutput(
        chunk_id=chunk_id,
        filing_id=filing_id,
        mentions_tariffs=False,
        earnings_impact_present=False,
        earnings_evidence=[],
        macro_risk_present=False,
        macro_evidence=[],
        supply_chain_risk_present=False,
        supply_chain_evidence=[],
        other_tariff_mentions=[],
        pass_through_mentioned=False,
        mitigation_mentioned=False,
        uncertainty_language=False,
        specific_tariff_programs=[],
        model_version=model_version,
        inference_timestamp=now,
        llm_skipped=skipped,
    )


def _null_chunk_output(
    chunk: ChunkRecord,
    *,
    model_version: str,
    skipped: bool,
) -> ChunkLLMOutput:
    return _null_chunk_output_ids(
        chunk.chunk_id,
        chunk.filing_id,
        model_version=model_version,
        skipped=skipped,
    )


def _chunk_output_from_api(
    chunk: ChunkRecord,
    data: dict[str, Any],
    *,
    model_version: str,
) -> ChunkLLMOutput:
    now = datetime.now(timezone.utc).isoformat()
    payload: dict[str, Any] = dict(data)
    payload["chunk_id"] = chunk.chunk_id
    payload["filing_id"] = chunk.filing_id
    payload["model_version"] = model_version
    payload["inference_timestamp"] = now
    payload["llm_skipped"] = False
    try:
        out = ChunkLLMOutput.model_validate(_normalize_chunk_llm_payload(payload))
    except ValidationError as e:
        logger.warning("[llm] chunk %s pydantic parse failure, null record: %s", chunk.chunk_id, e)
        return _null_chunk_output(chunk, model_version=model_version, skipped=False)
    earns = out.earnings_evidence
    macro = out.macro_evidence
    supp = out.supply_chain_evidence
    return out.model_copy(
        update={
            "earnings_impact_present": out.earnings_impact_present or bool(earns),
            "macro_risk_present": out.macro_risk_present or bool(macro),
            "supply_chain_risk_present": out.supply_chain_risk_present or bool(supp),
        }
    )


def _response_format_chunk(settings: Settings) -> dict[str, Any] | None:
    mode = settings.vllm_response_format.strip().lower()
    if mode in ("0", "none", "off"):
        return None
    if mode == "json_object":
        return {"type": "json_object"}
    return {
        "type": "json_schema",
        "json_schema": {
            "name": "chunk_tariff",
            "schema": CHUNK_OUTPUT_JSON_SCHEMA,
            "strict": True,
        },
    }


def _response_format_doc(settings: Settings) -> dict[str, Any] | None:
    mode = settings.vllm_response_format.strip().lower()
    if mode in ("0", "none", "off"):
        return None
    if mode == "json_object":
        return {"type": "json_object"}
    return {
        "type": "json_schema",
        "json_schema": {
            "name": "filing_tariff",
            "schema": DOC_OUTPUT_JSON_SCHEMA,
            "strict": True,
        },
    }


def _chat_template_kwargs_dict(settings: Settings) -> dict[str, Any] | None:
    raw = settings.vllm_chat_template_kwargs_json.strip()
    if not raw or raw in ("{}", "null", "None"):
        return None
    try:
        obj = orjson.loads(raw)
    except orjson.JSONDecodeError:
        logger.warning("VLLM_CHAT_TEMPLATE_KWARGS is not valid JSON; ignoring")
        return None
    if not isinstance(obj, dict) or not obj:
        return None
    return obj


def _build_vllm_extra_body(
    settings: Settings,
    *,
    guided_schema: dict[str, Any] | None,
    response_format: dict[str, Any] | None,
) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
    """Merge vLLM extra_body keys. Returns (extra_body, effective_response_format).

    vLLM OpenAI-compatible server forwards unknown keys on ``extra_body`` — use it for both
    ``guided_json`` (when enabled) and ``chat_template_kwargs`` (Qwen3 thinking off, etc.).
    If guided decoding is on, avoid sending duplicate strict ``json_schema`` in ``response_format``;
    use ``json_object`` and rely on ``guided_json`` for structure.
    """
    extra: dict[str, Any] = {}
    rf = response_format
    if settings.use_guided_decoding and guided_schema is not None:
        extra["guided_json"] = guided_schema
        if rf is not None and rf.get("type") == "json_schema":
            rf = {"type": "json_object"}
    tmpl = _chat_template_kwargs_dict(settings)
    if tmpl is not None:
        extra["chat_template_kwargs"] = tmpl
    return (extra if extra else None, rf)


async def _chat_json(
    client: AsyncOpenAI,
    settings: Settings,
    *,
    system: str,
    user: str,
    response_format: dict[str, Any] | None,
    guided_schema: dict[str, Any] | None = None,
    max_tokens_override: int | None = None,
) -> dict[str, Any]:
    delay = 1.0
    last_err: Exception | None = None
    for attempt in range(max(1, settings.vllm_max_retries)):
        try:
            extra, rf_eff = _build_vllm_extra_body(
                settings, guided_schema=guided_schema, response_format=response_format
            )
            kwargs: dict[str, Any] = dict(
                model=settings.vllm_model_name,
                messages=[
                    {"role": "system", "content": system},
                    {"role": "user", "content": user},
                ],
                max_tokens=max_tokens_override if max_tokens_override is not None else settings.vllm_max_tokens,
                temperature=settings.vllm_temperature,
                top_p=settings.vllm_top_p,
            )
            if rf_eff is not None:
                kwargs["response_format"] = rf_eff  # type: ignore[assignment]
            if extra:
                kwargs["extra_body"] = extra
            resp = await client.chat.completions.create(**kwargs)
            choice = resp.choices[0]
            if choice.finish_reason == "length":
                logger.warning(
                    "vLLM response truncated (finish_reason=length, max_tokens=%s); JSON may be incomplete",
                    kwargs["max_tokens"],
                )
            content = (choice.message.content or "").strip()
            return _parse_json_object(content)
        except (RateLimitError, APIConnectionError, APIStatusError, orjson.JSONDecodeError, UnicodeDecodeError) as e:
            last_err = e
            if isinstance(e, APIStatusError) and e.status_code not in (408, 409, 425, 429, 500, 502, 503, 504):
                raise
            logger.warning("vLLM call failed (%s), retry %s/%s", e, attempt + 1, settings.vllm_max_retries)
            await asyncio.sleep(delay)
            delay = min(delay * 2, 60.0)
    assert last_err is not None
    raise last_err


async def _process_one_chunk(
    client: AsyncOpenAI,
    settings: Settings,
    sem: asyncio.Semaphore,
    rf: dict[str, Any] | None,
    model_version: str,
    c: ChunkRecord,
) -> ChunkLLMOutput:
    if not keyword_hit(c.text):
        return _null_chunk_output(c, model_version=model_version, skipped=True)
    # estimate_tokens() uses word_count/0.75 (GPT-2 approx); Qwen3 BPE counts ~1.65-1.80x more on
    # dense financial prose.  Safety factor 0.55 ≈ 1/1.80 with extra margin for prompt wrapper + schema.
    chunk_completion = settings.llm_chunk_max_tokens
    body_budget_est = max(
        512,
        settings.vllm_model_max_context_tokens
        - chunk_completion
        - settings.llm_chunk_prompt_reserve_tokens,
    )
    body_budget = max(256, int(body_budget_est * 0.55))
    c_llm = c
    if estimate_tokens(c.text) > body_budget:
        logger.warning(
            "[llm] chunk %s: ~%s est. input tokens (body) exceeds safe budget %s (0.75 × %s) — truncating for API",
            c.chunk_id,
            estimate_tokens(c.text),
            body_budget,
            body_budget_est,
        )
        c_llm = c.model_copy(
            update={"text": truncate_text_to_estimated_tokens(c.text, body_budget)},
        )
    try:
        async with sem:
            data = await _chat_json(
                client,
                settings,
                system=CHUNK_SYSTEM_PROMPT,
                user=build_chunk_user_prompt(c_llm),
                response_format=rf,
                guided_schema=CHUNK_OUTPUT_JSON_SCHEMA if settings.use_guided_decoding else None,
                max_tokens_override=chunk_completion,
            )
        return _chunk_output_from_api(c, data, model_version=model_version)
    except Exception as e:
        logger.warning("[llm] chunk %s failure, null record: %s", c.chunk_id, e)
        return _null_chunk_output(c, model_version=model_version, skipped=False)


def _chunk_record_from_parquet_row(r: pd.Series) -> ChunkRecord:
    """Parquet round-trip uses plain dict + numpy arrays; Pydantic v2 rejects ``model_validate(Series)``."""
    d: dict[str, Any] = r.to_dict()
    k = d.get("keyword_hit_terms")
    if k is None or (isinstance(k, float) and pd.isna(k)):
        d["keyword_hit_terms"] = []
    elif hasattr(k, "tolist"):
        d["keyword_hit_terms"] = [str(x) for x in k.tolist() if str(x).strip()]
    elif isinstance(k, list):
        d["keyword_hit_terms"] = [str(x) for x in k if str(x).strip()]
    else:
        d["keyword_hit_terms"] = [str(k)] if str(k).strip() else []
    return ChunkRecord.model_validate(d)


def _flush_chunks_llm_parquet(
    out_path: Path,
    base_old: pd.DataFrame | None,
    run_outputs: list[ChunkLLMOutput],
) -> None:
    piece = pd.DataFrame([o.model_dump() for o in run_outputs])
    if base_old is not None and not base_old.empty:
        piece = pd.concat([base_old, piece], ignore_index=True)
    piece.to_parquet(out_path, index=False)


async def _async_run_llm_chunks(settings: Settings, *, force: bool = False) -> pd.DataFrame:
    path = settings.resolve(settings.chunks_parquet)
    df = pd.read_parquet(path)
    if "keyword_hit_terms" not in df.columns:
        df = df.copy()
        df["keyword_hit_terms"] = df["text"].astype(str).map(keyword_terms)
    if "keyword_hit" not in df.columns:
        df = df.copy()
        df["keyword_hit"] = df["text"].astype(str).map(keyword_hit)

    out_path = settings.resolve(settings.chunks_llm_parquet)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    done: set[str] = set()
    old_df: pd.DataFrame | None = None
    if out_path.is_file() and not force:
        old_df = pd.read_parquet(out_path)
        if "chunk_id" in old_df.columns:
            done = set(old_df["chunk_id"].astype(str))

    model_version = settings.vllm_model_name
    rf = _response_format_chunk(settings)

    chunk_ids = df["chunk_id"].astype(str)
    if force:
        pending_df = df
    else:
        pending_df = df.loc[~chunk_ids.isin(done)]

    # ``skip_llm_chunk_if_exists`` means "do not recompute when already complete" — not "ignore partial parquet".
    if pending_df.empty and not force and settings.skip_llm_chunk_if_exists and out_path.is_file():
        return pd.read_parquet(out_path)

    if pending_df.empty:
        if not out_path.is_file():
            pd.DataFrame(columns=[f for f in ChunkLLMOutput.model_fields.keys()]).to_parquet(
                out_path, index=False
            )
        return pd.read_parquet(out_path)

    kh = pending_df["keyword_hit"].fillna(False).astype(bool)
    skip_df = pending_df.loc[~kh]
    llm_df = pending_df.loc[kh]

    run_outputs: list[ChunkLLMOutput] = []
    for row in skip_df.itertuples(index=False):
        run_outputs.append(
            _null_chunk_output_ids(
                str(row.chunk_id),
                str(row.filing_id),
                model_version=model_version,
                skipped=True,
            )
        )

    base_old = None if force else old_df
    gather_cap = max(1, settings.llm_chunk_gather_batch_size)
    ckpt_every = settings.llm_chunk_checkpoint_every

    if llm_df.empty:
        if run_outputs:
            _flush_chunks_llm_parquet(out_path, base_old, run_outputs)
        elif not out_path.is_file():
            pd.DataFrame(columns=[f for f in ChunkLLMOutput.model_fields.keys()]).to_parquet(
                out_path, index=False
            )
        return pd.read_parquet(out_path)

    wait_for_vllm_http(settings)

    client = AsyncOpenAI(
        base_url=settings.vllm_base_url.rstrip("/"),
        api_key=settings.vllm_api_key,
        timeout=settings.vllm_timeout_sec,
        max_retries=0,
    )
    sem = asyncio.Semaphore(max(1, settings.vllm_max_concurrent_requests))

    if ckpt_every > 0 and run_outputs:
        _flush_chunks_llm_parquet(out_path, base_old, run_outputs)
        logger.info(
            "chunks_llm: wrote checkpoint (%s keyword-skip rows) before vLLM",
            len(run_outputs),
        )

    llm_records = [_chunk_record_from_parquet_row(llm_df.iloc[i]) for i in range(len(llm_df))]
    llm_since_ckpt = 0
    for i in range(0, len(llm_records), gather_cap):
        batch = llm_records[i : i + gather_cap]
        tasks = [_process_one_chunk(client, settings, sem, rf, model_version, c) for c in batch]
        batch_out = await asyncio.gather(*tasks)
        run_outputs.extend(batch_out)
        llm_since_ckpt += len(batch_out)
        if ckpt_every > 0 and llm_since_ckpt >= ckpt_every:
            _flush_chunks_llm_parquet(out_path, base_old, run_outputs)
            logger.info("chunks_llm: checkpoint (%s rows written this run)", len(run_outputs))
            llm_since_ckpt = 0

    if run_outputs:
        _flush_chunks_llm_parquet(out_path, base_old, run_outputs)

    return pd.read_parquet(out_path)


def run_llm_on_chunks(settings: Settings | None = None, *, force: bool = False) -> pd.DataFrame:
    settings = settings or get_settings()
    return asyncio.run(_async_run_llm_chunks(settings, force=force))


async def _async_doc_call(
    client: AsyncOpenAI,
    settings: Settings,
    sem: asyncio.Semaphore,
    filing_meta: dict[str, Any],
    chunk_rows: list[dict[str, Any]],
    rf: dict[str, Any] | None,
) -> dict[str, Any]:
    # Pass-2 output has 4 key_quote objects with 6 fields each + summary + programs list —
    # give it 50 % more headroom than the base setting (floored at 1536).
    doc_max_tokens = max(1536, int(settings.vllm_max_tokens * 1.5))
    async with sem:
        user = build_doc_user_prompt(filing_meta, chunk_rows)
        raw = await _chat_json(
            client,
            settings,
            system=DOC_SYSTEM_PROMPT,
            user=user,
            response_format=rf,
            guided_schema=DOC_OUTPUT_JSON_SCHEMA if settings.use_guided_decoding else None,
            max_tokens_override=doc_max_tokens,
        )
        if isinstance(raw, dict):
            _enrich_key_quotes_provenance(raw, chunk_rows)
        return raw


def _fiscal_year_from_meta(fmeta: dict[str, Any]) -> int | None:
    fd = fmeta.get("filing_date")
    if fd is None or (isinstance(fd, float) and pd.isna(fd)):
        return None
    ts = pd.to_datetime(fd, errors="coerce")
    if pd.isna(ts):
        return None
    try:
        return int(ts.year)
    except (TypeError, ValueError):
        return None


def _default_filing_output(fid: str, fmeta: dict[str, Any]) -> FilingLLMOutput:
    return FilingLLMOutput(
        filing_id=fid,
        profile_id=clean_meta_str(fmeta.get("profile_id")),
        ticker=clean_meta_str(fmeta.get("ticker")) or "unknown",
        issuer_name=clean_meta_str(fmeta.get("issuer_name") or fmeta.get("issuer")),
        filing_type=normalize_filing_type(fmeta.get("filing_type", "OTHER")),
        filing_date=clean_meta_str(fmeta.get("filing_date")),
        fiscal_year=_fiscal_year_from_meta(fmeta),
        has_tariff_discussion=False,
        tariff_direction="NONE",
        earnings_tariff_score=0,
        supply_chain_tariff_score=0,
        macro_tariff_score=0,
        pass_through_flag=False,
        mitigation_flag=False,
        mitigation_summary=None,
        quantified_impact=False,
        quantified_impact_text=None,
        specific_tariff_programs=[],
        disclosure_quality="BOILERPLATE",
        doc_summary_sentence="No tariff-related passages detected in extracted chunks.",
        key_quotes=[],
        first_tariff_section_path=None,
        counter_tariff_status=None,
        cusma_offset_credit_mentioned=None,
    )


def _clamp03(v: object) -> int:
    try:
        return max(0, min(3, int(v)))
    except (TypeError, ValueError):
        return 0


def _filing_from_llm_dict(data: dict[str, Any], fmeta: dict[str, Any], *, fid: str) -> FilingLLMOutput:
    progs = data.get("specific_tariff_programs") or []
    if not isinstance(progs, list):
        progs = []
    progs = [str(p) for p in progs if str(p).strip()]
    try:
        _cts_vals = frozenset({"active", "removed_sept_2025", "steel_aluminum_retained"})
        cts = data.get("counter_tariff_status")
        if cts not in _cts_vals:
            cts = None
        coc = data.get("cusma_offset_credit_mentioned")
        if coc in (None, "", "null"):
            coc = None
        elif isinstance(coc, str):
            coc = coc.lower() in ("1", "true", "yes")
        return FilingLLMOutput(
            filing_id=fid,
            profile_id=clean_meta_str(fmeta.get("profile_id")),
            ticker=clean_meta_str(fmeta.get("ticker")) or "unknown",
            issuer_name=clean_meta_str(fmeta.get("issuer_name") or fmeta.get("issuer")),
            filing_type=normalize_filing_type(fmeta.get("filing_type", "OTHER")),
            filing_date=clean_meta_str(fmeta.get("filing_date")),
            fiscal_year=_fiscal_year_from_meta(fmeta),
            has_tariff_discussion=bool(data.get("has_tariff_discussion", False)),
            tariff_direction=normalize_filing_tariff_direction(data.get("tariff_direction", "NONE")),
            earnings_tariff_score=_clamp03(data.get("earnings_tariff_score", 0)),
            supply_chain_tariff_score=_clamp03(data.get("supply_chain_tariff_score", 0)),
            macro_tariff_score=_clamp03(data.get("macro_tariff_score", 0)),
            pass_through_flag=bool(data.get("pass_through_flag", False)),
            mitigation_flag=bool(data.get("mitigation_flag", False)),
            mitigation_summary=data.get("mitigation_summary"),
            quantified_impact=bool(data.get("quantified_impact", False)),
            quantified_impact_text=data.get("quantified_impact_text"),
            specific_tariff_programs=progs,
            disclosure_quality=normalize_disclosure_quality(data.get("disclosure_quality", "BOILERPLATE")),
            doc_summary_sentence=str(data.get("doc_summary_sentence", "")),
            key_quotes=_parse_key_quotes(data.get("key_quotes")),
            first_tariff_section_path=data.get("first_tariff_section_path"),
            counter_tariff_status=cts,  # type: ignore[arg-type]
            cusma_offset_credit_mentioned=coc,
        )
    except ValidationError as e:
        logger.warning("[llm] filing %s output validate failure, default record: %s", fid, e)
        return _default_filing_output(fid, fmeta)


def _filing_model_from_csv_row(row: pd.Series) -> FilingLLMOutput:
    d: dict[str, Any] = row.to_dict()
    for col in ("key_quotes", "specific_tariff_programs"):
        v = d.get(col)
        if isinstance(v, str) and v.strip():
            try:
                d[col] = orjson.loads(v)
            except orjson.JSONDecodeError:
                d[col] = []
        elif v is None or (isinstance(v, float) and pd.isna(v)):
            d[col] = []
    for k in list(d.keys()):
        val = d[k]
        if isinstance(val, float) and pd.isna(val):
            d[k] = None
    d["profile_id"] = clean_meta_str(d.get("profile_id"))
    d["ticker"] = clean_meta_str(d.get("ticker")) or "unknown"
    d["issuer_name"] = clean_meta_str(d.get("issuer_name"))
    d["filing_date"] = clean_meta_str(d.get("filing_date"))
    # Optional enum / bool from CSV
    _cts_vals = frozenset({"active", "removed_sept_2025", "steel_aluminum_retained"})
    cts = d.get("counter_tariff_status")
    if cts not in _cts_vals:
        d["counter_tariff_status"] = None
    coc = d.get("cusma_offset_credit_mentioned")
    if coc is None or (isinstance(coc, str) and coc.strip().lower() in ("", "none", "null")):
        d["cusma_offset_credit_mentioned"] = None
    elif isinstance(coc, str):
        d["cusma_offset_credit_mentioned"] = coc.strip().lower() in ("1", "true", "yes")
    return FilingLLMOutput.model_validate(d)


async def _async_run_llm_docs(
    settings: Settings,
    *,
    force: bool = False,
    update_filing_ids: set[str] | None = None,
) -> pd.DataFrame:
    filings_path = settings.resolve(settings.filings_index_path)
    filings = pd.read_csv(filings_path)

    chunks = pd.read_parquet(settings.resolve(settings.chunks_parquet))
    chunks_llm = pd.read_parquet(settings.resolve(settings.chunks_llm_parquet))

    merged = chunks.merge(chunks_llm, on=["chunk_id", "filing_id"], how="left")
    positive = merged[merged["mentions_tariffs"] == True]  # noqa: E712

    out_csv = settings.resolve(settings.filings_llm_csv)
    out_pq = settings.resolve(settings.filings_llm_parquet)
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    out_pq.parent.mkdir(parents=True, exist_ok=True)

    if (
        not force
        and settings.skip_llm_doc_if_exists
        and out_csv.is_file()
        and update_filing_ids is None
    ):
        return pd.read_csv(out_csv)

    client = AsyncOpenAI(
        base_url=settings.vllm_base_url.rstrip("/"),
        api_key=settings.vllm_api_key,
        timeout=settings.vllm_timeout_sec,
        max_retries=0,
    )
    sem = asyncio.Semaphore(max(1, settings.vllm_max_concurrent_requests))
    rf = _response_format_doc(settings)

    ufid: set[str] | None = (
        {str(x) for x in update_filing_ids} if update_filing_ids is not None else None
    )

    filing_order: list[str] = []
    tasks: list[Any] = []
    for filing_id, group in positive.groupby("filing_id"):
        fs = str(filing_id)
        if ufid is not None and fs not in ufid:
            continue
        sub = filings[filings["filing_id"].astype(str) == fs]
        if sub.empty:
            continue
        filing_order.append(fs)
        meta = sub.iloc[0].to_dict()
        meta["filing_id"] = fs
        rows = group.to_dict(orient="records")
        tasks.append(_async_doc_call(client, settings, sem, meta, rows, rf))

    if tasks:
        wait_for_vllm_http(settings)

    results: list[Any] = await asyncio.gather(*tasks) if tasks else []
    by_fid: dict[str, Any] = dict(zip(filing_order, results))

    new_by_fid: dict[str, FilingLLMOutput] = {}
    if ufid is not None:
        for fid in ufid:
            sub = filings[filings["filing_id"].astype(str) == fid]
            if sub.empty:
                continue
            fmeta = sub.iloc[0].to_dict()
            if fid in by_fid:
                new_by_fid[fid] = _filing_from_llm_dict(by_fid[fid], fmeta, fid=fid)
            else:
                new_by_fid[fid] = _default_filing_output(fid, fmeta)

    prev_by_fid: dict[str, FilingLLMOutput] = {}
    if ufid is not None and not force:
        # Prefer parquet when doing targeted refresh — round-trips lists reliably; CSV can break Pydantic.
        prev_src = out_pq if out_pq.is_file() else out_csv
        if prev_src.is_file():
            prev_df = pd.read_parquet(prev_src) if prev_src == out_pq else pd.read_csv(prev_src)
            for _, pr in prev_df.iterrows():
                pfi = str(pr["filing_id"])
                if pfi in ufid:
                    continue
                try:
                    prev_by_fid[pfi] = _filing_model_from_csv_row(pr)
                except ValidationError:
                    logger.warning(
                        "[llm] could not restore filing %s from prior Pass-2 artifact (%s)",
                        pfi,
                        prev_src.name,
                    )

    out_models: list[FilingLLMOutput] = []
    for _, frow in filings.iterrows():
        fid = str(frow["filing_id"])
        fmeta = frow.to_dict()
        if ufid is not None:
            if fid in new_by_fid:
                out_models.append(new_by_fid[fid])
            elif fid in prev_by_fid:
                out_models.append(prev_by_fid[fid])
            else:
                out_models.append(_default_filing_output(fid, fmeta))
        elif fid in by_fid:
            out_models.append(_filing_from_llm_dict(by_fid[fid], fmeta, fid=fid))
        else:
            out_models.append(_default_filing_output(fid, fmeta))

    out_records = [m.model_dump() for m in out_models]
    df_out = pd.DataFrame(out_records)
    df_out.to_parquet(out_pq, index=False)
    df_filings_for_csv(df_out).to_csv(out_csv, index=False)
    write_company_filing_json_artifacts(out_records, settings)
    return df_out


def run_llm_on_docs(
    settings: Settings | None = None,
    *,
    force: bool = False,
    update_filing_ids: set[str] | None = None,
) -> pd.DataFrame:
    settings = settings or get_settings()
    return asyncio.run(
        _async_run_llm_docs(
            settings,
            force=force,
            update_filing_ids=update_filing_ids,
        )
    )
