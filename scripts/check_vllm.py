#!/usr/bin/env python3
"""Check vLLM reachability, model id, and that hyperparameters / extra_body work."""
from __future__ import annotations

import argparse
import asyncio
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import httpx
from openai import APIStatusError, AsyncOpenAI

from tariff_agent.utils.async_llm_client import _build_vllm_extra_body, _strip_thinking_blocks
from tariff_agent.utils.config import get_settings
from tariff_agent.prompts.chunk_prompt import CHUNK_SYSTEM_PROMPT, CHUNK_OUTPUT_JSON_SCHEMA


async def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--skip-guided-test", action="store_true", help="Only run plain + json_object with template kwargs")
    args = p.parse_args()

    s = get_settings()
    base = s.vllm_base_url.rstrip("/")

    key_hint = (s.vllm_api_key[:4] + "…") if len(s.vllm_api_key) > 4 else "(short)"
    print("Configuration (from .env / env):")
    print(f"  {base=} {s.vllm_model_name=} api_key_hint={key_hint!r}")
    print(f"  max_tokens={s.vllm_max_tokens} temp={s.vllm_temperature} top_p={s.vllm_top_p}")
    print(f"  use_guided_decoding={s.use_guided_decoding}")

    async with httpx.AsyncClient(timeout=30.0) as hc:
        r = await hc.get(
            f"{base}/models",
            headers={"Authorization": f"Bearer {s.vllm_api_key}"},
        )
    print(f"\nGET /v1/models -> {r.status_code}")
    if r.status_code != 200:
        print(r.text[:500])
        print("\nHint: set VLLM_API_KEY to match vLLM --api-key (OpenAI client sends Authorization: Bearer …).")
        return 1
    data = r.json()
    mids = [m.get("id") for m in data.get("data", [])]
    print(f"  models: {mids}")
    if s.vllm_model_name not in mids:
        print(f"\nERROR: VLLM_MODEL_NAME {s.vllm_model_name!r} not in server list. Fix .env.")
        return 1

    client = AsyncOpenAI(base_url=base, api_key=s.vllm_api_key, timeout=180.0, max_retries=0)

    print("\nProbe 1: plain completion (temp / top_p / max_tokens)")
    resp = await client.chat.completions.create(
        model=s.vllm_model_name,
        messages=[
            {"role": "system", "content": "Reply with exactly the word: OK"},
            {"role": "user", "content": "ping"},
        ],
        max_tokens=64,
        temperature=s.vllm_temperature,
        top_p=s.vllm_top_p,
    )
    raw1 = (resp.choices[0].message.content or "").strip()
    t1 = _strip_thinking_blocks(raw1)
    fr = resp.choices[0].finish_reason
    print(f"  content={t1!r} finish_reason={fr}")
    if "redacted_thinking" in raw1.lower() and raw1 != t1:
        print("  note: stripped <redacted_thinking> wrapper (pipeline does this before JSON decode).")
    if fr == "length":
        print("  note: hit max_tokens; increase if you need full reply.")

    tmpl_body: dict = {}
    from tariff_agent.utils.async_llm_client import _chat_template_kwargs_dict

    tk = _chat_template_kwargs_dict(s)
    if tk:
        tmpl_body["chat_template_kwargs"] = tk
    print("\nProbe 2: response_format=json_object + chat_template_kwargs (pipeline-style)")
    raw2 = ""
    try:
        resp2 = await client.chat.completions.create(
            model=s.vllm_model_name,
            messages=[
                {"role": "system", "content": 'Output only JSON: {"mentions_tariffs": false}'},
                {"role": "user", "content": "no tariffs"},
            ],
            max_tokens=64,
            temperature=s.vllm_temperature,
            top_p=s.vllm_top_p,
            response_format={"type": "json_object"},
            extra_body=tmpl_body if tmpl_body else None,
        )
        raw2 = (resp2.choices[0].message.content or "").strip()
        obj = json.loads(raw2)
        assert obj.get("mentions_tariffs") is False
        print(f"  OK parsed: {raw2[:120]!r}")
    except (AssertionError, json.JSONDecodeError) as e:
        print(f"  FAIL: {e} raw={raw2[:300]!r}")
        return 2

    if args.skip_guided_test or not s.use_guided_decoding:
        print("\nSkipping full guided_json probe (--skip-guided-test or USE_GUIDED_DECODING=0).")
        return 0

    print("\nProbe 3: full Pass-1 guided_json + chat_template (USE_GUIDED_DECODING=1)")
    extra, rf_eff = _build_vllm_extra_body(
        s,
        guided_schema=CHUNK_OUTPUT_JSON_SCHEMA,
        response_format={"type": "json_object"},
    )
    raw3 = ""
    try:
        resp3 = await client.chat.completions.create(
            model=s.vllm_model_name,
            messages=[
                {"role": "system", "content": CHUNK_SYSTEM_PROMPT},
                {"role": "user", "content": "Tariffs may increase our steel costs in Q2."},
            ],
            max_tokens=min(768, s.vllm_max_tokens),
            temperature=s.vllm_temperature,
            top_p=s.vllm_top_p,
            response_format=rf_eff,
            extra_body=extra,
        )
        raw3 = (resp3.choices[0].message.content or "").strip()
        obj3 = json.loads(raw3)
        if not obj3 or "mentions_tariffs" not in obj3:
            print(f"  WARN: weak/empty JSON from guided call: {raw3[:400]!r}")
            print("  Hint: start vLLM with a guided-decoding backend, or set USE_GUIDED_DECODING=0")
            return 3
        print(f"  OK keys: {list(obj3.keys())[:12]}... mentions_tariffs={obj3.get('mentions_tariffs')}")
    except APIStatusError as e:
        print(f"  FAIL HTTP {e.status_code}: {e.message}")
        return 3
    except json.JSONDecodeError as e:
        print(f"  FAIL JSON: {e} raw={raw3[:400]!r}")
        return 3

    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
