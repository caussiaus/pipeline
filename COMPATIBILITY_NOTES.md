# Compatibility notes

## Corrections applied (vs older scaffold / matrix drafts)

| Topic | Wrong | Correct | Why |
|--------|--------|---------|-----|
| **langchain-core pin** | `>=0.3.50,<0.4` (LangGraph 0.x era) | `>=1.2.21,<2` | LangGraph **1.1.6** resolves **langchain-core 1.2.x**; a 0.3.x upper bound will fail to install. |
| **langchain-openai** | Listed in requirements | **Omitted** | Current **langchain-openai** releases pull **openai ≥ 2.x**, which conflicts with **`openai>=1.68,<2`** used for the official AsyncOpenAI client against vLLM. |
| **opencv-python** | Unpinned (docling → 4.13+) | **`==4.10.0.84`** | OpenCV **4.13+** requires **numpy ≥ 2**; this project keeps **numpy 1.x** for docling/pandas stability. |
| **extra_body** | Only `guided_json` *or* only chat args | **Both allowed**: `guided_json` (when enabled) **and** `chat_template_kwargs` | vLLM’s OpenAI-compatible server forwards extra fields on **`extra_body`**; combine **guided JSON** with **`{"enable_thinking": false}`** for Qwen3 extraction. |
| **SqliteSaver** | Async checkpointer + sync `invoke` | **`SqliteSaver.from_conn_string(...)()` context manager + `graph.invoke`** | Match sync entrypoint; avoid **AsyncSqliteSaver** + `invoke` hang/mismatch. |
| **VLLM_BATCH_SIZE** | Client batch loop knob | **Removed** | Throughput = **`asyncio.gather`** + **semaphore** + vLLM continuous batching; concurrency is **`VLLM_MAX_CONCURRENT_REQUESTS`** (alias **`VLLM_CONCURRENCY`**). |

## Install order

```bash
pip install --force-reinstall torch torchvision --index-url https://download.pytorch.org/whl/cpu
pip install -r requirements.txt
python -c "import langgraph, openai, docling, pydantic; print('OK')"
```

## Recover if NumPy 2.x appears

```bash
pip install --force-reinstall "numpy>=1.26,<2" opencv-python==4.10.0.84
```

## Config knobs (env)

- **`VLLM_MAX_CONCURRENT_REQUESTS`** (alias **`VLLM_CONCURRENCY`**) — client-side parallelism.
- **`VLLM_CHAT_TEMPLATE_KWARGS`** — JSON merged into **`extra_body["chat_template_kwargs"]`**.
- **`USE_GUIDED_DECODING`** — when true, Pass 1/2 JSON schemas are also sent as **`extra_body["guided_json"]`**; if **`VLLM_RESPONSE_FORMAT`** would use **`json_schema`**, the client switches to **`json_object`** for `response_format` to avoid double-constraining the same request (server-dependent).
- **`CHECKPOINT_SQLITE_PATH`** — optional; enables sync **`SqliteSaver`** with **`run_full_pipeline`**.
