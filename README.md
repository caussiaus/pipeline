# PDF → Dataset Pipeline

A self-hosted, end-to-end tool for turning a folder of PDFs into a structured, LLM-extracted tabular dataset.  Designed for financial/ESG research on SEDAR+ and TSX filings, but generalizable to any corpus of documents.

## What it does

1. **Ingest** — scan a PDF root, deduplicate by MD5, filter to English, parse with [Docling](https://github.com/DS4SD/docling), chunk into semantically coherent passages.
2. **Extract** — run a two-pass LLM pipeline (chunk-level classification → document-level consolidation) against your own vLLM server.
3. **Build** — interactive schema designer lets you describe fields in natural language; the agent proposes, you refine, evidence is shown inline with source-PDF highlights.
4. **Export** — clean CSV / Parquet dataset ready for downstream analysis.

## Architecture

```
app.py                      Streamlit entry-point
app_pages/
  workspace.py              Main wizard (load → ingest → schema → extract → export)
  corpus_setup.py           Advanced corpus management
  build_dataset.py          n8n-style schema editor
  datasets.py               Browse & inspect saved datasets
  browse.py                 Raw document browser
  feedback.py               Human review / feedback
tariff_agent/
  corpus/                   CorpusConfig + runtime env overrides
  nodes/                    LangGraph pipeline nodes
  utils/
    chunking.py             Docling → ChunkRecord (with bounding boxes)
    docling_pipeline.py     PDF → structured JSON (crash-resilient)
    config.py               Pydantic settings (all env-overridable)
    pdf_evidence.py         Render highlighted evidence from source PDFs
    llm_client.py           OpenAI-compatible vLLM client
    ...
scripts/
  run_corpus_pipeline.py    CLI runner (supports --trial-n for small batches)
  prep_corpus_folder.py     Flatten, deduplicate, filter PDFs to a staging dir
  build_dataset.py          Headless dataset export
data/
  metadata/                 Filings indexes, issuer lists (checked in)
raw_data/
  criteria/                 Tariff/policy criteria documents
output/                     Generated per-corpus (git-ignored — regenerated)
```

## Quick start

### 1. Clone & install

```bash
git clone https://github.com/caussiaus/pipeline.git
cd pipeline
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
```

### 2. Configure

```bash
cp .env.example .env
# edit .env — set PDF roots, vLLM endpoint, HF_HOME, etc.
```

### 3. Run the app

```bash
streamlit run app.py
```

Open http://localhost:8501.  Use the **Workspace** page to load a PDF folder and walk through the full pipeline.

### 4. (Optional) Prepare a flat corpus

```bash
python scripts/prep_corpus_folder.py \
  --src "/mnt/c/Users/you/MyPDFs" \
  --out-dir output/corpus_flat \
  --manifest output/corpus_flat/manifest.csv
```

Deduplicates by MD5, filters to English, writes a flat folder.

### 5. Run the pipeline headlessly (trial)

```bash
python scripts/run_corpus_pipeline.py \
  --corpus output/corpus_configs/my_corpus.yaml \
  --stages all \
  --trial-n 10      # only the 10 smallest PDFs
```

## Requirements

- Python ≥ 3.10
- A running [vLLM](https://github.com/vllm-project/vllm) server (or any OpenAI-compatible endpoint)
- CUDA GPU recommended for Docling parsing (CPU mode works, slower)
- [Docling](https://github.com/DS4SD/docling) model weights pulled automatically on first run

## Deployment (Streamlit Community Cloud)

1. Push this repo to GitHub (see [Publishing](#publishing) below).
2. Go to [share.streamlit.io](https://share.streamlit.io) → **New app** → select `caussiaus/pipeline` → `app.py`.
3. Add your `.env` values as **Secrets** in the Streamlit dashboard (Settings → Secrets).
4. Share the app URL with your team; use Streamlit's viewer-email allowlist for access control.

> **Note:** Streamlit Community Cloud does not have GPU access. For full Docling + vLLM inference you'll need a self-hosted deployment (Docker / VM with GPU, or a cloud GPU instance).

## Publishing

```bash
git remote add origin https://github.com/caussiaus/pipeline.git
git push -u origin main
```

## Configuration reference

All settings live in `tariff_agent/utils/config.py` and are overridable via environment variables.  See `.env.example` for the full list with descriptions.

## License

MIT
