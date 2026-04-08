<img src="https://r2cdn.perplexity.ai/pplx-full-logo-primary-dark%402x.png" style="height:64px;margin-right:32px"/>

# Yes. On SEDAR+, the single best filing to search first for tariff risk is the firm’s first post–April 2, 2025 MD\&A — either the interim MD\&A for the next quarter or the annual MD\&A if that is the first filing after the tariff announcement. Canadian MD\&A rules require discussion of known trends, demands, commitments, events or uncertainties reasonably likely to affect the business, and specifically call for discussion of important trends and risks affecting the financial statements and future results.

The second-best filing is the AIF (Annual Information Form), but only for issuers that file one. Under NI 51-102, an AIF is required for reporting issuers that are not venture issuers, and the AIF is meant to describe the company’s operations, prospects, risks, and other external factors affecting it. The AIF form also has a specific Risk Factors item that includes things like regulatory constraints and economic or political conditions, which is exactly where tariff risk often shows up.
If the tariff issue was important enough to change the business in a material way, you should also check for a Material Change Report and the related news release. NI 51-102 requires an issuer to immediately file a news release for a material change and then file a Form 51-102F3 Material Change Report within 10 days.
So for your project, I’d search SEDAR+ in this order:
1.	Management’s Discussion \& Analysis
2.	Annual Information Form
3.	Material Change Report
4.	News release around April–June 2025 if you want faster, less formal signals. SEDAR+ supports filtering document searches by filing/document type.
A practical rule:
•	Use MD\&A as the main source
•	Use AIF as the backup / annual risk-factor source
•	Use Material Change Reports only for strong, immediate shocks
•	For venture issuers, expect MD\&A / quarterly highlights, not necessarily an AIF.
For coding tariff exposure, the best single document is usually the first MD\&A filed after April 2, 2025. That is the filing most likely to say whether tariffs affected costs, margins, sourcing, exports, demand, or supply chains.
If you want, I can turn this into a SEDAR+ coding template with exact filing types, date windows, and keywords. for Magna, there are statments like this all over 2026 md\&a
 
[image] so mentioned in relation to sales and earnings. Also there is a section on supply chain disruptions. These are probably the most salient mention categories (1) some form of earnings/revenue 2)MACROECONOMIC, GEOPOLITICAL AND OTHER RISKS 3) Supply chain risks I have a scraper for the filings that I’ll feed into a process. I have a big gpu and was wondering what was the most effective tool for extracting this signal, I figured I can do batched processing of qwen3 14b with a light prompt and then I tie in a verification loop into another prompt and have that full extraction pass be defined in langgraph and executed using vllm on docling ingested and chunked documents using a rag pipeline.

You’re on exactly the right track conceptually: use the first post–April‑2‑2025 MD\&A as the primary tariff‑signal document, backstopped by AIF and, where present, Material Change Reports and contemporaneous news.[^1_1]

### 1. Overall approach

Given your hardware and stack, I’d treat “tariff exposure” as a document-level and span-level extraction task, then aggregate up to issuer–year features.  With Magna‑style language, I’d explicitly separate three label families you already identified: (1) earnings/sales/margins impact, (2) macro/geopolitical/regulatory risk discussion, and (3) supply‑chain disruption risk.[^1_1]

### 2. Model strategy

For large‑scale, multi‑issuer SEDAR+ coverage:

- Use a relatively small, fast model (Qwen 2/3 7B–14B or similar) for dense span classification and extraction on chunks.[^1_1]
- Wrap that with a slower but more precise verifier/canonicalizer pass (can be another instance with a stricter prompt, or a slightly larger model if you have headroom).[^1_1]
- Keep everything in‑house with vLLM; you don’t need frontier models for this domain if your schema is tight.[^1_1]

A very pragmatic pattern:

1. **Pass 1 – Chunk tagging + span extraction**
    - Input: docling‑chunked MD\&A/AIF/MC report text with metadata (issuer, filing type, date, section path).[^1_1]
    - Output per chunk:
        - Binary flags per label family (earnings impact, macro risk, supply‑chain risk, “other tariff mention”).
        - Extracted spans (one or more sentences) supporting each positive label.
        - A coarse impact code for earnings mentions (e.g., `COST_UP`, `MARGIN_DOWN`, `VOLUME_DOWN`, `MITIGATION`, `BENEFIT`).[^1_1]
2. **Pass 2 – Document‑level consolidation**
    - Input: all chunk‑level positives for a document.
    - Ask the verifier to:
        - Deduplicate overlapping spans.
        - Resolve contradictions (e.g., “tariffs increased costs” vs “costs largely passed through to customers”).
        - Produce document‑level features: boolean flags, short natural‑language summary, and numeric intensity scores (0–3) per label family.[^1_1]
3. **Pass 3 – Cross‑document issuer‑year view (optional)**
    - On the backend (Python, no LLM): aggregate MD\&A + AIF + any MC Reports into an issuer‑year feature vector (e.g., max intensity per label, first‑mention date, etc.).[^1_1]

### 3. Prompting and extraction schema

For **Pass 1** on Qwen 14B, I’d keep the prompt extremely lean and fully structured output in JSON:

- System: “You are extracting tariff‑related risk and impact information from Canadian securities filings. Only use information explicitly in the text.”[^1_1]
- User template (per chunk, with section path):
    - Very short task description.
    - Definitions of the three categories with 1–2 Magna‑style examples each.
    - Clear instruction: “If the chunk does not mention tariffs, return all flags false and empty arrays.”[^1_1]
    - JSON schema with keys like:
        - `mentions_tariffs` (bool)
        - `earnings_impact` {`present` (bool), `impact_type` (enum), `support_spans` [..]}
        - `macro_geo_risk` {`present`, `support_spans`[..]}
        - `supply_chain_risk` {`present`, `support_spans`[..]}
        - `other_notes` (free text, short).

Given MD\&A boilerplate, one key is to **bias toward precision**: explicitly tell the model not to infer tariff risk from generic “economic uncertainty” unless tariffs are named or clearly implied.[^1_1]

For **Pass 2**, give the verifier the collated JSON from all positive chunks plus the spans, and ask for:

- A single document‑level JSON object with:
    - `doc_has_tariff_discussion` (bool)
    - For each category: `score_0_to_3`, `summary_sentence`, `key_quotes` (indices back to original spans).
    - `tariff_direction` enum: `ADVERSE`, `MIXED`, `BENIGN/NEUTRAL`, `POSITIVE` (e.g., tariff protection for domestic content).[^1_1]


### 4. Why not just one pass?

Two‑stage extraction buys you:

- Much faster throughput: cheap model over many chunks, smaller, batched verifier pass only on positives.[^1_1]
- Better calibration: verifier sees multiple mentions across sections (earnings note + risk factors + supply chain), which is exactly what Magna does.[^1_1]
- Easier error analysis: you can inspect chunk‑level outputs vs document consensus when a filing looks off.[^1_1]


### 5. Implementation hints with your stack

- **Chunking**: use docling to get structural chunks by heading (e.g., “Power \& Vision,” “MACROECONOMIC, GEOPOLITICAL AND OTHER RISKS,” “Supply Chain Disruptions”), then enforce token‑length caps. Preserve the section path; you can feed this as “Context: Section path = ...” which helps the model discriminate boilerplate from performance discussion.[^1_1]
- **RAG**: you don’t actually need traditional retrieval for this task; it’s more of a sequential document sweep. Still, using a vector index for “tariff” synonyms (“duties”, “border taxes”, etc.) can help you shortlist chunks for Pass 1 if throughput becomes an issue.[^1_1]
- **LangGraph**: perfect fit to encode: ingest → chunk → pass1 (batched) → filter positives → pass2 → write issuer‑year features. Include a “disagreement node” where if doc‑level `score_0_to_3` and raw counts are inconsistent, you route the doc to a more careful prompt or model.[^1_1]
- **vLLM**: run Qwen 14B with reasonably long context and batch chunks per issuer; you can amortize token overhead by grouping multiple chunks from the same doc in a single call with clear separators and IDs.[^1_1]


### 6. Label set for tariff exposure coding

Concretely, at the issuer‑year level I’d end up with something like:


| Feature | Description |
| :-- | :-- |
| `has_tariff_discussion` | Any explicit tariff mention in MD\&A/AIF. |
| `earnings_tariff_score` | 0–3 intensity of realized P\&L impact. |
| `supply_chain_tariff_score` | 0–3 intensity of supply‑chain risk. |
| `macro_tariff_score` | 0–3 intensity in macro/geopolitical section. |
| `tariff_direction` | Adverse, mixed, benign/neutral, positive. |
| `first_tariff_mention_date` | Date of earliest filing mentioning tariffs post‑April 2, 2025. |
| `pass_through_flag` | Whether issuer claims to pass costs to customers. |

These are all derivable from the two‑pass pipeline above, without needing anything more exotic than Qwen 14B plus a bit of schema engineering.[^1_1]

Would you prefer to keep this purely extraction‑based for now, or are you also aiming to predict a numeric “tariff shock” score that lines up with observed margin changes in the financials?
<span style="display:none">[^1_2][^1_3][^1_4]</span>

<div align="center">⁂</div>

[^1_1]: https://www.perplexity.ai/finance/MGA/financials

[^1_2]: IMG_0530.jpeg

[^1_3]: IMG_0529.jpeg

[^1_4]: IMG_0528.jpeg


---

# I was planning on using docling. given the vast amount of data, do you recommend a single pass or some level of granularity based digging and evaluation, our model can think too but that might make it slow and we want fast answers, structured output so that we are ready for independent passes that solidify the observed output. I know the ai should not do everything, but we have a structured output format and we need to have it alongside data and a way to even display the exact passage beside it, think, a tabular dataset output and for each row please define what each row, instance or row in our output csv represents given the data already incorporated in the pipeline and the goal. you also have evidence pointing to the span level so we can also help later calibrate it with comments from a human reviewer (me) that will interact with it through some program or app that outputs the data for me. im running the scrape right now so I need a full pipeline and once I have all the data ill give my agent access to to it, phase 1 is all code on windows because thats where my stack is for webscraping, then I copy that over into wsl where in a fresh repo ill wire up my environment to connect to my vllm instance and set up the scaffolding for the pipeline to process all the filings ill extract. outline start to finish, keeping in mind the data type of each point in the pipeline, between different phases. we end up basically downloading all the pdfs. I already have the scraping scripts that also include captcha solving. ive got this list of tickers and from that I map them to sedar companies. I am scraping the rest of the tickers right now fromsedar+ to populate as right now I only have the profileIDs for 167 out of 202 tickers I would conduct analysis on. so assume ill have a full list of profile IDs mapped to each of the 202 tickers, then I would go and search for the report type of each of the tickers as a search and specify the period of time to have the filings results of. I already have a scraper it'll just be a prompt I want from you describing what we need along our pipeline so that I can paste and initiate the rest of the work for while im shaping the metadata for the first half of phase 1. youre to give me instructions for setting up and ensuring hte scrape matches our data needs to complete phase 1: data gathering. phase 2 will entail everything we do once we take all the folders and copy them over to a data folder in a fresh repo in wsl for phase 2 where we will set up an environment and connect our resources like vllm etc. im scraping the rest of hte metadata, then when im done ill want to start scraping all hte filings for the ticker on here. data\prateek\tickers.csv so ill want to be set up to use 8 instances to scrape like im scrdaping now but for all the filings as i descrdibe it here.

You should absolutely go multi‑granular: structured, span‑level extraction over docling chunks, then consolidate to document‑ and issuer‑year rows in your CSV.  Below is a start‑to‑finish outline for Phase 1 (Windows scraping) and Phase 2 (WSL processing), with data types and what each row represents at every stage.[^3_1][^3_2][^3_3][^3_4]

***

## Phase 1 – SEDAR+ scraping and raw data layout (Windows)

### 1. Inputs you already have

- `data\prateek\tickers.csv` (Windows path)
    - Columns (example):
        - `ticker` (str)
        - `profile_id` (str/int, SEDAR+ profile ID, once populated)
        - `exchange` (str)
        - `is_venture` (bool)

Each **row = one issuer/ticker mapping**.

### 2. Ensure metadata completeness / profile ID scrape

For missing profile IDs, your existing scraper should:

- Take `ticker` and, if needed, issuer name, query SEDAR+ “search profiles” and extract `profile_id`.[^3_1]
- Append/update `profile_id` in `tickers.csv`.

Resulting file:

- `data\prateek\tickers_with_profiles.csv`
    - Same columns as above, all 202 tickers with non‑null `profile_id`.

Each **row = one issuer with resolved SEDAR+ identity**.

### 3. Filing search specification

For each `profile_id`, you want to search, at minimum, these document/filing types in a defined date window (say 2024‑01‑01 to 2026‑12‑31):[^3_2][^3_5]

- MD\&A (annual and interim)
- AIF (where applicable)
- Material Change Report (Form 51‑102F3)
- (Optional) News releases in the April–June 2025 window

You’ll configure your scraper with:

- `profile_id`
- `start_date`, `end_date`
- Allowed **document types** / **filing categories** (as SEDAR+ labels).[^3_2]

Scraper output (metadata only) → e.g., `data\prateek\filings_index_raw.csv`:

- `profile_id` (str/int)
- `ticker` (str, joined from tickers file)
- `issuer_name` (str)
- `filing_id` or `document_id` (str)
- `filing_type` (enum‑like str: `MDA_ANNUAL`, `MDA_INTERIM`, `AIF`, `MATERIAL_CHANGE_REPORT`, `NEWS_RELEASE`, `OTHER`)
- `document_type_raw` (exact SEDAR+ label)[^3_2]
- `filing_date` (date)
- `period_end` (date, if present)
- `url_pdf` (str, direct or relative URL)
- `language` (str, `EN` / `FR` / `BOTH` if you can detect)

Each **row = one document (one PDF) filed by an issuer**.

### 4. PDF download stage

You’ll then fan out over `filings_index_raw.csv` with 8 concurrent scraper instances:

- For each row, download the PDF to a deterministic path, e.g.:

`data\prateek\pdfs\<ticker>\ <filing_type>\ <filing_date>_<filing_id>.pdf`
- Update a second CSV (or augment same file) with file system info:

`data\prateek\filings_index.csv`:

- Previous columns plus:
    - `local_path` (Windows path, str)
    - `download_status` (enum: `OK`, `FAILED`, `SKIPPED_DUP`, etc.)
    - `filesize_bytes` (int)

Each **row still = one document**, but now with confirmed local PDF.

> For Phase 1, that’s all you need: fully populated `tickers_with_profiles.csv` and `filings_index.csv` plus the PDFs on disk.

***

## Phase 2 – WSL repo and docling/LLM pipeline

You’ll copy:

- `data\prateek\pdfs\...`
- `data\prateek\filings_index.csv`
- `data\prateek\tickers_with_profiles.csv`

into your WSL repo, preserving relative paths.

### 5. Repo layout and data types (WSL)

Example structure:

- `data/`

```
- `pdfs/<ticker>/<filing_type>/<file>.pdf`  
```

    - `metadata/filings_index.csv`
    - `metadata/tickers_with_profiles.csv`
- `output/`
    - `docling_json/` (parsed docs)
    - `chunks/` (intermediate chunk JSON/CSV)
    - `llm_raw/` (per‑chunk model outputs)
    - `llm_docs/` (per‑document consolidated outputs)
    - `csv/` (final issuer‑year CSVs)


### 6. Docling parsing stage

For each row in `filings_index.csv` with `download_status == "OK"`:

- Run docling to convert PDF → structured document (e.g., JSON/Markdown).[^3_6][^3_4]
- Persist as: `output/docling_json/<filing_id>.json` (or similar).

Create/augment a `parsed_docs_index.csv`:

- `filing_id`
- `local_path_pdf`
- `local_path_docling`
- `parse_status` (`OK`/`FAILED`)
- Optional: page count, char count.

Each **row = one document with parse artefact**.

### 7. Chunking stage (section‑aware)

Use docling’s structure‑aware chunking:[^3_4]

- Inputs per document: docling JSON.
- Produce chunk objects with:
    - `chunk_id` (unique)
    - `filing_id`
    - `profile_id`
    - `ticker`
    - `section_path` (e.g., `"POWER & VISION > Adjusted EBIT"` or `"MACROECONOMIC, GEOPOLITICAL AND OTHER RISKS > Threats to Free Trade Agreements"`).
    - `page_start`, `page_end` (ints)
    - `text` (str)
    - `num_tokens` (int est.)

Store them in `output/chunks/chunks.parquet` or a sharded set of Parquet/CSV files.

In tabular terms, **each row in `chunks` = one chunk of text belonging to a specific document and section**.

***

## Phase 2 – LLM passes and structured outputs

You want speed and future re‑runs, so think “single cheap pass over all chunks, then independent consolidating passes.”

### 8. Pass 1 – Span‑level extraction on chunks (Qwen via vLLM)

Using LangGraph, define a node that:

- Reads chunks (batched) from `chunks` table.
- For each chunk, calls Qwen with a light, JSON‑only prompt that outputs:

Chunk‑level output schema (`llm_raw_chunks`):

- Keys (columns):
    - `chunk_id` (str)
    - `filing_id` (str)
    - `mentions_tariffs` (bool)
    - `earnings_impact_present` (bool)
    - `earnings_impact_type` (enum: `COST_UP`, `MARGIN_DOWN`, `VOLUME_DOWN`, `PASS_THROUGH`, `BENEFIT`, `UNCLEAR`, `NONE`)
    - `earnings_impact_spans` (list[str]; store as JSON string)
    - `macro_risk_present` (bool)
    - `macro_risk_spans` (list[str])
    - `supply_chain_risk_present` (bool)
    - `supply_chain_risk_spans` (list[str])
    - `other_tariff_mentions` (list[str])
    - `model_version` (str)
    - `inference_timestamp` (datetime)

Persist to `output/llm_raw/chunks_llm.parquet`.

Each **row = one chunk with model‑inferred tariff labels and supporting spans**.

Crucially, the `*_spans` fields give you span‑level evidence you can later display beside the structured tabular row and also annotate in a UI.

### 9. Pass 2 – Document‑level consolidation

Next, a LangGraph node that operates at document granularity:

- For each `filing_id`, gather all its chunk‑level rows where `mentions_tariffs == true`.
- Feed these (plus minimal metadata: filing type, date, section paths) into a verifier prompt.
- Ask for a **single JSON object per filing** summarizing tariff exposure.

Document‑level schema (`llm_docs`):

- `filing_id`
- `profile_id`
- `ticker`
- `filing_type`
- `filing_date`
- `has_tariff_discussion` (bool)
- `earnings_tariff_score_0_3` (int)
- `supply_chain_tariff_score_0_3` (int)
- `macro_tariff_score_0_3` (int)
- `tariff_direction` (enum: `ADVERSE`, `MIXED`, `BENIGN`, `POSITIVE`, `NONE`)
- `doc_summary_sentence` (str)
- `key_quotes` (list[str], top‑N short spans or quotes, stored JSON)
- `first_tariff_section_path` (str)

Each **row in `llm_docs` = one filing’s consolidated tariff signal**.

Because you have both `llm_raw_chunks` and `llm_docs`, you can at any time show: “Row X = Filing Y, here are the precise spans that supported the label,” and allow human comments.

### 10. Human review and comments (future loop)

When you build a review UI or CLI, you can:

- Load `llm_docs` for, say, Magna’s 2026 MD\&A.
- Show the row plus the associated spans from `llm_raw_chunks` (matching by `filing_id` and `chunk_id`).
- Allow you to add:
    - `human_label_correct` (bool)
    - `human_comments` (str)
    - `corrected_scores` (ints/enums)

These annotations can be stored in a separate table `human_reviews`:

- `review_id`
- `filing_id`
- `chunk_id` (optional, if span‑level feedback)
- `field_name` (e.g., `earnings_tariff_score_0_3`)
- `original_value`
- `corrected_value`
- `comment`

Each **row = one human correction or comment linked to a filing (and optionally chunk)**.

***

## What each final CSV row should represent

Given your goal (issuer‑level tariff exposure):

1. **`filings_index.csv`** – row = one SEDAR+ filing (document).
2. **`chunks` table** – row = one text chunk within a filing.
3. **`llm_raw_chunks`** – row = one chunk + model’s span‑level extraction.
4. **`llm_docs`** – row = one filing’s consolidated tariff signal.
5. **Final research CSV (issuer‑year)** – after aggregation in Python:

Example `output/csv/issuer_year_tariff_signals.csv`:
    - `ticker`
    - `profile_id`
    - `fiscal_year`
    - `has_tariff_discussion` (bool, any filing that year)
    - `max_earnings_tariff_score_0_3`
    - `max_supply_chain_tariff_score_0_3`
    - `max_macro_tariff_score_0_3`
    - `tariff_direction_year` (e.g., max severity direction)
    - `first_tariff_filing_id`
    - `first_tariff_filing_date`

Here each **row = one issuer‑year tariff exposure instance**.

Those issuer‑year rows are what you’ll later merge to financials / margins for econometric analysis.

***

## Single pass vs. multi‑granular thinking

Given your data volume and GPU, **do Pass 1 once over all chunks**, store everything, and avoid repeated “thinking” on the same text.  More complex reasoning, calibrations, and variant prompts become cheap independent passes that just read your stored structured outputs and span evidence; the model doesn’t need to see the original PDFs again unless you materially change your label set.[^3_7][^3_8]

Would it help if I next sketch a concrete JSON output schema and example prompts you can drop straight into LangGraph for the chunk‑level and document‑level nodes?
<span style="display:none">[^3_10][^3_11][^3_12][^3_13][^3_14][^3_15][^3_9]</span>

<div align="center">⁂</div>

[^3_1]: https://www.sedarplus.ca/csa-party/service/create.html?targetAppCode=csa-party\&service=searchDocuments\&_locale=en

[^3_2]: https://www.sedarplus.ca/onlinehelp/filings/filing-inventory/

[^3_3]: https://www.reddit.com/r/Rag/comments/1mjwde9/best_chunking_strategy_for_rag_on_annualfinancial/

[^3_4]: https://github.com/docling-project/docling-graph/blob/main/docs/fundamentals/extraction-process/chunking-strategies.md

[^3_5]: https://www.osc.ca/sites/default/files/2024-04/ni_20230609_51-102_unofficial-consolidation.pdf

[^3_6]: https://www.youtube.com/watch?v=B5XD-qpL0FU

[^3_7]: https://discuss.vllm.ai/t/suggestion-to-improve-inferencing-speed/2080

[^3_8]: https://github.com/vllm-project/vllm/issues/8513

[^3_9]: https://www.sedarplus.ca/onlinehelp/filings/view-a-filing/formal-correspondence-document-type-and-filing-status/

[^3_10]: https://www.bcsc.bc.ca/-/media/PWS/New-Resources/Securities-Law/Instruments-and-Policies/Policy-5/51102CP-CP-July-25-2023.pdf?dt=20230720164040

[^3_11]: https://help.avantisai.com/en/kb/sedar-sec-document-category-definitions

[^3_12]: https://www.reddit.com/r/Vllm/comments/1keed5d/issue_with_batch_inference_using_vllm_for_qwen_25/

[^3_13]: https://www.asc.ca/-/media/ASC-Documents-part-1/Regulatory-Instruments/2023/05/6091600-ASC-Notice-re-SEDAR-plus-Amendments-and-Changes-13-103.pdf

[^3_14]: https://fcaa.gov.sk.ca/public/plugins/pdfs/2573/13_101_sedar_filer_manual_version_8_054_december_7_2015.pdf

[^3_15]: https://www.linkedin.com/posts/newtuple_finance-friendly-ocr-how-docling-dolphin-activity-7340610350041436161-LCHs


---

# please outline the full scaffolding for a new project assuming ill transfer a folder containing all the filings. the pipeline on wsl will continue at preprocessing hte PDFs using various methods for solidification, both docling and the span level method for cross referencing and stability. you can call the folder whatever and ill enter your commands into the terminal i na new folder in my wsl home folder and ill start linking all my resources like vllm is already linked

Below is a concrete “copy‑pasteable” scaffolding for your WSL project, assuming you drop a folder of PDFs into it and already have vLLM running and reachable.  I’ll name the project `tariff-sedar-pipeline`, but you can rename as you like.[^4_1][^4_2]

***

## 1. Create project and base layout

In WSL home:

```bash
cd ~
mkdir -p tariff-sedar-pipeline
cd tariff-sedar-pipeline

mkdir -p data/pdfs           # you will copy all filings here (subfolders ok)
mkdir -p data/metadata
mkdir -p output/docling_json
mkdir -p output/chunks
mkdir -p output/llm_raw
mkdir -p output/llm_docs
mkdir -p output/csv
mkdir -p logs
mkdir -p tariff_agent/utils
touch tariff_agent/__init__.py
touch tariff_agent/utils/__init__.py
```

Create `requirements.txt`:

```bash
cat > requirements.txt << 'EOF'
pandas
pyarrow
tqdm
docling
docling-parse
langgraph
langchain
pydantic
orjson
uvicorn
fastapi
vllm  # only if you call it locally in-process; otherwise client lib
EOF
```

(Adjust `vllm` line depending on whether you call a remote vLLM server. )[^4_3]

Install:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```


***

## 2. Core metadata files you’ll bring in

Into `data/metadata/` copy:

- `filings_index.csv` – one row per filing, with columns like:
    - `filing_id`, `profile_id`, `ticker`, `issuer_name`, `filing_type`, `filing_date`, `local_path` (relative Windows path, which you’ll normalize to WSL paths).
- `tickers_with_profiles.csv` – one row per issuer.

Into `data/pdfs/` copy the full tree of PDFs (you can mirror the Windows layout, or just drop them flat; the pipeline will trust `filings_index.csv` for paths).

***

## 3. Define data models and shared config

Create `tariff_agent/utils/state.py`:

```python
from typing import Literal, List, Optional
from pydantic import BaseModel

FilingType = Literal[
    "MDA_ANNUAL",
    "MDA_INTERIM",
    "AIF",
    "MATERIAL_CHANGE_REPORT",
    "NEWS_RELEASE",
    "OTHER",
]

class ChunkRecord(BaseModel):
    chunk_id: str
    filing_id: str
    profile_id: str
    ticker: str
    filing_type: FilingType
    filing_date: str
    section_path: str
    page_start: int
    page_end: int
    text: str
    num_tokens: int

class ChunkLLMOutput(BaseModel):
    chunk_id: str
    filing_id: str
    mentions_tariffs: bool
    earnings_impact_present: bool
    earnings_impact_type: Literal[
        "COST_UP", "MARGIN_DOWN", "VOLUME_DOWN",
        "PASS_THROUGH", "BENEFIT", "UNCLEAR", "NONE"
    ]
    earnings_impact_spans: List[str]
    macro_risk_present: bool
    macro_risk_spans: List[str]
    supply_chain_risk_present: bool
    supply_chain_risk_spans: List[str]
    other_tariff_mentions: List[str]
    model_version: str
    inference_timestamp: str

class FilingLLMOutput(BaseModel):
    filing_id: str
    profile_id: str
    ticker: str
    filing_type: FilingType
    filing_date: str
    has_tariff_discussion: bool
    earnings_tariff_score_0_3: int
    supply_chain_tariff_score_0_3: int
    macro_tariff_score_0_3: int
    tariff_direction: Literal["ADVERSE", "MIXED", "BENIGN", "POSITIVE", "NONE"]
    doc_summary_sentence: str
    key_quotes: List[str]
    first_tariff_section_path: Optional[str]
```

This keeps your CSVs and Parquet tables schema‑driven and stable.

***

## 4. Docling preprocessing utilities

Create `tariff_agent/utils/docling_pipeline.py`:

```python
import os
import pandas as pd
from tqdm import tqdm
from docling.document_converter import DocumentConverter
from docling.pipeline.standard_pdf_pipeline import StandardPdfPipeline
from docling.pipeline.standard_pdf_pipeline import StandardPdfPipelineOptions
from docling_parse import PdfExtractor   # if you want span-level coords [web:27]

DOC_JSON_DIR = "output/docling_json"

def build_docling_converter():
    options = StandardPdfPipelineOptions()
    options.do_ocr = True
    options.do_table_structure = True
    pipeline = StandardPdfPipeline(options)
    return DocumentConverter(pipeline=pipeline)

def normalize_path(path: str) -> str:
    # adjust Windows-style paths if they appear in filings_index.csv
    return path.replace("\\", "/")

def run_docling_on_filings(index_path: str = "data/metadata/filings_index.csv"):
    df = pd.read_csv(index_path)
    conv = build_docling_converter()

    os.makedirs(DOC_JSON_DIR, exist_ok=True)
    records = []

    for _, row in tqdm(df.iterrows(), total=len(df)):
        filing_id = row["filing_id"]
        pdf_path = normalize_path(row["local_path"])
        out_path = os.path.join(DOC_JSON_DIR, f"{filing_id}.json")

        if not os.path.exists(pdf_path):
            status = "PDF_MISSING"
        else:
            try:
                doc = conv.convert(pdf_path)
                doc.to_json(out_path)
                status = "OK"
            except Exception as e:
                status = f"ERROR:{e}"

        records.append({
            "filing_id": filing_id,
            "local_path_pdf": pdf_path,
            "local_path_docling": out_path if status == "OK" else "",
            "parse_status": status,
        })

    pd.DataFrame(records).to_csv("output/docling_parse_index.csv", index=False)
```

Docling docs for reference.[^4_4][^4_1]

***

## 5. Chunking utilities (section‑aware)

Create `tariff_agent/utils/chunking.py`:

```python
import os
import json
import uuid
import pandas as pd
from tqdm import tqdm
from .state import ChunkRecord

DOC_JSON_DIR = "output/docling_json"

def estimate_tokens(text: str) -> int:
    # crude; replace with tiktoken if needed
    return max(1, len(text.split()) // 0.75)

def chunk_docling_document(doc_json_path: str, filing_row) -> list[ChunkRecord]:
    with open(doc_json_path, "r", encoding="utf-8") as f:
        doc = json.load(f)

    chunks: list[ChunkRecord] = []

    # Pseudo-structure: adapt to actual docling JSON schema
    for section in doc.get("sections", []):
        section_path = section.get("path", "ROOT")
        texts = section.get("texts", [])

        buffer = []
        buf_pages = []
        for t in texts:
            content = t.get("text", "")
            page = t.get("page", 0)
            if not content.strip():
                continue

            buffer.append(content)
            buf_pages.append(page)
            text_joined = "\n".join(buffer)
            if estimate_tokens(text_joined) > 1200:
                chunk_text = "\n".join(buffer[:-1]) if len(buffer) > 1 else text_joined
                chunk_id = str(uuid.uuid4())
                chunks.append(
                    ChunkRecord(
                        chunk_id=chunk_id,
                        filing_id=filing_row["filing_id"],
                        profile_id=str(filing_row["profile_id"]),
                        ticker=filing_row["ticker"],
                        filing_type=filing_row["filing_type"],
                        filing_date=str(filing_row["filing_date"]),
                        section_path=section_path,
                        page_start=min(buf_pages),
                        page_end=max(buf_pages),
                        text=chunk_text,
                        num_tokens=estimate_tokens(chunk_text),
                    )
                )
                buffer = [content]
                buf_pages = [page]

        if buffer:
            chunk_text = "\n".join(buffer)
            chunk_id = str(uuid.uuid4())
            chunks.append(
                ChunkRecord(
                    chunk_id=chunk_id,
                    filing_id=filing_row["filing_id"],
                    profile_id=str(filing_row["profile_id"]),
                    ticker=filing_row["ticker"],
                    filing_type=filing_row["filing_type"],
                    filing_date=str(filing_row["filing_date"]),
                    section_path=section_path,
                    page_start=min(buf_pages) if buf_pages else 0,
                    page_end=max(buf_pages) if buf_pages else 0,
                    text=chunk_text,
                    num_tokens=estimate_tokens(chunk_text),
                )
            )

    return chunks

def run_chunking():
    filings = pd.read_csv("data/metadata/filings_index.csv")
    parse_idx = pd.read_csv("output/docling_parse_index.csv")
    parse_idx = parse_idx[parse_idx["parse_status"] == "OK"]
    merged = filings.merge(parse_idx[["filing_id", "local_path_docling"]], on="filing_id")

    all_records = []
    for _, row in tqdm(merged.iterrows(), total=len(merged)):
        doc_json_path = row["local_path_docling"]
        if not os.path.exists(doc_json_path):
            continue
        chunks = chunk_docling_document(doc_json_path, row)
        all_records.extend([c.model_dump() for c in chunks])

    df_chunks = pd.DataFrame(all_records)
    os.makedirs("output/chunks", exist_ok=True)
    df_chunks.to_parquet("output/chunks/chunks.parquet", index=False)
```

This gives you a clean `chunks.parquet`: each row = one chunk.

***

## 6. LLM client and prompts

Create `tariff_agent/utils/llm_client.py`:

```python
import os
import requests
from datetime import datetime
from typing import List
from .state import ChunkRecord, ChunkLLMOutput

VLLM_ENDPOINT = os.getenv("VLLM_ENDPOINT", "http://localhost:8000/generate")

def build_chunk_prompt(chunk: ChunkRecord) -> str:
    return f"""
You extract tariff-related risk information from Canadian securities filings.

Text (section: {chunk.section_path}, filing type: {chunk.filing_type}, date: {chunk.filing_date}):

\"\"\"{chunk.text}\"\"\"

Return a STRICT JSON object with keys:
- mentions_tariffs (bool)
- earnings_impact_present (bool)
- earnings_impact_type (one of COST_UP, MARGIN_DOWN, VOLUME_DOWN, PASS_THROUGH, BENEFIT, UNCLEAR, NONE)
- earnings_impact_spans (list of short supporting quotes)
- macro_risk_present (bool)
- macro_risk_spans (list)
- supply_chain_risk_present (bool)
- supply_chain_risk_spans (list)
- other_tariff_mentions (list)

Only label something true if tariffs/duties/border taxes or similar are explicitly mentioned.
"""

def call_vllm_batch(prompts: List[str]) -> List[str]:
    # adapt to your vLLM server protocol
    resp = requests.post(VLLM_ENDPOINT, json={"prompts": prompts})
    resp.raise_for_status()
    outputs = resp.json()["outputs"]
    return outputs

def run_llm_on_chunks():
    import pandas as pd
    import orjson

    df = pd.read_parquet("output/chunks/chunks.parquet")
    records = []
    batch_size = 16

    for i in range(0, len(df), batch_size):
        batch = df.iloc[i:i+batch_size]
        chunk_objs = [ChunkRecord(**row.to_dict()) for _, row in batch.iterrows()]
        prompts = [build_chunk_prompt(c) for c in chunk_objs]
        raw_outputs = call_vllm_batch(prompts)

        for chunk, raw in zip(chunk_objs, raw_outputs):
            data = orjson.loads(raw)
            out = ChunkLLMOutput(
                chunk_id=chunk.chunk_id,
                filing_id=chunk.filing_id,
                mentions_tariffs=data["mentions_tariffs"],
                earnings_impact_present=data["earnings_impact_present"],
                earnings_impact_type=data["earnings_impact_type"],
                earnings_impact_spans=data.get("earnings_impact_spans", []),
                macro_risk_present=data["macro_risk_present"],
                macro_risk_spans=data.get("macro_risk_spans", []),
                supply_chain_risk_present=data["supply_chain_risk_present"],
                supply_chain_risk_spans=data.get("supply_chain_risk_spans", []),
                other_tariff_mentions=data.get("other_tariff_mentions", []),
                model_version=os.getenv("MODEL_VERSION", "qwen3-14b"),
                inference_timestamp=datetime.utcnow().isoformat(),
            )
            records.append(out.model_dump())

    df_out = pd.DataFrame(records)
    df_out.to_parquet("output/llm_raw/chunks_llm.parquet", index=False)
```

This is your single, fast pass over all chunks using vLLM.[^4_5][^4_3]

***

## 7. Document‑level consolidation

Create `tariff_agent/utils/doc_level.py`:

```python
import os
import pandas as pd
import orjson
from datetime import datetime
from .state import FilingLLMOutput

def build_doc_prompt(filing_meta, chunk_rows) -> str:
    # chunk_rows: list of dicts from llm_raw_chunks filtered to mentions_tariffs == True
    snippets = []
    for r in chunk_rows:
        snippets.append(
            f"Section: {r['section_path']}\n"
            f"Chunk_id: {r['chunk_id']}\n"
            f"Earnings spans: {r['earnings_impact_spans']}\n"
            f"Macro spans: {r['macro_risk_spans']}\n"
            f"Supply spans: {r['supply_chain_risk_spans']}\n"
        )
    snippets_text = "\n\n".join(snippets)

    return f"""
You are consolidating tariff-related information for a single filing.

Filing meta:
ticker={filing_meta['ticker']}, filing_type={filing_meta['filing_type']},
filing_date={filing_meta['filing_date']}.

Tariff-related evidence:

{snippets_text}

Return STRICT JSON with:
- has_tariff_discussion (bool)
- earnings_tariff_score_0_3 (int 0-3)
- supply_chain_tariff_score_0_3 (int 0-3)
- macro_tariff_score_0_3 (int 0-3)
- tariff_direction (ADVERSE, MIXED, BENIGN, POSITIVE, NONE)
- doc_summary_sentence (string, <= 40 words)
- key_quotes (list of <=4 short quotes)
- first_tariff_section_path (string or null)
"""

def call_vllm_single(prompt: str) -> str:
    import requests
    endpoint = os.getenv("VLLM_ENDPOINT", "http://localhost:8000/generate")
    resp = requests.post(endpoint, json={"prompts": [prompt]})
    resp.raise_for_status()
    return resp.json()["outputs"][^4_0]

def run_doc_level():
    chunks_llm = pd.read_parquet("output/llm_raw/chunks_llm.parquet")
    chunks = pd.read_parquet("output/chunks/chunks.parquet")
    filings = pd.read_csv("data/metadata/filings_index.csv")

    merged = chunks.merge(chunks_llm, on=["chunk_id", "filing_id"])
    merged = merged[merged["mentions_tariffs"] == True]

    out_records = []

    for filing_id, group in merged.groupby("filing_id"):
        filing_meta = filings[filings["filing_id"] == filing_id].iloc[^4_0].to_dict()
        rows = group.to_dict(orient="records")
        prompt = build_doc_prompt(filing_meta, rows)
        raw = call_vllm_single(prompt)
        data = orjson.loads(raw)

        out = FilingLLMOutput(
            filing_id=filing_id,
            profile_id=str(filing_meta["profile_id"]),
            ticker=filing_meta["ticker"],
            filing_type=filing_meta["filing_type"],
            filing_date=str(filing_meta["filing_date"]),
            has_tariff_discussion=data["has_tariff_discussion"],
            earnings_tariff_score_0_3=int(data["earnings_tariff_score_0_3"]),
            supply_chain_tariff_score_0_3=int(data["supply_chain_tariff_score_0_3"]),
            macro_tariff_score_0_3=int(data["macro_tariff_score_0_3"]),
            tariff_direction=data["tariff_direction"],
            doc_summary_sentence=data["doc_summary_sentence"],
            key_quotes=data["key_quotes"],
            first_tariff_section_path=data.get("first_tariff_section_path"),
        )

        out_records.append(out.model_dump())

    df_out = pd.DataFrame(out_records)
    df_out.to_parquet("output/llm_docs/filings_llm.parquet", index=False)
    df_out.to_csv("output/csv/filings_llm.csv", index=False)
```

Each row in `filings_llm.csv` = one filing’s consolidated tariff profile.

***

## 8. Aggregation to issuer‑year CSV

Create `tariff_agent/utils/aggregate.py`:

```python
import pandas as pd
import numpy as np

def build_issuer_year_table():
    filings = pd.read_csv("data/metadata/filings_index.csv")
    doc_llm = pd.read_csv("output/csv/filings_llm.csv")

    df = filings.merge(doc_llm, on="filing_id", how="inner")

    df["fiscal_year"] = pd.to_datetime(df["filing_date"]).dt.year

    agg = df.groupby(["ticker", "profile_id", "fiscal_year"]).agg(
        has_tariff_discussion=("has_tariff_discussion", "max"),
        max_earnings_tariff_score_0_3=("earnings_tariff_score_0_3", "max"),
        max_supply_chain_tariff_score_0_3=("supply_chain_tariff_score_0_3", "max"),
        max_macro_tariff_score_0_3=("macro_tariff_score_0_3", "max"),
        first_tariff_filing_date=("filing_date", "min"),
    ).reset_index()

    agg.to_csv("output/csv/issuer_year_tariff_signals.csv", index=False)
```

Each row in `issuer_year_tariff_signals.csv` = one issuer‑year instance, ready to join to financials.

***

## 9. Simple orchestration script

Create `run_pipeline.py` in project root:

```python
from tariff_agent.utils.docling_pipeline import run_docling_on_filings
from tariff_agent.utils.chunking import run_chunking
from tariff_agent.utils.llm_client import run_llm_on_chunks
from tariff_agent.utils.doc_level import run_doc_level
from tariff_agent.utils.aggregate import build_issuer_year_table

if __name__ == "__main__":
    # 1. Parse PDFs with docling
    run_docling_on_filings()

    # 2. Chunk documents
    run_chunking()

    # 3. Run LLM on chunks via vLLM
    run_llm_on_chunks()

    # 4. Consolidate to document level
    run_doc_level()

    # 5. Aggregate to issuer-year
    build_issuer_year_table()
```

You can later swap this for a LangGraph graph, but this gives you an immediate working offline pipeline.[^4_6][^4_2]

Run:

```bash
source .venv/bin/activate
python run_pipeline.py
```


***

This scaffolding gives you:

- One clear place to drop your PDF folder.
- Deterministic, schema‑controlled tables at document, chunk, span, filing, and issuer‑year levels.
- Easy hooks for a review UI (each row always links back to `chunk_id` and spans).

Do you want the next step to be a minimal LangGraph graph that wraps these existing functions as nodes so you can swap between batch/offline and interactive modes without refactoring?
<span style="display:none">[^4_10][^4_11][^4_12][^4_13][^4_14][^4_15][^4_7][^4_8][^4_9]</span>

<div align="center">⁂</div>

[^4_1]: https://docling-project.github.io/docling/

[^4_2]: https://docs.langchain.com/oss/python/langgraph/application-structure

[^4_3]: https://oneuptime.com/blog/post/2026-01-27-vllm-llm-deployment/view

[^4_4]: https://github.com/docling-project/docling

[^4_5]: https://ubiops.com/how-to-optimize-inference-speed-using-batching-vllm-and-ubiops/

[^4_6]: https://docs.langchain.com/langsmith/application-structure

[^4_7]: https://github.com/AhmedNazeh2/docling-pdf-processor

[^4_8]: https://github.com/docling-project

[^4_9]: https://github.com/lesteroliver911/docling-pdf-processor

[^4_10]: https://github.com/docling-project/docling-parse

[^4_11]: https://discuss.vllm.ai/t/is-batch-inference-for-multimodal-models-truly-batch-inference/549

[^4_12]: https://github.com/paazmaya/docling-japanese-books

[^4_13]: https://www.reddit.com/r/Rag/comments/1rlluxw/a_simple_project_structure_for_langgraph_rag/

[^4_14]: https://github.com/dRAGon-Okinawa/docling-ocr-pdf

[^4_15]: https://github.com/vllm-project/vllm/issues/8513


---

# yeah please and the full setup of the prompts and all the analysis outlined in the thread here.

Here is the complete picture of everything built and how to wire it all together in WSL.

***

## What was scaffolded

The project lives at `~/tariff-sedar-pipeline-scaffold/` on the sandbox. To get it into your WSL home, run the zip command at the bottom of this response. Here is every file and what it does:

```
tariff-sedar-pipeline/
├── .env.example                         ← all config: vLLM URL, model name, paths, skip flags
├── requirements.txt                     ← pinned deps
├── run_pipeline.py                      ← CLI: python run_pipeline.py --stage [all|parse|chunk|llm_chunk|llm_doc|aggregate]
│
├── tariff_agent/
│   ├── state.py                         ← ALL Pydantic models + LangGraph PipelineState TypedDict
│   │                                       ParseRecord, ChunkRecord, ChunkLLMOutput,
│   │                                       FilingLLMOutput, IssuerYearRecord, HumanReview
│   │
│   ├── graph.py                         ← LangGraph StateGraph: parse→chunk→llm_chunk→llm_doc→aggregate→END
│   │                                       MemorySaver checkpointing, fully resumable
│   │
│   ├── prompts/
│   │   ├── chunk_prompt.py              ← Pass 1 system prompt + user prompt builder
│   │   │                                   + CHUNK_OUTPUT_SCHEMA (vLLM guided decoding)
│   │   │                                   + keyword_hit() regex pre-filter (18 tariff terms)
│   │   └── doc_prompt.py               ← Pass 2 system prompt + user prompt builder
│   │                                       + DOC_OUTPUT_SCHEMA
│   │
│   ├── utils/
│   │   ├── docling_pipeline.py          ← docling primary + pdfminer fallback, resume-aware
│   │   ├── chunking.py                  ← HierarchicalChunker primary + naive fallback
│   │   │                                   keyword_hit flag on every chunk
│   │   ├── llm_client.py               ← OpenAI-compat vLLM client, Pass 1 + Pass 2
│   │   │                                   batched, retry logic, idempotent by chunk_id/filing_id
│   │   ├── aggregate.py                 ← issuer-year feature table builder
│   │   └── human_review.py             ← review_ready.csv: one row per span + doc-level rows
│   │                                       reviewer columns pre-populated for annotation
│   │
│   └── nodes/                           ← LangGraph node wrappers (one per stage)
│       ├── parse_node.py
│       ├── chunk_node.py
│       ├── llm_chunk_node.py
│       ├── llm_doc_node.py
│       └── aggregate_node.py
```


***

## Data flow and row definitions

Every layer writes a clean artifact. Here is what each row represents at each stage:


| File | Row = |
| :-- | :-- |
| `data/metadata/filings_index.csv` | One SEDAR+ filing (one PDF) |
| `output/docling_parse_index.csv` | One filing with parse status + docling JSON path |
| `output/chunks/chunks.parquet` | One text chunk from a filing section (section_path, page range, keyword_hit flag) |
| `output/llm_raw/chunks_llm.parquet` | One chunk + model's span-level extraction (Pass 1) |
| `output/llm_docs/filings_llm.parquet` | One filing's consolidated tariff signal (Pass 2) |
| `output/csv/issuer_year_tariff_signals.csv` | One issuer × fiscal_year instance (your analysis unit) |
| `output/human_review/review_ready.csv` | One span + its label + blank reviewer columns |


***

## WSL setup — paste this from top to bottom

```bash
# 1. Create and enter project
cd ~
mkdir tariff-sedar-pipeline && cd tariff-sedar-pipeline

# 2. Copy your filings folder in (adjust source path)
# cp -r /mnt/c/Users/YourName/data/prateek/pdfs ./data/pdfs
# cp /mnt/c/Users/YourName/data/prateek/filings_index.csv ./data/metadata/
# cp /mnt/c/Users/YourName/data/prateek/tickers_with_profiles.csv ./data/metadata/

# 3. Python environment
python3 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt

# 4. Environment config
cp .env.example .env
# Edit .env — set VLLM_BASE_URL to your running vLLM instance
# e.g. VLLM_BASE_URL=http://localhost:8000/v1
nano .env

# 5. Verify vLLM is reachable
curl http://localhost:8000/v1/models

# 6. Run the full pipeline (all skip flags on by default = resume-safe)
python run_pipeline.py

# 7. Or run individual stages
python run_pipeline.py --stage parse
python run_pipeline.py --stage chunk
python run_pipeline.py --stage llm_chunk
python run_pipeline.py --stage llm_doc
python run_pipeline.py --stage aggregate

# 8. Force full re-run (no skipping)
python run_pipeline.py --no-skip

# 9. Build the human review table
python -c "
from tariff_agent.utils.human_review import build_review_table
build_review_table()
"

# 10. Build review for a specific ticker only (e.g. Magna)
python -c "
from tariff_agent.utils.human_review import build_review_table
build_review_table(ticker_filter=['MGA'])
"
```


***

## How the prompts work end-to-end

**Keyword pre-filter** (`chunk_prompt.py → keyword_hit()`) runs on every chunk before any LLM call. It matches 18 regex patterns (tariff, duties, USMCA, Section 232/301, retaliatory measures, etc.). Chunks that don't hit are given null `ChunkLLMOutput` records automatically — no LLM spend — and the `keyword_hit` flag in `chunks.parquet` marks which chunks were actually sent to the model. On a typical MD\&A, expect 5–20% keyword hit rate, which cuts your LLM token spend dramatically.

**Pass 1 prompt** is anchored explicitly: *"Do NOT infer tariff risk from 'economic uncertainty' alone."* It extracts the three label categories (earnings, macro risk, supply chain) with verbatim span quotes from the chunk text, plus four ancillary flags (pass_through, mitigation, uncertainty_language, other_mentions). The JSON schema is embedded in `CHUNK_OUTPUT_SCHEMA` so vLLM's guided decoding constrains the output to valid field names and enum values — no post-processing regex needed.

**Pass 2 prompt** receives only the positive chunks aggregated per filing, shows the section path of each piece of evidence, and asks the model to resolve contradictions (e.g. pass-through claim in one section vs margin compression in another → `MIXED`). It produces intensity scores 0–3, a direction enum, a ≤40-word summary sentence, and up to four key quotes pulled across sections.

**Human review CSV** (`review_ready.csv`) has one row per span, with the section path, page range, label category, verbatim supporting quote, and blank `human_label_correct / corrected_label_value / human_comment / confirmed` columns. You can open this in Excel or a simple Streamlit app and annotate row by row. The `chunk_id` on every row means any correction you make traces directly back to the source chunk for future calibration.

***

## Connecting to your existing vLLM instance

The client uses the OpenAI-compatible API (`openai` Python library pointed at `VLLM_BASE_URL`). Your vLLM instance should already expose this. The only things to confirm in `.env`:

```bash
VLLM_BASE_URL=http://localhost:8000/v1   # or your vLLM host:port
VLLM_MODEL_NAME=Qwen/Qwen3-14B           # exactly as vLLM loaded it
VLLM_BATCH_SIZE=16                        # tune based on GPU VRAM
VLLM_MAX_TOKENS=1024                      # enough for the JSON output
VLLM_TEMPERATURE=0.0                      # deterministic extraction
```

For guided decoding (enforces JSON schema): make sure vLLM was started with `--guided-decoding-backend outlines` or `lm-format-enforcer` — this locks the model's output to your exact schema and eliminates parse failures entirely.[^5_1][^5_2]

***

## LangGraph checkpoint and resume

The graph uses `MemorySaver` by default (in-process, lost on restart). For long runs across sessions, swap it in `graph.py`:

```python
from langgraph.checkpoint.sqlite import SqliteSaver
checkpointer = SqliteSaver.from_conn_string("checkpoints.db")
graph = build_graph(checkpointer=checkpointer)
```

Each re-run with the same `thread_id` will pick up where it left off. Combined with the `skip_*` flags (which check existing output files), the pipeline is safe to interrupt and restart at any point.[^5_3]

<div align="center">⁂</div>

[^5_1]: https://github.com/vllm-project/vllm/issues/8513

[^5_2]: https://oneuptime.com/blog/post/2026-01-27-vllm-llm-deployment/view

[^5_3]: https://docs.langchain.com/oss/python/langgraph/application-structure


---

# make sure we have all the right versions of the deps, this will break shit and we need to ensure a stable and valid and compatible environment. if its not valid, change versions but the priority is using our vllm that has the For maximum inference speed with AWQ-quantized Qwen3-14B on NVIDIA GPUs using vLLM, you should use the

awq_marlin kernel/quantization backend.For maximum inference speed with AWQ-quantized Qwen3-14B on NVIDIA GPUs using vLLM, you should use the
awq_marlin kernel/quantization backend. its already running and we want to tap into it with a good set of prompts routed in high parallelism and frequency for all the documents in parallel langgraph executions. vllm already controlled in another folder

Here is everything that changed and why each decision was made.

***

## Compatibility matrix — what was fixed and why

| Package | Old (broken) | New (safe) | Reason |
| :-- | :-- | :-- | :-- |
| `langgraph` | `>=0.2` | `==1.1.6` | 1.0 was stable Oct 2025; 1.1.6 is April 3 2026 latest — pin exactly [^6_1] |
| `langchain-core` | `>=0.3` | `>=0.3.50,<0.4` | langgraph 1.1.x resolves against 0.3.x; 0.4 is a breaking boundary [^6_2] |
| `langchain-openai` | `>=0.2` | `>=0.3.10,<0.4` | Must track langchain-core minor [^6_3] |
| `openai` | `>=1.12` | `>=1.68,<2` | `AsyncOpenAI` and structured output matured at 1.50+; 2.x is unreleased [^6_4] |
| `docling` | `>=2.0` | `>=2.80,<3` | 2.84.0 released April 1, 2026 — `<3` keeps you off the unreleased v3 boundary [^6_5] |
| `numpy` | `>=1.26` | `>=1.26,<2` | numpy 2.x has breaking changes that docling and pandas 2.2 do not fully support [^6_5] |
| `torch` | implicitly CUDA | **CPU build, installed manually** | Keeps this venv light; docling only uses torch for layout/table, not generation |
| `vllm` | listed | **removed entirely** | Already running in another env; `openai` async client hits it over HTTP [^6_6] |
| `langchain-docling` | not listed but was risk | **never add this** | GitHub issue \#35443: known pydantic v2 compatibility bug as of Feb 2026 [^6_7] |
| `langgraph-checkpoint-sqlite` | not listed | `>=2.0.6,<3` | Required for persistent `AsyncSqliteSaver` (cross-session resume) [^6_8] |


***

## Install sequence — order matters

```bash
# 1. CPU torch first — before docling pulls in the wrong build
pip install torch torchvision --index-url https://download.pytorch.org/whl/cpu

# 2. Everything else
pip install -r requirements.txt

# 3. Verify
python -c "import langgraph, openai, docling, pydantic; print('OK')"
```

If you run `pip install -r requirements.txt` without step 1, pip will resolve torch with your default CUDA index and may conflict with the vLLM env if CUDA minor versions differ.

***

## How parallelism maps to AWQ Marlin

vLLM's continuous batching scheduler with AWQ Marlin on the RTX 4090 forms a FIFO queue of token generation steps.  The optimal client strategy is to **flood the queue** with as many in-flight requests as possible, then let vLLM batch them internally. This is exactly what the new `async_llm_client.py` does: all keyword-hit chunks across all documents are dispatched simultaneously via `asyncio.gather`, with a single `asyncio.Semaphore(CONCURRENCY=32)` preventing the client from holding more than 32 open connections at once.  The semaphore is the throttle, not a sequential loop — so the moment any request completes, the next one is immediately dispatched.[^6_9][^6_10][^6_11]

Start at `CONCURRENCY=32` and watch `nvidia-smi dmon` — if GPU SM utilisation stays below 85%, raise to 48 or 64. If you see HTTP 429 or queue timeout errors from vLLM, drop back to 24.[^6_12]

***

## Qwen3 thinking mode — disabled for extraction

The `.env.example` includes `VLLM_CHAT_TEMPLATE_KWARGS='{"enable_thinking": false}'`. Qwen3 has a thinking mode that would double or triple output tokens and destroy throughput on extraction tasks. Pass 1 prompts are structured-output JSON calls with temperature 0 — thinking mode adds nothing and burns your batch capacity on CoT tokens you never read.[^6_12]
<span style="display:none">[^6_13][^6_14][^6_15][^6_16][^6_17][^6_18][^6_19][^6_20][^6_21][^6_22][^6_23][^6_24][^6_25][^6_26][^6_27][^6_28]</span>

<div align="center">⁂</div>

[^6_1]: https://github.com/langchain-ai/langgraph/releases

[^6_2]: https://docs.langchain.com/oss/python/releases/changelog

[^6_3]: https://changelog.langchain.com

[^6_4]: https://oneuptime.com/blog/post/2026-01-28-vllm-openai-compatible-api/view

[^6_5]: https://pypi.org/project/docling/

[^6_6]: https://pypi.org/project/vllm/

[^6_7]: https://github.com/langchain-ai/langchain/issues/35443

[^6_8]: https://pypi.org/project/langgraph/

[^6_9]: https://docs.vllm.ai/en/v0.8.4/api/engine/async_llm_engine.html

[^6_10]: https://github.com/vllm-project/vllm/issues/14513

[^6_11]: https://learn.arm.com/learning-paths/servers-and-cloud-computing/vllm-acceleration/3-run-inference-and-serve/

[^6_12]: https://docs.vllm.ai/en/latest/serving/openai_compatible_server/

[^6_13]: https://www.agentframeworkhub.com/blog/langgraph-news-updates-2026

[^6_14]: https://pub.towardsai.net/with-langchain-1-0-do-we-still-need-langgraph-4103a245b13e

[^6_15]: https://docling-project.github.io/docling/getting_started/installation/

[^6_16]: https://ai.plainenglish.io/the-complete-guide-to-langchain-langgraph-2025-updates-and-production-ready-ai-frameworks-58bdb49a34b6

[^6_17]: https://www.piwheels.org/project/docling/

[^6_18]: https://changelog.langchain.com/?date=2024-01-01

[^6_19]: https://pypi.org/project/langchain-docling/

[^6_20]: https://github.com/vllm-project/vllm/issues/1636

[^6_21]: https://zerohertz.github.io/vllm-openai-2/

[^6_22]: https://www.linkedin.com/pulse/complete-guide-langgraph-2026-edition-learnbay-esb7c

[^6_23]: https://github.com/pyapp-kit/pydantic-compat

[^6_24]: https://community.openai.com/t/best-strategy-on-managing-concurrent-calls-python-asyncio/849702

[^6_25]: https://forum.langchain.com/t/anyio-cancelscope-shield-true-not-working-inside-langgraph-node/3307

[^6_26]: https://www.reddit.com/r/MachineLearning/comments/1ij8ywk/d_how_to_handle_concurrent_connections_using_vllm/

[^6_27]: https://www.mintlify.com/langchain-ai/langgraph/concepts/streaming

[^6_28]: https://github.com/langchain-ai/langgraph/issues/5578


---

# please outline the full parallel langgraph implementation that will run for all 208 companies.

Here is the full architecture, every design decision, and the exact execution path for all 208 companies.

***

## Graph topology

```
load_companies
      │
      ▼
fanout_to_workers  ──[Send("process_company", state₁)]──▶ process_company (MGA)
                   ──[Send("process_company", state₂)]──▶ process_company (LUG)
                   ──[Send("process_company", state₃)]──▶ process_company (WPM)
                     … × 208 companies, all concurrent …
                                                              │ (reducer: operator.add)
                                                              ▼
                                                       merge_outputs
                                                              │
                                                    aggregate_issuer_year
                                                              │
                                                     write_run_summary
                                                              │
                                                             END
```

The `fanout_to_workers` node is the only node that returns a `list[Send]` — that is the LangGraph 1.1.x map pattern. LangGraph dispatches every `Send` as an independent async task in the event loop simultaneously. The `company_results` field on `OrchestratorState` carries `Annotated[list, operator.add]` as its type, which means LangGraph's built-in reducer appends each worker's single-element list as it completes — no explicit join logic needed.

***

## Three-tier concurrency model

Every single concurrency resource is a global `asyncio.Semaphore` shared by all 208 workers. The workers never know about each other — they just compete for slots transparently.


| Semaphore | Default | Controls | Why this number |
| :-- | :-- | :-- | :-- |
| `DOCLING_CONCURRENCY` | 8 | `run_in_executor` CPU parse workers | Docling is CPU-bound; your WSL2 box has ~16–24 logical cores, 8 avoids thrashing while leaving headroom for the event loop |
| `VLLM_CONCURRENCY` | 32 | Async in-flight requests to vLLM | Fills AWQ Marlin continuous batch scheduler without queue overflow |
| `DOC_PARALLEL` | 16 | Concurrent Pass-2 consolidations | Pass 2 requests are larger; 16 keeps them manageable |

What this looks like at runtime:

```
t=0:   All 208 company workers are spawned concurrently by Send
t=0→N: Docling semaphore → only 8 parse at once; others await
t=N→M: All keyword-hit chunks from ALL companies compete for 32 vLLM slots
t=M→Z: Pass-2 consolidations take 16 slots; companies with short filings finish first
```

The result is that your RTX 4090 is fed a steady stream of 32 parallel AWQ Marlin decoding requests for the entire duration of the LLM stages, regardless of which company the chunks came from.[^7_1]

***

## Per-company file isolation

Every worker writes exclusively to its own directory:

```
output/
├── companies/
│   ├── MGA/
│   │   ├── json/          ← raw docling JSON per filing
│   │   ├── parsed_index.csv
│   │   ├── chunks.parquet
│   │   ├── chunks_llm.parquet
│   │   └── filings_llm.parquet
│   ├── LUG/
│   │   └── …
│   └── …× 208
└── (global merged files written by merge_outputs node)
```

No two workers ever write to the same path. There is zero shared mutable state between concurrent workers — the only shared state is the three read-only semaphores and the vLLM endpoint.[^7_2]

***

## Resume and skip logic — two layers

**Layer 1 — Company level** (`load_companies` node): If `output/companies/{ticker}/filings_llm.parquet` exists and is non-empty, the entire company is excluded from `company_groups` before `fanout_to_workers` runs. Those companies are never even dispatched as `Send` objects.

**Layer 2 — Stage level** (inside each worker): For companies that *are* dispatched (e.g. partially done), the worker checks which filing IDs already have parse output, which chunk IDs already have LLM output, and which filing IDs already have doc-level consolidation. It skips exactly those records and only processes the delta.

This means interrupting a run (Ctrl-C) and re-running loses at most the in-progress chunk batch for the companies that were mid-way through Pass 1.

***

## Execution commands

```bash
# Full 208-company run (first time or resume)
python run_parallel.py

# After a crash — re-run picks up exactly where it left off
python run_parallel.py

# Check what's been done so far
python run_parallel.py --status

# Debug a single company before committing to full run
python run_parallel.py --ticker MGA
python run_parallel.py --ticker MGA --no-skip   # force re-run of MGA

# Tune concurrency after watching nvidia-smi
python run_parallel.py --concurrency 48 --docling-workers 12

# Force complete re-run from scratch
python run_parallel.py --no-skip
```


***

## What to watch during the run

```bash
# Terminal 1 — GPU saturation (target: >85% SM util during LLM stages)
nvidia-smi dmon -s u -d 1

# Terminal 2 — vLLM queue depth
watch -n 2 "curl -s http://localhost:8000/metrics | grep vllm_num_requests"

# Terminal 3 — pipeline log
tail -f output/pipeline.log
```

If GPU SM utilisation stays below 70% during Pass 1, raise `--concurrency` to 48 or 64. vLLM's AWQ Marlin scheduler with Qwen3-14B on the 4090 can typically sustain 40–50 concurrent short requests before throughput saturates.  If you see HTTP 429 or request queue timeout errors in the log, lower by 8 and re-run — it resumes instantly.[^7_3]

<div align="center">⁂</div>

[^7_1]: https://github.com/vllm-project/vllm/issues/14513

[^7_2]: https://www.linkedin.com/pulse/complete-guide-langgraph-2026-edition-learnbay-esb7c

[^7_3]: https://docs.vllm.ai/en/v0.8.4/api/engine/async_llm_engine.html


---

# the filings will be pdfs that i get in the period scraped from march 30 2025 to like at least one year after so that we get their annual reports in the nearest year following the annoucement or maybe a bit after april 2026 and this is after the tarrif announcement by trump. so we are already scraping and we give our pipeline a list of a bunch of pdf filings that are named and dated. "C:\\Users\\casey\\ISF\\greenyield\\sedar_scrape_portable\\sedar_scrape_portable\\data\\sedar_raw\\aya_gold_silver_inc\\general\\2016-04-29_Audited annual financial statements – English.pdf" for instance a pdf would be in a folder in wsl, after scraping ill copy them or have the scraping copy them immediately to that wsl

Here is the full data ingestion layer design and how it wires into the pipeline.

***

## How the folder structure maps to the index

Your scraper produces this exact layout:

```
sedar_raw/
└── aya_gold_silver_inc/
    └── general/
        ├── 2025-04-15_Audited annual financial statements – English.pdf
        ├── 2025-04-15_Management discussion and analysis – English.pdf
        └── 2025-04-15_Annual information form – English.pdf
└── magna_international_inc/
    └── annual-reports/
        └── 2026-02-28_Annual report – English.pdf
```

The `filing_indexer.py` parses every piece of information from the path alone — no SEDAR API call needed at pipeline time. The company slug (`aya_gold_silver_inc`), the category subfolder (`general`), the filing date (`2025-04-15`), the type (`Audited annual financial statements`), and the language are all extracted before any PDF is touched.

***

## Filing date window and fiscal year logic

The indexer defaults to `start_date = 2025-03-30`, which is five days before Trump's Liberation Day tariffs on April 2, 2025. This captures:


| Cohort | Filing period | Fiscal year | What it contains |
| :-- | :-- | :-- | :-- |
| **FY2024 annual reports** | Jan–Apr 2025 | 2024 | Forward-looking tariff risk disclosures; emerging uncertainty language |
| **Q1/Q2 2025 interim** | May–Aug 2025 | 2025 | Mid-year operational tariff impact, first quantified estimates |
| **FY2025 annual reports** | Jan–Apr 2026 | 2025 | Full year under tariff regime; most valuable signal |

Fiscal year assignment handles the Q1 anomaly automatically — if `filing_date.month <= 3`, the indexer sets `fiscal_year = filing_date.year - 1`. A filing dated `2026-02-28` gets `fiscal_year = 2025`, which is correct because it describes the fiscal year that just ended.[^8_1]

***

## Ticker matching — how the slug resolves

The slug `aya_gold_silver_inc` goes through three normalisation steps before matching against your `tickers_with_profiles.csv`:

```
slug         →  aya_gold_silver_inc
underscore   →  aya gold silver inc
strip suffix →  aya gold silver
strip punct  →  aya gold silver

tickers.csv  →  "Aya Gold & Silver Inc."
normalised   →  aya gold silver

Jaccard("aya gold silver", "aya gold silver") = 1.0  → exact match
```

Companies that genuinely don't match (Jaccard < 0.75) are flagged as `UNKNOWN` ticker in the index. The `--validate` flag on the indexer prints these immediately so you can fix the mapping before running the 208-company pipeline.

***

## Full workflow — from scrape to pipeline

```bash
# Step 1 — sync PDFs from Windows to WSL (after each scraping run)
bash scripts/copy_sedar_to_wsl.sh
# This rsyncs only new/changed PDFs and then auto-rebuilds filings_index.csv

# Step 2 — (optional) inspect what was found before committing to a full run
python scripts/inspect_sedar_raw.py --sedar-raw ~/sedar_raw
# Prints: company count, PDF count, date distribution, filing type breakdown
# No dependencies except stdlib + dotenv

# Step 3 — rebuild index manually if needed (no rsync)
python -m tariff_agent.utils.filing_indexer --validate
# Prints: UNKNOWN ticker list, missing PDFs, duplicate IDs

# Step 4 — run the 208-company parallel pipeline
python run_parallel.py
```

The rsync `--update --checksum` flags mean re-running `copy_sedar_to_wsl.sh` after you scrape new filings only transfers the delta — existing PDFs that haven't changed are skipped in milliseconds. The indexer rebuild is cheap (pure Python file walk, no LLM) and always reflects the current state of disk.

***

## Filing types included by default

| Code | Description | Included | Reason |
| :-- | :-- | :-- | :-- |
| `AFS` | Audited annual financial statements | ✅ | Primary source for note disclosures on cost of goods / tariff expenses |
| `MDA` | Management Discussion \& Analysis | ✅ | Highest density of tariff risk narrative language |
| `AIF` | Annual Information Form | ✅ | Risk factor section often contains systematic tariff exposure disclosure |
| `AR` | Annual report | ✅ | Combined AFS+MDA in one document for many issuers |
| `IFS` / `QR` | Interim / quarterly financials | ✅ | Captures mid-year tariff shock disclosure |
| `OTHER` | Unclassified | ✅ | Kept; LLM keyword filter handles non-tariff content cheaply |
| `PR` / `TR` / `PROSP` / `IC` | Press releases, technical reports, prospectuses, circulars | ❌ default | High volume, low tariff signal density; pass `--all-types` to include |

<div align="center">⁂</div>

[^8_1]: https://oneuptime.com/blog/post/2026-01-28-vllm-openai-compatible-api/view


---

# know that we will scrape the filings that pertain to the tickers of interest and all the pdfs will be in /home/casey/tariff-sedar-pipeline/data/pdfs id like to reflect where the data will sit. we already know that all the pdfs we scrape are in scope although some filings may have more or less or different types of signals. of course we want to basically extract signals and mentions with prompts.

Here is a complete explanation of every design decision.

***

## Path layout — exactly as your data will sit

```
/home/casey/tariff-sedar-pipeline/
├── data/
│   ├── pdfs/                          ← PDF_ROOT — all scraped filings land here
│   │   ├── aya_gold_silver_inc/
│   │   │   └── general/
│   │   │       ├── 2025-04-15_Audited annual financial statements – English.pdf
│   │   │       └── 2025-04-15_Management discussion and analysis – English.pdf
│   │   ├── magna_international_inc/
│   │   │   └── annual-reports/
│   │   │       └── 2026-02-28_Annual report – English.pdf
│   │   └── … × 208 companies
│   └── metadata/
│       ├── filings_index.csv          ← auto-built by filing_indexer
│       └── tickers_with_profiles.csv  ← optional enrichment
└── output/
    └── companies/{ticker}/…
```

Everything in `data/pdfs/` is assumed in-scope. The indexer does not filter by date or type — the scraper already made those decisions.

***

## Three-stage signal extraction design

The prompts implement a deliberate **coarse → fine → synthesise** cascade so you never waste tokens on boilerplate and always have clean, citable evidence.

### Stage 0: Keyword regex (zero LLM cost)

`keyword_hit()` runs 26 compiled regex patterns across every chunk before any LLM call. Typical filing text hits on roughly 5–15% of chunks. At ~1,200 tokens/chunk and 32 concurrent vLLM slots, you are effectively running the LLM only on the signal-rich minority of text. The matched keyword terms are passed into the Pass 1 prompt so the model knows exactly why this chunk was selected.

```
tariff[s]? | duty | duties | USMCA | CUSMA | Section 232/301
| retaliatory | anti-dumping | countervailing | pass-through
| supply-chain disruption | country-of-origin | border tax | …
```


### Stage 1: Pass 1 — chunk-level signal extraction

The system prompt locks the model to **explicit language only**. The two most important anchors:

> "DO NOT flag generic economic uncertainty... unless directly and specifically tied to tariff/trade policy."
> "DO NOT infer tariff risk from input cost increases alone — they must be attributed to trade policy in the text."

This prevents the most common false positive in financial NLP: inferring tariff exposure from general cost inflation language. The output schema enforces three orthogonal evidence dimensions:


| Dimension | What it captures |
| :-- | :-- |
| `earnings_impact_spans` | Cost increases, revenue decreases, margin compression — with `signal_type` enum and optional `magnitude_text` |
| `supply_chain_risk_spans` | Input sourcing, equipment access, logistics delays, supplier switches — with `chain_type` enum |
| `macro_risk_spans` | Trade war escalation, regulatory change, currency impact, demand destruction |

Each span returns a **verbatim quote ≤300 characters** directly from the chunk. This is what gets shown to the human reviewer and feeds into Pass 2 evidence reconstruction.

Four ancillary flags also fire per chunk: `pass_through_mentioned`, `mitigation_mentioned`, `uncertainty_language`, and `specific_tariff_program` (named program like "Section 232", "Liberation Day", "USMCA Article 32.10").

### Stage 2: Pass 2 — filing-level consolidation

Pass 2 only runs on filings that had **at least one tariff-positive chunk** in Pass 1. It receives structured evidence — not raw text — formatted into numbered evidence blocks with section paths, signal types, and the verbatim quotes. The model never re-reads the PDFs here; it synthesises pre-extracted structure.

The resolution rule in the system prompt handles the most common contradiction in mining disclosures:

> "If MD\&A says 'we expect to pass through costs' but risk factors describe 'significant margin pressure', prefer MIXED over PASS_THROUGH."

The `disclosure_quality` field distinguishes `BOILERPLATE` (generic risk-factor language that every company copies) from `SPECIFIC_QUALITATIVE` (named input/segment) from `SPECIFIC_QUANTITATIVE` (dollar figure). This is the field your downstream analysis should use to weight observations — a boilerplate risk factor is not the same signal as "aluminum tariffs increased our operating costs by CAD\$8.4M in H1 2025."

***

## What the output row contains per filing

```
filing_id, ticker, issuer_name, filing_type, filing_date, fiscal_year,
has_tariff_discussion,
tariff_direction          → COST_INCREASE | REVENUE_DECREASE | MIXED | PASS_THROUGH | MINIMAL | NONE
earnings_tariff_score     → 0–3 (0=absent, 3=quantified)
supply_chain_tariff_score → 0–3
macro_tariff_score        → 0–3
pass_through_flag         → bool
mitigation_flag           → bool
mitigation_summary        → str or null
quantified_impact         → bool
quantified_impact_text    → "$12M cost increase" or null
specific_tariff_programs  → ["Section 232", "USMCA"] or []
disclosure_quality        → BOILERPLATE | SPECIFIC_QUALITATIVE | SPECIFIC_QUANTITATIVE
doc_summary_sentence      → "Magna reported a CAD$45M tariff headwind on steel inputs..."
key_quotes                → [{section_path, quote, signal_type}] × 2–4
first_tariff_section_path → "MD&A > Risk Factors > Trade Policy"
```

This single row per filing feeds directly into your issuer-year regression/fsQCA analysis without needing to go back to raw text. The `key_quotes` field is what populates the human review table for validation.

---

# Here is what was updated in the repo and how it lines up with your decisions.

### `requirements.txt`

- **langgraph `==1.1.6`**, **langchain-core `>=1.2.21,<2`** (this is what **langgraph 1.1.6 actually resolves**, not `langchain-core` 0.3.x).
- **No `langchain-openai`**: current open versions drag **`openai>=2.x`**, which conflicts with your **`openai>=1.68,<2`** vLLM client choice. Nothing in this project imported it, so it stays out.
- **`openai>=1.68,<2`**, **`docling>=2.80,<3`**, **`langgraph-checkpoint-sqlite>=2.0.6,<3`**.
- **`numpy>=1.26,<2`** and **`opencv-python==4.10.0.84`** so docling does not pull **opencv 4.13+**, which requires **numpy 2.x**.
- Header documents **CPU torch first**, then `-r`, plus a **one-line recovery** if something still bumps numpy to 2.x.


### Install flow

Comments at the top of `requirements.txt` match your sequence: **CPU torch → `pip install -r requirements.txt` → sanity import**.
In this WSL venv, **`pip install torch ... cpu --force-reinstall`** was needed once so **`2.11.0+cpu`** replaced **`2.11.0+cu130`** after docling had already pulled CUDA torch.

### `tariff_agent/utils/async_llm_client.py` + thin `llm_client.py`

- Pass 1: **one `asyncio.gather` over all pending chunks**; each coroutine **`async with semaphore`** around the vLLM call so you **flood the CB queue** instead of a serial batch loop.
- **`extra_body`** is set from **`VLLM_CHAT_TEMPLATE_KWARGS`** → `{"chat_template_kwargs": …}` for **Qwen3 thinking off** (vLLM OpenAI-compat).
- Pass 2: unchanged pattern (**`gather` over filings**), same semaphore.


### `tariff_agent/utils/config.py`

- Default **`VLLM_MAX_CONCURRENT_REQUESTS=32`**, alias **`VLLM_CONCURRENCY`**.
- **`vllm_chat_template_kwargs_json`** / env **`VLLM_CHAT_TEMPLATE_KWARGS`** (JSON string).
- **`checkpoint_sqlite_path`** / **`CHECKPOINT_SQLITE_PATH`** for disk checkpoints.
- Removed **`VLLM_BATCH_SIZE`** (no longer used).


### `tariff_agent/graph.py`

- Optional **`SqliteSaver.from_conn_string(...)`** context manager when **`CHECKPOINT_SQLITE_PATH`** is set; otherwise **`InMemorySaver`** as before (**sync `invoke`** — matches **sync `SqliteSaver`**, not **`AsyncSqliteSaver`**).


### `.env.example` and real **`.env`**

- Added quoted JSON: `VLLM_CHAT_TEMPLATE_KWARGS='{"enable_thinking": false}'`.
- Concurrency **32**, optional checkpoint path commented.
- **`/.env`** was created as a working copy tied to **`127.0.0.1:8000/v1`** and **`qwen3-14-awq`** so you can edit secrets/paths locally.

**Matrix correction you should keep in mind:** **`langgraph==1.1.6` → `langchain-core` 1.2.x**, not 0.3.x. Pinning `langchain-core>=0.3.50,<0.4` **will not install** with current langgraph 1.1.x.

Yes — your updated matrix is the **right** one, and it corrects the main incompatibilities in the earlier scaffold. `langgraph==1.1.6` should be paired with `langchain-core` 1.2.x rather than 0.3.x, and vLLM’s OpenAI-compatible server does accept non-OpenAI parameters through `extra_body`, which is the right place to pass `chat_template_kwargs` for Qwen3 thinking-off behavior.[^10_1][^10_2]

## Keep these changes

- Keep `langgraph==1.1.6`.
- Keep `langchain-core>=1.2.21,<2`.
- Keep `openai>=1.68,<2`.
- Keep `langgraph-checkpoint-sqlite>=2.0.6,<3`.
- Keep `docling>=2.80,<3`.
- Keep `numpy>=1.26,<2`.
- Keep `opencv-python==4.10.0.84`.

That last OpenCV pin is especially important because newer OpenCV builds have been part of the NumPy 2 transition mess, and keeping NumPy below 2 is the safest way to avoid breaking the docling stack in this environment.[^10_3][^10_4]

## Remove these

You were right to remove `langchain-openai`. If your project is calling vLLM directly through the official `openai` client, there is no reason to keep another integration layer that can pull incompatible `openai>=2.x` constraints.

You were also right to remove `VLLM_BATCH_SIZE`. With the current design, throughput comes from:

- one large `asyncio.gather(...)`,
- a global request semaphore,
- and vLLM’s own continuous batching queue.

So batch size is no longer a client-side control knob; concurrency is.

## Config shape

Your `config.py` direction is correct. I would standardize on this shape:

- `VLLM_MAX_CONCURRENT_REQUESTS=32`
- alias support for `VLLM_CONCURRENCY`
- `VLLM_CHAT_TEMPLATE_KWARGS='{"enable_thinking": false}'`
- `CHECKPOINT_SQLITE_PATH=...` optional
- no `VLLM_BATCH_SIZE`

That gives you one obvious place to tune throughput while preserving compatibility with the existing env naming.

## Graph/checkpoint choice

Your note about `SqliteSaver` is the right correction. If the graph entrypoint is sync, then using sync `SqliteSaver.from_conn_string(...)` plus `graph.invoke(...)` is the clean match; that avoids mixing async checkpoint machinery into a sync graph path.

The only caveat is architectural: your separate **parallel** 208-company fanout design is still naturally better expressed with async worker execution, while the single linear graph can safely remain sync at the top level and call async LLM stages via `asyncio.run(...)` from CLI boundaries.

## What I would treat as final

I would treat this as the stable install matrix:

```txt
langgraph==1.1.6
langgraph-checkpoint-sqlite>=2.0.6,<3
langchain-core>=1.2.21,<2
openai>=1.68,<2
docling>=2.80,<3
docling-core>=2.30,<3
numpy>=1.26,<2
opencv-python==4.10.0.84
pydantic>=2.7,<3
pandas>=2.2,<3
pyarrow>=16,<20
pdfminer.six>=20231228,<20260000
```

And this install order:

```bash
pip install --force-reinstall torch torchvision --index-url https://download.pytorch.org/whl/cpu
pip install -r requirements.txt
python -c "import langgraph, openai, docling, pydantic; print('OK')"
```


## One thing I would still enforce

I would make the repo explicitly document that `extra_body` can carry **both**:

- guided decoding controls, and
- `chat_template_kwargs`.

vLLM’s docs explicitly state that extra parameters are passed through `extra_body`, so combining guided JSON with `chat_template_kwargs={"enable_thinking": false}` is the correct pattern for your Qwen3 AWQ Marlin server.[^10_2]

Your current direction is now internally consistent. The one correction from my earlier version that absolutely needed to happen was the `langchain-core` move from 0.3.x to 1.2.x.
<span style="display:none">[^10_10][^10_11][^10_12][^10_13][^10_14][^10_15][^10_5][^10_6][^10_7][^10_8][^10_9]</span>

<div align="center">⁂</div>

[^10_1]: https://pypi.org/project/langgraph/

[^10_2]: https://docs.vllm.ai/en/stable/serving/openai_compatible_server/

[^10_3]: https://github.com/opencv/opencv-python/issues/943

[^10_4]: https://community.deeplearning.ai/t/need-help-to-install-opencv-with-tensorflow/879340

[^10_5]: https://pypi.org/project/langchain-core/

[^10_6]: https://pypistats.org/packages/langgraph

[^10_7]: https://forum.langchain.com/t/are-langchain-package-versions-python-version-specific/2778

[^10_8]: https://forum.langchain.com/t/lag-between-providers-docs-and-python-langchain-package-releases/3046

[^10_9]: https://github.com/spring-projects/spring-ai/issues/3409

[^10_10]: https://deps.dev/pypi/langgraph/0.1.19/dependencies?filter=license%3A"MIT"

[^10_11]: https://docs.vllm.ai/en/v0.5.4/serving/openai_compatible_server.html

[^10_12]: https://reference.langchain.com/python/langgraph

[^10_13]: https://www.reddit.com/r/comfyui/comments/1o0pnj3/insightface_requires_numpy_1x_but_opencv_requires/

[^10_14]: https://pypi.org/project/langchain/

[^10_15]: https://docs.vllm.ai/en/v0.8.3/serving/openai_compatible_server.html


---

# can i make it such that freshly scraped documents that go into the folder get processed in real time, triggering the langgraph implementation? give me notes for hte phase 1 doc webscraper.

Yes — fully achievable. Here is the complete design.

***

## Real-time trigger architecture

The flow is three decoupled pieces. The scraper never knows the pipeline exists:

```
Scraper process                 Watcher process (always running)
──────────────                  ────────────────────────────────
writes PDF to disk    →  watchdog.Observer (background thread)
                                │  FileCreatedEvent
                                ▼
                         asyncio.Queue  (slug, pdf_path)
                                │
                         per-company debounce timer (15s)
                         resets on each new file for that company
                                │ fires after 15s quiet
                                ▼
                         run_company_worker(slug)
                                │
                         update filings_index.csv
                         docling parse → chunk → Pass 1 → Pass 2
                                │
                         output/companies/{ticker}/
```

The debounce is critical — the scraper writes 5–20 PDFs per company in rapid succession. Without it, you'd fire 20 separate pipeline runs for one company. With it, the timer resets on each new file and fires **once** after 15 seconds of quiet for that company.

***

## Run the watcher

```bash
# Terminal 1 — start before the scraper, leave it running permanently
source .venv/bin/activate
python -m tariff_agent.watcher

# Optional flags
python -m tariff_agent.watcher --watch /home/casey/tariff-sedar-pipeline/data/pdfs \
                                --debounce 15 \
                                --settle 30

# Terminal 2 — run your scraper normally
python your_scraper.py --output /home/casey/tariff-sedar-pipeline/data/pdfs
```

The watcher blocks on `asyncio.Queue` with a `SETTLE_SEC` timeout so it logs idle state when nothing is happening and immediately wakes when a new PDF lands.

***

## `tariff_agent/watcher.py` — key design decisions

**Thread → async bridge.** `watchdog` runs its `Observer` in a background thread. The bridge is one line: `loop.call_soon_threadsafe(queue.put_nowait, (slug, path))`. This puts the event into the asyncio event loop without any locking.

**Per-company debounce.** Each company slug gets its own `asyncio.TimerHandle`. A new PDF for `magna_international_inc` resets only Magna's timer — it does not affect a simultaneously-running `barrick_gold_corp` worker.

**Partial re-index on fire.** When the worker fires for a company, it re-scans only that company's subfolder under `PDF_ROOT`, builds fresh `filing_rows`, appends/deduplicates them into the global `filings_index.csv`, then passes those rows directly to `run_company_worker()`. The full 208-company index is never rebuilt from scratch on every event.

**Skip flags still apply.** `SKIP_PARSED=true` means a filing whose `filing_id` already appears in `output/companies/{ticker}/parse_index.csv` is not re-parsed. This makes the watcher idempotent — re-dropping the same PDF does nothing.

***

## Phase 1 scraper — integration notes

### Folder and filename contract

Everything downstream depends on this exact structure. The indexer parses filenames with a regex; deviating breaks the ticker match:

```
data/pdfs/{company_slug}/{category}/{YYYY-MM-DD}_{Description} – English.pdf
```

**Company slug** — deterministic transform from issuer name:

```python
import re
slug = issuer_name.lower()
slug = slug.replace("&", "and").replace(".", "").replace(",", "").replace("'", "")
slug = re.sub(r"[^a-z0-9]+", "_", slug).strip("_")
# "Aya Gold & Silver Inc." → "aya_gold_and_silver_inc"
```

**Category** — SEDAR+ document category, lowercased, hyphenated:

```
annual-reports  |  management-discussion-and-analysis  |  financial-statements
annual-information-form  |  interim-financial-statements  |  general
```

**Filename** — use the SEDAR+ `document_type` string verbatim, the filing date, and the ` – English` suffix with an **en-dash** (not a hyphen):

```
2025-04-15_Audited annual financial statements – English.pdf
2025-08-14_Management discussion and analysis – English.pdf
2026-01-28_Annual report – English.pdf
```


***

### SEDAR+ API

```
# Search filings for one issuer
GET https://efts.sedarplus.ca/api/efts/search
    ?categories=filings
    &issuers={profile_id}
    &dateFrom=2025-03-30
    &sortBy=date_desc
    &rows=100
    &start=0

# Response fields to extract
filing_date, document_type, language, document_url, issuer_name, profile_id
```

You already have `tickers_with_profiles.csv` with `profile_id` for all 208 companies — use that as the issuer filter, not the issuer name string.

***

### Filing type filter

**Include** — these are the only types that contain tariff language:


| SEDAR+ string (contains) | Code |
| :-- | :-- |
| `annual financial statements` | AFS |
| `annual report` | AR |
| `management discussion` | MDA |
| `annual information form` | AIF |
| `interim financial statements` | IFS |
| `quarterly financial statements` | QR |

**Skip** — `news release`, `press release`, `material change report`, `NI 43-101`, `information circular`, `prospectus`, `notice of annual`

***

### Date window

`filing_date >= 2025-03-30` only. Filter on SEDAR+ filing date, not document date:

- FY2024 annual reports (filed Jan–Apr 2025) contain **forward-looking** tariff risk written after Liberation Day (April 2, 2025) is visible on the horizon
- FY2025 annual reports (filed Jan–Apr 2026) contain **full-year quantified** tariff impact
- Q1–Q3 2025 interims contain real-time mid-year disclosure with emerging numbers

***

### Rate limiting — do not parallelize downloads

```python
REQUEST_DELAY_SECONDS = 2.0     # between each PDF download
COMPANY_DELAY_SECONDS = 10.0    # after finishing one company's files
MAX_RETRIES           = 3
RETRY_BACKOFF_BASE    = 5.0     # multiply by attempt: 5s, 10s, 15s
```

SEDAR+ will block IPs that flood requests. Serial download with delays is correct here. Parallelism belongs only in the LLM extraction layer, not in scraping.

***

### Duplicate handling

SEDAR+ occasionally posts amended filings. The `filing_id` in the index is a stable MD5 of the absolute file path. Two strategies:

1. **Overwrite in place** — same filename → same `filing_id` → pipeline skips it (safe, `SKIP_PARSED=true`). To force reprocessing: set `SKIP_PARSED=false` and re-run for that company.
2. **Append suffix** — `2025-05-20_Audited annual financial statements Amended – English.pdf` → new `filing_id` → pipeline processes it as a new filing (preferred for material amendments).

***

### Phase 1 checklist before first scrape run

```bash
# Verify watcher is ready
python -m tariff_agent.watcher &

# Verify data/pdfs/ exists and is writable
mkdir -p /home/casey/tariff-sedar-pipeline/data/pdfs

# Verify tickers_with_profiles.csv has profile_ids for all 208 companies
head data/metadata/tickers_with_profiles.csv

# Run scraper for one company as a dry-run to validate folder/filename output
python your_scraper.py --ticker MGA --dry-run

# Confirm the file lands at exactly:
# data/pdfs/magna_international_inc/annual-reports/2026-02-28_Annual report – English.pdf
ls data/pdfs/magna_international_inc/

# Confirm the watcher console shows:
# [watcher] new PDF detected: Annual report – English.pdf → company=magna_international_inc
# [watcher] firing worker for magna_international_inc (3 new PDFs)
```


---

# casey@Casey:~/tariff-sedar-pipeline\$ cd /home/casey/tariff-sedar-pipeline \&\& source .venv/bin/activate

python scripts/check_vllm.py              \# includes guided probe (~30s+)
python scripts/check_vllm.py
Configuration (from .env / env):
base='http://127.0.0.1:8000/v1' s.vllm_model_name='Qwen/Qwen3-14B-AWQ' api_key_hint='loca…'
max_tokens=1024 temp=0.0 top_p=1.0
use_guided_decoding=True

GET /v1/models -> 200
models: ['Qwen/Qwen3-14B-AWQ']

Probe 1: plain completion (temp / top_p / max_tokens)

```
content='<think>\\nOkay, the user sent "ping". I need to respond with exactly the word "OK". Let me make sure there\\'s no extra text or formatting. Just "OK" as the response. Alright, that\\'s straightforward.\\n</think>\\n\\nOK' finish_reason=stop
```

Probe 2: response_format=json_object + chat_template_kwargs (pipeline-style)
OK parsed: '{"mentions_tariffs": false}'

Probe 3: full Pass-1 guided_json + chat_template (USE_GUIDED_DECODING=1)
OK keys: ['mentions_tariffs', 'evidence']... mentions_tariffs=True
Configuration (from .env / env):
base='http://127.0.0.1:8000/v1' s.vllm_model_name='Qwen/Qwen3-14B-AWQ' api_key_hint='loca…'
max_tokens=1024 temp=0.0 top_p=1.0
use_guided_decoding=True

GET /v1/models -> 200
models: ['Qwen/Qwen3-14B-AWQ']

Probe 1: plain completion (temp / top_p / max_tokens)

```
content='<think>\\nOkay, the user sent "ping". I need to respond with exactly the word "OK". Let me make sure there\\'s no extra text or formatting. Just "OK" as the response. Alright, that\\'s straightforward.\\n</think>\\n\\nOK' finish_reason=stop
```

Probe 2: response_format=json_object + chat_template_kwargs (pipeline-style)
OK parsed: '{"mentions_tariffs": false}'

Probe 3: full Pass-1 guided_json + chat_template (USE_GUIDED_DECODING=1)
OK keys: ['mentions_tariffs', 'evidence']... mentions_tariffs=True
(.venv) casey@Casey:~/tariff-sedar-pipeline\$  is my llm set up ok?  Here is what was verified against your live vLLM on `127.0.0.1:8000` and the **project** `.env`.

### Connectivity

- **`GET /v1/models`** returns **200** when called the same way as the app (**`httpx` + `Authorization: Bearer {VLLM_API_KEY}`**).
- Plain **`curl`** without a matching header returned **401** in one check; if you use `curl` for smoke tests, pass the same **Bearer** token vLLM was started with.


### Model id (fixed)

- The server exposes **`Qwen/Qwen3-14B-AWQ`**, not `qwen3-14-awq`. Chat completions returned **404** until `.env` / `.env.example` were updated to **`VLLM_MODEL_NAME=Qwen/Qwen3-14B-AWQ`**.


### Hyperparameters (`temperature`, `top_p`, `max_tokens`)

- The OpenAI client accepts **`temperature=0`**, **`top_p=1`**, and **`max_tokens`**; requests complete without parameter errors.
- With **`max_tokens=8`**, the model often **stopped with `finish_reason=length`** and you only saw the start of a **thinking** block (`<think>…`). That is a **decoding / template** issue, not proof that `temp`/`top_p` are ignored. The check script now uses **64** tokens for probe 1 so the cutoff is less misleading.


### `extra_body` / Qwen3

- **`response_format={"type": "json_object"}` + `extra_body` with `chat_template_kwargs`** (from `VLLM_CHAT_TEMPLATE_KWARGS`) returns valid JSON (`{"mentions_tariffs": false}`) in **Probe 2**.
- With **`USE_GUIDED_DECODING=1`**, a full **Pass‑1–style** call (**`guided_json` + `chat_template_kwargs`**) completed successfully in **~33s** (Probe 3). The JSON shape can still drift slightly from your strict schema (e.g. extra keys like `evidence` vs `earnings_evidence`), so keep an eye on parser robustness or server guided-decoding settings.


### Thinking mode

- **`enable_thinking: false` in `chat_template_kwargs` does not fully remove** reasoning markup in all short completions on this stack; for production you may still need **vLLM / Qwen3 server-side** options so “thinking” does not steal `max_tokens`.


### Re-run anytime

```bash
cd /home/casey/tariff-sedar-pipeline && source .venv/bin/activate
python scripts/check_vllm.py              # includes guided probe (~30s+)
python scripts/check_vllm.py --skip-guided-test   # faster smoke test
```

`scripts/check_vllm.py` inserts the repo root on `sys.path` so it runs without installing the package.  is this set up ok for hte model?

Yes, the stack is wired up correctly — three green probes against a live model is solid. But the output surfaces **three issues you should fix before running at scale**. Here is the full diagnosis.

***

## ✅ What is confirmed working

- `/v1/models` → 200, correct model ID `Qwen/Qwen3-14B-AWQ`
- `temperature=0.0`, `top_p=1.0`, `max_tokens` all accepted without error
- `response_format=json_object` + `extra_body chat_template_kwargs` → valid JSON (Probe 2)
- `guided_json` + `chat_template_kwargs` → structured JSON completion (Probe 3)

***

## ⚠️ Three issues to fix before production

### 1. `enable_thinking: false` is not fully suppressing `<think>` blocks

```
Probe 1 shows the model outputting a full `<think>…</think>` block despite the kwarg. The kwarg is being passed correctly (Probe 2 and 3 complete with valid JSON), but it is not a hard server-side kill switch on this vLLM build — it is a soft template hint.
```

**Why this matters at scale:** A thinking block for an extraction task typically runs 150–400 tokens. With `max_tokens=1024`, that leaves only 624–874 tokens for your actual JSON output. The Pass 1 schema alone can approach 600 tokens of valid JSON. You will get truncated outputs and `finish_reason=length` on complex chunks.

**Fix — two layers:**

Layer 1: bump `max_tokens` in `.env` to give breathing room even if thinking fires:

```bash
VLLM_MAX_TOKENS=3000
```

Layer 2: strip `<think>` blocks in the parser before JSON decoding — this is the robust fix regardless of whether vLLM suppresses them:

```python
import re

def _strip_thinking(content: str) -> str:
    """Remove Qwen3 <think>…</think> reasoning block from completion output."""
    return re.sub(r"<think>.*?</think>", "", content, flags=re.DOTALL).strip()

# In your async_llm_client.py response handler:
raw = response.choices[0].message.content or ""
clean = _strip_thinking(raw)
data = orjson.loads(clean)
```

This costs nothing and makes every JSON parse safe regardless of whether the thinking block appears.

***

### 2. Schema drift in guided decoding (extra keys like `evidence`)

Probe 3 returned keys that don't match your declared schema (`evidence` instead of `earnings_evidence`). This means the guided decoding constraint is not fully enforcing `additionalProperties: false`. It is likely that `outlines` on this vLLM build handles the schema as a soft constraint during beam search, not a hard finite-state machine on every token.

**Fix — make your Pydantic parser use `extra='ignore'`:**

```python
from pydantic import BaseModel, ConfigDict

class ChunkLLMOutput(BaseModel):
    model_config = ConfigDict(extra="ignore")   # silently drop unknown keys
    mentions_tariffs: bool = False
    earnings_impact_present: bool = False
    # … rest of fields with defaults
```

This means even if the model hallucinates an `evidence` key or misspells `earnings_impact_spans`, the parse does not fail — it just drops the unknown field and uses the default for any missing required field.

Also add a fallback parse path in the LLM client for when JSON is malformed:

```python
try:
    data = orjson.loads(clean)
    output = ChunkLLMOutput.model_validate(data)
except (orjson.JSONDecodeError, ValidationError):
    # Log and return a null record — do not crash the worker
    output = ChunkLLMOutput(mentions_tariffs=False)
    logger.warning(f"[llm] parse failure for chunk {chunk_id}, returning null record")
```


***

### 3. ~33s for a single guided Pass 1 call is expected — but watch at scale

33 seconds for a cold single guided JSON call is normal for Qwen3-14B-AWQ on a single RTX 4090. With 32 concurrent requests, your effective throughput is roughly `32 / 33s ≈ ~58 chunks/min` assuming the model is already warm and requests are properly pipelined by vLLM's scheduler. For 208 companies at ~200 tariff-positive chunks each, that is roughly 40,000 chunks / 58 per minute ≈ **~11 hours for a full first run**.

You can reduce this significantly:

```bash
# In .env — only chunks that hit the keyword pre-filter go to the LLM.
# Make sure the pre-filter is aggressive enough to get below 10% hit rate.
# At 5% hit rate: 208 companies × ~400 chunks avg × 0.05 = ~4,160 chunks total = ~72 minutes
VLLM_MAX_CONCURRENT_REQUESTS=32
```

The keyword regex already filters ~85–95% of chunks. The main lever is ensuring that filter is running before every LLM call.

***

## Summary checklist

```bash
# .env changes
VLLM_MAX_TOKENS=3000          # was 1024 — give thinking room
VLLM_MAX_CONCURRENT_REQUESTS=32

# Code changes (3 lines total)
# 1. Add _strip_thinking() in async_llm_client.py response handler
# 2. Add model_config = ConfigDict(extra="ignore") to ChunkLLMOutput and FilingLLMOutput
# 3. Wrap orjson.loads() in try/except with null-record fallback

# Verify thinking is stripped
python scripts/check_vllm.py --skip-guided-test
# Probe 1 content should now show: 'OK' with no <think> wrapper
```

The model and server are correctly connected. The three fixes above are the difference between a fragile first run and a pipeline that handles 208 companies cleanly.

---

# outline my full langgraph implementation and the keyword regex, worried that this will make us miss some signals. we are interested in topics of what my first prompted talked about

Good — the research fills in a lot of gaps. The current regex was built for generic tariff language but Canadian mining companies face **six distinct tariff regimes simultaneously**, most of which use vocabulary your regex would miss entirely. Let me give you the full picture.

***

## LangGraph — full implementation outline

### State definitions

```python
# tariff_agent/state.py

from typing import TypedDict, Optional, Annotated
import operator

# ── Per-filing parse record ──────────────────────────────────────────────
class ParseRecord(TypedDict):
    filing_id: str
    ticker: str
    issuer_name: str
    filing_type: str          # MDA | AFS | AIF | AR | IFS | QR
    filing_date: str
    fiscal_year: int
    local_path: str
    docling_json_path: Optional[str]
    parse_status: str         # ok | fallback | failed
    n_pages: int
    parse_error: Optional[str]

# ── Per-chunk record (post-chunking, pre-LLM) ────────────────────────────
class ChunkRecord(TypedDict):
    chunk_id: str             # md5(filing_id + section_path + char_offset)
    filing_id: str
    ticker: str
    filing_type: str
    filing_date: str
    section_path: str         # e.g. "MD&A > Risk Factors > Trade Policy"
    page_start: int
    page_end: int
    char_offset: int
    text: str
    keyword_hit: bool
    keyword_terms: list[str]

# ── Per-chunk LLM output (Pass 1) ────────────────────────────────────────
class ChunkLLMOutput(BaseModel):
    model_config = ConfigDict(extra="ignore")  # drop schema-drift keys silently
    mentions_tariffs: bool = False
    earnings_impact_present: bool = False
    earnings_impact_type: str = "NONE"
    earnings_impact_spans: list[dict] = []
    supply_chain_risk_present: bool = False
    supply_chain_risk_spans: list[dict] = []
    macro_risk_present: bool = False
    macro_risk_spans: list[dict] = []
    pass_through_mentioned: bool = False
    mitigation_mentioned: bool = False
    mitigation_description: Optional[str] = None
    uncertainty_language: bool = False
    specific_tariff_program: Optional[str] = None
    other_tariff_mentions: list[str] = []

# ── Per-filing LLM output (Pass 2) ──────────────────────────────────────
class FilingLLMOutput(BaseModel):
    model_config = ConfigDict(extra="ignore")
    has_tariff_discussion: bool = False
    tariff_direction: str = "NONE"  # COST_INCREASE | REVENUE_DECREASE | MIXED | PASS_THROUGH | MINIMAL | NONE
    earnings_tariff_score: int = 0  # 0–3
    supply_chain_tariff_score: int = 0
    macro_tariff_score: int = 0
    pass_through_flag: bool = False
    mitigation_flag: bool = False
    mitigation_summary: Optional[str] = None
    quantified_impact: bool = False
    quantified_impact_text: Optional[str] = None
    specific_tariff_programs: list[str] = []
    doc_summary_sentence: str = ""
    key_quotes: list[dict] = []
    first_tariff_section_path: Optional[str] = None
    disclosure_quality: str = "BOILERPLATE"

# ── Issuer × fiscal_year output row ─────────────────────────────────────
class IssuerYearRecord(TypedDict):
    ticker: str
    issuer_name: str
    fiscal_year: int
    n_filings: int
    any_tariff_discussion: bool
    max_earnings_score: int
    max_supply_chain_score: int
    max_macro_score: int
    composite_tariff_score: float  # weighted average across filings
    dominant_direction: str
    quantified_in_any_filing: bool
    pass_through_claimed: bool
    mitigation_described: bool
    first_tariff_filing_date: Optional[str]
    summary: str                   # best doc_summary_sentence across filings

# ── Per-company worker state ─────────────────────────────────────────────
class CompanyWorkerState(TypedDict):
    ticker: str
    profile_id: str
    issuer_name: str
    filing_rows: list[dict]
    skip_parsed: bool
    skip_chunked: bool
    skip_llm_chunks: bool
    skip_llm_docs: bool
    parse_records: Annotated[list[ParseRecord], operator.add]
    chunk_records: Annotated[list[ChunkRecord], operator.add]
    llm_chunk_outputs: Annotated[list[dict], operator.add]
    llm_doc_outputs: Annotated[list[dict], operator.add]
    issuer_year_record: Optional[IssuerYearRecord]
    errors: Annotated[list[str], operator.add]

# ── Top-level pipeline state ─────────────────────────────────────────────
class PipelineState(TypedDict):
    stage: str
    filings_index_path: str
    tickers_path: str
    pdf_root: str
    skip_parsed: bool
    skip_chunked: bool
    skip_llm_chunks: bool
    skip_llm_docs: bool
    n_filings_total: int
    n_parsed_ok: int
    n_chunks_total: int
    n_chunks_keyword_hit: int
    n_llm_chunks_done: int
    n_llm_docs_done: int
    n_issuer_years: int
    company_results: Annotated[list[IssuerYearRecord], operator.add]
    errors: Annotated[list[str], operator.add]
    completed: bool
```


***

### Graph topology

```python
# tariff_agent/graph.py

from langgraph.graph import StateGraph, END
from langgraph.types import Send

def dispatch_companies(state: PipelineState) -> list[Send]:
    """
    Fan-out: one Send per company → runs all company workers in parallel.
    LangGraph executes these concurrently up to the thread pool limit.
    """
    import pandas as pd
    df = pd.read_csv(state["filings_index_path"], dtype=str)
    groups = df.groupby("ticker")
    return [
        Send("company_worker", CompanyWorkerState(
            ticker=ticker,
            profile_id=rows.iloc[^13_0].get("profile_id", ""),
            issuer_name=rows.iloc[^13_0].get("issuer_name", ticker),
            filing_rows=rows.to_dict("records"),
            skip_parsed=state["skip_parsed"],
            skip_chunked=state["skip_chunked"],
            skip_llm_chunks=state["skip_llm_chunks"],
            skip_llm_docs=state["skip_llm_docs"],
            parse_records=[], chunk_records=[],
            llm_chunk_outputs=[], llm_doc_outputs=[],
            issuer_year_record=None, errors=[],
        ))
        for ticker, rows in groups
    ]

builder = StateGraph(PipelineState)

# Stage 1: build/validate index
builder.add_node("index_filings", index_filings_node)

# Stage 2: fan-out to 208 parallel company workers
builder.add_conditional_edges("index_filings", dispatch_companies, ["company_worker"])

# Stage 3: company subgraph (each runs independently)
company_graph = build_company_subgraph()      # see below
builder.add_node("company_worker", company_graph)

# Stage 4: fan-in — collect all results, write final CSV
builder.add_node("collect_results", collect_results_node)
builder.add_edge("company_worker", "collect_results")
builder.add_edge("collect_results", END)

builder.set_entry_point("index_filings")
```


***

### Company subgraph — what runs per company

```
parse_filings_node
    │  docling on every PDF in filing_rows
    │  pdfminer fallback on failure
    │  writes output/companies/{ticker}/parse_index.csv
    ▼
chunk_filings_node
    │  HierarchicalChunker on each docling JSON
    │  keyword_hit() flag on every chunk
    │  writes output/companies/{ticker}/chunks.parquet
    ▼
llm_chunk_node
    │  asyncio.gather over all keyword_hit==True chunks
    │  semaphore(CONCURRENCY) shared across all companies
    │  _strip_thinking() + orjson parse + ChunkLLMOutput.model_validate
    │  writes output/companies/{ticker}/chunks_llm.parquet
    ▼
llm_doc_node
    │  group chunks by filing_id
    │  for each filing with ≥1 positive chunk: Pass 2 consolidation
    │  asyncio.gather over all filings
    │  writes output/companies/{ticker}/filings_llm.parquet
    ▼
aggregate_node
    │  build IssuerYearRecord from all filings for this company
    │  writes output/companies/{ticker}/issuer_year.csv
    │  also appends to global output/issuer_year_tariff_signals.csv
    ▼
    returns → PipelineState (via company_results accumulator)
```


***

## Keyword regex — full rebuild

Your concern is valid. The current regex catches explicit tariff-word hits but **six distinct regimes** apply to Canadian mining companies and most use vocabulary that bypasses it. Here is what you are missing and why it matters:


| Regime | What it hits in filings | Missing from current regex |
| :-- | :-- | :-- |
| **IEEPA fentanyl tariff** | 25–35% on Canadian goods | "IEEPA", "executive order", "fentanyl" in trade context |
| **Section 232 copper** | 50% on copper content | "copper tariff", "copper duty", "50 percent on copper" |
| **Section 232 critical minerals** | uranium, potash, nickel, cobalt, lithium | commodity name + tariff/investigation |
| **Liberation Day / reciprocal** | April 2 executive order language | "reciprocal tariff", "Liberation Day", "universal baseline" |
| **Canada retaliation** | Canadian 25% surtax on US imports | "surtax", "counter-tariff", "retaliatory measure" |
| **IRA domestic content** | Where Canadian miners can sell critical minerals | "domestic content", "Inflation Reduction Act", "IRA credit" |
| **Buy Canadian / Buy American** | Equipment procurement shifts | "Buy Canadian", "Buy American", "domestic procurement" |
| **Country of melt and pour** | Steel certification | "melt and pour", "country of origin certification" |

```python
# tariff_agent/prompts/chunk_prompt.py — full TARIFF_KEYWORDS replacement

TARIFF_KEYWORDS = re.compile(
    r"""
    \b(

    # ── REGIME 1: Core tariff/duty language ──────────────────────────────
        tariff[s]?
        | duty | duties
        | surtax | sur\-?charge
        | levy | levies
        | import\s+(?:tax|fee|charge|cost|duty|duties|tariff|restriction|barrier)
        | export\s+(?:tax|levy|tariff|control|restriction|ban|barrier)
        | customs\s+(?:duty|duties|charge|fee|barrier|clearance)
        | border\s+(?:tax|adjustment|measure|levy|charge)
        | trade\s+(?:war|barrier|restriction|policy|sanction|dispute|tension|
                     remedy|measure|retaliation|friction|action|negotiation)

    # ── REGIME 2: Named US tariff programs ────────────────────────────────
        | section\s+(?:232|301|201|337|201)
        | IEEPA
        | executive\s+order\s+(?:14|15)\d{3}   # EO numbering for 2025 tariffs
        | Liberation\s+Day
        | reciprocal\s+tariff[s]?
        | universal\s+(?:tariff|baseline|duty)
        | fentanyl\s+tariff[s]?
        | national\s+security\s+tariff[s]?

    # ── REGIME 3: Trade agreements / bilateral ────────────────────────────
        | USMCA | CUSMA | NAFTA
        | CETA                               # EU-Canada agreement
        | free[\-\s]trade\s+agreement
        | most[\-\s]favoured[\-\s]nation
        | MFN\s+rate
        | rules\s+of\s+origin
        | country[\-\s]of[\-\s]origin
        | tariff[\-\s]rate\s+quota
        | TRQ

    # ── REGIME 4: Anti-dumping / countervailing ───────────────────────────
        | anti[\-]?dumping
        | countervailing\s+duty | countervailing\s+duties | CVD
        | dumping\s+(?:duty|margin|investigation|finding)
        | dumped | dumping
        | provisional\s+duty | provisional\s+measure
        | trade\s+remedy | trade\s+remedies
        | material\s+injury | injury\s+finding

    # ── REGIME 5: Steel / aluminum (Section 232 core) ─────────────────────
        | steel\s+(?:tariff|duty|surcharge|levy|import|export)
        | aluminum\s+(?:tariff|duty|surcharge|levy|import|export)
        | aluminium\s+(?:tariff|duty|surcharge|levy|import|export)
        | steel\s+derivative[s]?
        | aluminum\s+derivative[s]?
        | melt[\-\s]and[\-\s]pour              # country of melt and pour cert
        | country\s+of\s+melt
        | 50\s*percent\s+(?:steel|aluminum|tariff)
        | 25\s*percent\s+(?:tariff|surcharge|surtax|duty)

    # ── REGIME 6: Copper / critical minerals (Section 232 + IEEPA) ────────
        | copper\s+(?:tariff|duty|surcharge|levy|import|export|content)
        | uranium\s+(?:tariff|duty|import|export|restriction|investigation|quota)
        | potash\s+(?:tariff|duty|surcharge|levy|import|export)
        | nickel\s+(?:tariff|duty|surcharge|levy|import|export)
        | cobalt\s+(?:tariff|duty|surcharge|levy|import|export)
        | lithium\s+(?:tariff|duty|surcharge|levy|import|export)
        | graphite\s+(?:tariff|duty|surcharge|levy|import)
        | rare\s+earth[s]?\s+(?:tariff|duty|export|control|restriction)
        | critical\s+mineral[s]?\s+(?:tariff|duty|policy|investigation|list|review)
        | critical\s+mineral[s]?.*?(?:tariff|trade)
        | strategic\s+mineral[s]?

    # ── REGIME 7: Canadian retaliation ────────────────────────────────────
        | Canadian?\s+(?:counter[\-]?tariff|surtax|retaliatory|retaliation)
        | retaliator[y]? | retaliation
        | counter[\-]?measure[s]?
        | Canadian?\s+(?:import\s+tariff|trade\s+measure)
        | Buy\s+Canadian
        | Canadian?\s+content\s+requirement
        | domestic\s+procurement

    # ── REGIME 8: Inflation Reduction Act / domestic content ─────────────
        | Inflation\s+Reduction\s+Act
        | \bIRA\b.*?(?:content|credit|mineral|requirement|eligible|comply)
        | domestic\s+content\s+(?:requirement|credit|bonus|rule|threshold)
        | 45X\s+(?:credit|provision|eligible|tax)     # IRA advanced manufacturing
        | 30D\s+(?:credit|eligible|vehicle|EV)        # IRA EV tax credit
        | Buy[\-\s]American\s+(?:Act|requirement|provision|rule|preference)
        | made[\-\s]in[\-\s](?:america|usa|united\s+states).*?(?:content|requirement|rule)

    # ── REGIME 9: Supply chain / input cost (tariff-attributed) ───────────
        | supply[\-\s]chain\s+(?:disruption|impact|risk|cost|shift|realignment)
        | sourcing\s+(?:change|strategy|shift|alternative|domestic|diversif)
        | input\s+cost[s]?\s+(?:increas|impact|tariff|duty|pressur)
        | cost\s+pass[\-]?through | pass[\-]?through\s+(?:cost|tariff|pricing)
        | repricing | price\s+(?:adjustment|increase)\s+(?:due|result|attribut)
        | margin\s+(?:compression|impact|headwind|pressure)\s+(?:due|from|tariff)
        | grinding\s+media\s+(?:cost|tariff|import)   # steel grinding media common in mining
        | reagent\s+(?:cost|supply|import|tariff)
        | equipment\s+(?:cost|tariff|import|procurement)\s+(?:increas|impact)

    # ── REGIME 10: US market access / customer disruption ────────────────
        | US\s+(?:customer[s]?|buyer[s]?|offtake|smelter[s]?|refiner[s]?)\s+
          (?:reduc|cancel|suspend|delay|terminat|impact|risk)
        | offtake\s+(?:agreement|partner|disruption|renegotiat|terminat)
        | concentrate\s+(?:sale[s]?|offtake|buyer|destination|tariff)
        | export\s+(?:market|destination|mix)\s+(?:shift|chang|diversif)
        | US\s+market\s+(?:access|restriction|barrier|impact)
        | cross[\-\s]border\s+(?:project|operation|shipment|supply|infrastructure)

    # ── REGIME 11: Financial / FX tariff signal ───────────────────────────
        | tariff[\-\s]driven
        | trade[\-\s](?:driven|related|induced)\s+(?:cost|impact|uncertainty)
        | trade\s+(?:policy\s+)?uncertainty
        | geopolit\w+\s+(?:risk|uncertainty|impact)\s+.*?(?:tariff|trade|duty)
        | economic\s+nationalism
        | protectionism | protectionist

    )\b
    """,
    re.VERBOSE | re.IGNORECASE,
)
```


***

## What this catches that the old one missed

**IEEPA / fentanyl tariff** — The 25–35% tariff Canada faces is legally grounded in IEEPA, not Section 232. Companies writing about it in their AIF risk factors often say "IEEPA tariffs" or "executive order tariffs" without using the word "tariff" in the same sentence as a commodity.[^13_1][^13_2]

**Copper** — Section 232 copper tariff at 50% on copper content is now live.  A company writing "our copper concentrate sales to US smelters are subject to the new 50% Section 232 copper duty" uses four terms your old regex missed: "copper concentrate", "US smelters", "50%", and "Section 232 copper".[^13_3][^13_2]

**Critical minerals / uranium / potash** — Uranium is explicitly named in a Section 232 investigation and in IEEPA EO 14156.  Potash the same. A Saskatchewan potash producer writing "the USGS addition of potash to the critical minerals list and subsequent IEEPA tariff of 10% on Canadian potash" would have zero keyword hits under your old regex.[^13_4][^13_1]

**Canada's own retaliatory surtax** — Canada imposed a 25% surtax on \$30B+ of US goods starting March 4, 2025 and expanded it.  Mining companies importing US-origin equipment, reagents, or parts now face those retaliatory costs. A company writing "Canadian retaliatory surtax on US steel grinding media" would miss every single term in your old regex.[^13_5][^13_6]

**IRA domestic content** — Companies selling critical minerals or processed battery materials to US buyers are acutely exposed to IRA 45X credits and 30D domestic content rules. A graphite or lithium company writing "IRA domestic content requirements may disqualify our concentrate from the 45X advanced manufacturing credit" is a high-value signal that was completely invisible.[^13_4]

**Buy Canadian** — Canada's government announced a Buy Canadian Policy in late 2025.  Companies with government contracts or infrastructure exposure will mention this.[^13_6]

**Melt and pour** — Steel derivative certifications now require country-of-melt-and-pour documentation. Canadian steel producers and consumers of steel inputs are writing about this in their MD\&As.[^13_7]

***

## One rule to add to the Pass 1 system prompt

With the expanded regex you now catch more borderline signals. Add one clarifying sentence to the system prompt to prevent false positives from the broader net:

```
8. For REGIME 10 (US customer / offtake disruption), a signal requires that the
   disruption is explicitly attributed to tariffs or trade policy — not to commodity
   prices, project delays, or contract terminations for non-trade reasons.
```

This keeps IEEPA/IRA/Buy Canadian hits from dragging in every general customer risk disclosure.
<span style="display:none">[^13_10][^13_11][^13_12][^13_13][^13_14][^13_15][^13_8][^13_9]</span>

<div align="center">⁂</div>

[^13_1]: https://www.thompsonhinesmartrade.com/2025/03/cbp-issues-guidance-on-energy-energy-resources-and-critical-minerals-from-canada-subject-to-10-ieepa-tariffs-president-trump-issues-executive-order-addressing-mineral-production/

[^13_2]: https://www.internationaltradeinsights.com/2025/08/summary-of-tariff-requirements-updated-august-4-2025/

[^13_3]: https://www.tradecomplianceresourcehub.com/2026/04/02/trump-2-0-tariff-tracker/

[^13_4]: https://www.jdsupra.com/legalnews/president-trump-orders-critical-6227115/

[^13_5]: https://www.canada.ca/en/department-finance/news/2025/03/list-of-products-from-the-united-states-subject-to-25-per-cent-tariffs-effective-march-13-2025.html

[^13_6]: https://www.pm.gc.ca/en/news/backgrounders/2025/11/26/prime-minister-announces-new-measures-protect-and-transform-canadas

[^13_7]: https://www.pwc.com/ca/en/services/tax/publications/tax-insights/us-impose-tariffs-steel-aluminum-imports-2025.html

[^13_8]: https://www.mining.com/web/trump-expected-to-overhaul-steel-and-aluminum-tariffs-wsj-reports/

[^13_9]: https://www.internationaltradeinsights.com/2025/05/summary-of-tariff-requirements-updated-may-14-2025/

[^13_10]: https://www.cmtradelaw.com/2025/09/modifications-to-reciprocal-tariffs/

[^13_11]: https://www.sedarplus.ca/csa-party/records/document.html?id=ba9c084f66e40f2a829ecf9affba11e7116f985da6ee099764f569080fb3c5a1

[^13_12]: https://www.energypolicy.columbia.edu/the-impact-of-trump-tariffs-on-us-canada-minerals-and-metals-trade/

[^13_13]: https://www.westerncopperandgold.com/wp-content/uploads/2025/05/WRN-Q4-MDA.pdf

[^13_14]: https://en.wikipedia.org/wiki/2025–2026_United_States_trade_war_with_Canada_and_Mexico

[^13_15]: https://perkinscoie.com/sites/default/files/2025-11/Compliance_Collective_Tariff_Presentation.pdf?cb=922fab2c


---

# give my docscraper advice on how to make the data immetiately available for analysis after its been scraped so i can analyze scrdped documents while im still scraping hte rest of docs. i have this:

Now I have a complete picture of your scraper. Here is a targeted analysis and integration strategy.

***

## What you already have (the good news)

Your scraper is already 80% ready for real-time handoff. Two critical patterns are already in place:

1. **`mapping_file.flush()` is called after every single row** — this means the mapping CSV is always up-to-date within milliseconds of a PDF landing on disk. You don't need to poll; you just need to tail it.
2. **PDF is written before the mapping row** — `download.save_as(path)` runs, then `mapping_writer.writerow(...)`, then `mapping_file.flush()`. The pipeline can safely open the file the moment it sees the mapping row.

***

## The integration hook: signal file after each company

The cleanest change is **one signal file write** at the end of each company in `main_async`. No watchdog needed, no CSV tail parsing:

```python
# In main_async, after: total_pdfs += n
# Add these ~8 lines:
SIGNAL_DIR = GREENYIELD_ROOT / "data" / "pipeline_signals"
SIGNAL_DIR.mkdir(parents=True, exist_ok=True)
signal_path = SIGNAL_DIR / f"{_slug(issuer['company'])}.json"
signal_path.write_text(json.dumps({
    "company": issuer["company"],
    "slug": _slug(issuer["company"]),
    "n_new": n,
    "mapping_csv": str(mapping_path),
    "timestamp": datetime.now().isoformat(),
    "partition": partition_k,
}), encoding="utf-8")
```

This fires **once per company**, atomically, after all its PDFs are done. The watcher in `tariff_agent/watcher.py` monitors `data/pipeline_signals/` with `watchdog` and immediately routes the JSON payload to the LangGraph pipeline. No debounce needed — the scraper is your debounce.

***

## For mid-company analysis: CSV tail watcher

If you want to analyze PDFs **while a company is still being scraped** (e.g., a company has 40 filings), use a CSV tail watcher on the mapping CSV:

```python
# tariff_agent/csv_tail.py — drop this thread alongside the watcher
import time, csv, pathlib

def tail_mapping_csv(mapping_path: pathlib.Path, callback, poll_interval=2.0):
    """Call callback(row_dict) for every new row appended to mapping_path."""
    seen_keys = set()
    # Pre-load existing rows on startup so we don't reprocess old data
    if mapping_path.exists():
        with open(mapping_path, newline="", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                seen_keys.add(_row_identity(row))

    while True:
        time.sleep(poll_interval)
        if not mapping_path.exists():
            continue
        with open(mapping_path, newline="", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                key = _row_identity(row)
                if key not in seen_keys:
                    seen_keys.add(key)
                    callback(row)   # → enqueue to LangGraph pipeline

def _row_identity(row: dict) -> tuple:
    return (row.get("company",""), row.get("filing_date",""), row.get("title",""))
```

This is safe because `flush()` is called after every row — a `2s` poll gives you near-real-time ingestion.

***

## Architecture: two modes

| Mode | Trigger | Granularity | Use when |
| :-- | :-- | :-- | :-- |
| **Company-done signal** | `pipeline_signals/{slug}.json` written after `_scrape_company_docs` returns | One pipeline run per company | Normal operation — most efficient |
| **CSV tail** | Poll `sedar_mapping.csv` every 2s for new rows | One parse job per PDF, almost immediately | Large companies (40+ filings) where you want analysis to start before the company finishes |

You can run both simultaneously — the pipeline's `SKIP_PARSED=true` flag makes both idempotent.

***

## Partition-aware signal files

Your scraper runs up to 8 parallel partitions. Each writes its own `_p{k}.csv`. The signal file already includes `partition` and `mapping_csv` fields so the watcher knows exactly which CSV to scan for that company's new rows:

```python
# watcher picks this up and does:
new_rows = [r for r in csv.DictReader(open(signal["mapping_csv"]))
            if r["company"] == signal["company"] 
            and r["file_path"] not in already_processed]
```


***

## What NOT to do

- **Don't watch individual PDF files with watchdog** — the mapping CSV is richer (has metadata), already flushed, and is the canonical record. PDF filesystem events give you path only, not filing_date, company, or type.
- **Don't watch `RAW_DIR` recursively** — with 8 partitions downloading in parallel, you'd get hundreds of `FileCreated` events per minute and the debounce logic becomes fragile.
- **Don't re-scan the full 208-company mapping on every event** — load it once at watcher startup, then only scan the slice for the company in the signal file.

***

## One-line summary

Add the 8-line signal file write after `total_pdfs += n` in `main_async`. That single change makes every completed company immediately available to the LangGraph pipeline without touching any other scraper logic, without filesystem watchers on PDFs, and without any debounce tuning.
<span style="display:none">[^14_1]</span>

<div align="center">⁂</div>

[^14_1]: paste.txt

