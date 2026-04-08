#!/usr/bin/env python3
"""
evidence_viewer.py — local HTTP server for browsing tariff-signal pipeline output.

Shows issuer/filing summary, key quotes with page references, and serves
PDF files from the local filesystem so you can click to view the exact page.

Usage:
    python3 scripts/evidence_viewer.py            # opens http://localhost:7070
    python3 scripts/evidence_viewer.py --port 7070
    python3 scripts/evidence_viewer.py --no-open  # don't auto-open browser
"""
from __future__ import annotations

import argparse
import json
import mimetypes
import os
import sys
import threading
import urllib.parse
import webbrowser
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent


def _load_data() -> dict:
    import pandas as pd

    out: dict = {"filings": [], "issuer_year": [], "review": [], "status": {}}

    filings_llm_csv = ROOT / "output/csv/filings_llm.csv"
    issuer_year_csv = ROOT / "output/csv/issuer_year_tariff_signals.csv"
    review_csv = ROOT / "output/human_review/review_ready.csv"
    filings_idx = ROOT / "data/metadata/filings_index.csv"

    status: dict = {}
    for k, p in [
        ("filings_llm", filings_llm_csv),
        ("issuer_year", issuer_year_csv),
        ("review", review_csv),
        ("filings_index", filings_idx),
    ]:
        status[k] = p.exists()
    out["status"] = status

    if filings_llm_csv.exists():
        df = pd.read_csv(filings_llm_csv)
        out["filings"] = df.where(pd.notnull(df), None).to_dict(orient="records")

    if issuer_year_csv.exists():
        df = pd.read_csv(issuer_year_csv)
        out["issuer_year"] = df.where(pd.notnull(df), None).to_dict(orient="records")

    if review_csv.exists():
        df = pd.read_csv(review_csv)
        out["review"] = df.where(pd.notnull(df), None).to_dict(orient="records")

    if filings_idx.exists():
        df = pd.read_csv(filings_idx)
        out["filings_index"] = df.where(pd.notnull(df), None).to_dict(orient="records")

    return out


def _html_page(data: dict) -> str:
    filings = data.get("filings", [])
    issuer_year = data.get("issuer_year", [])
    review = data.get("review", [])
    status = data.get("status", {})
    findex = {r["filing_id"]: r for r in data.get("filings_index", [])}

    # Summary stats
    total_filings = len(filings)
    tariff_filings = sum(1 for f in filings if f.get("has_tariff_discussion"))
    quant_filings = sum(1 for f in filings if f.get("quantified_impact"))

    status_html = "".join(
        f'<span class="badge {"ok" if v else "missing"}">{k}: {"✓" if v else "✗"}</span>'
        for k, v in status.items()
    )

    if not filings:
        body_html = f"""
        <div class="empty-state">
          <h2>Pipeline not yet complete</h2>
          <p>Artifact status: {status_html}</p>
          <p>Check <code>output/supervised.log</code> for progress.</p>
          <p><a href="/data.json">View raw data JSON</a> &nbsp; <a href="/" onclick="setTimeout(()=>location.reload(),3000)">Refresh in 3s</a></p>
        </div>"""
    else:
        # Build filing cards
        cards = []
        for f in sorted(filings, key=lambda x: (-(x.get("tariff_direction") or "NONE" != "NONE"),
                                                  x.get("ticker") or "")):
            fid = f.get("filing_id", "")
            meta = findex.get(fid, {})
            pdf_rel = meta.get("local_path", "")

            direction = f.get("tariff_direction") or "NONE"
            dir_cls = {
                "COST_INCREASE": "dir-cost", "REVENUE_DECREASE": "dir-rev",
                "MIXED": "dir-mixed", "PASS_THROUGH": "dir-pass",
                "MINIMAL": "dir-min", "NONE": "dir-none",
            }.get(direction, "dir-none")

            scores = (
                f"E:{f.get('earnings_tariff_score',0)} "
                f"SC:{f.get('supply_chain_tariff_score',0)} "
                f"M:{f.get('macro_tariff_score',0)}"
            )
            qual = f.get("disclosure_quality") or ""
            summary = f.get("doc_summary_sentence") or ""
            programs = f.get("specific_tariff_programs") or "[]"
            try:
                prog_list = json.loads(programs) if isinstance(programs, str) else programs
                programs_html = " ".join(f'<span class="tag">{p}</span>' for p in (prog_list or []))
            except Exception:
                programs_html = f'<span class="tag">{programs}</span>'

            # Key quotes
            kq_raw = f.get("key_quotes") or "[]"
            try:
                kq_list = json.loads(kq_raw) if isinstance(kq_raw, str) else kq_raw
            except Exception:
                kq_list = []

            quotes_html = ""
            for kq in (kq_list or [])[:4]:
                if not isinstance(kq, dict):
                    continue
                page = kq.get("page_start") or kq.get("page_end") or ""
                page_link = ""
                if pdf_rel and page:
                    page_link = f'<a class="page-link" href="/pdf/{urllib.parse.quote(pdf_rel)}#page={page}" target="_blank">p.{page} ↗</a>'
                sig = kq.get("signal_type") or ""
                sig_cls = "sig-cost" if "COST" in sig else "sig-rev" if "REVENUE" in sig else "sig-other"
                quote_text = str(kq.get("quote") or "")[:300]
                sec = kq.get("section_path") or ""
                quotes_html += f"""
                <div class="quote">
                  <span class="sig {sig_cls}">{sig}</span>
                  {page_link}
                  <span class="sec-path">{sec}</span>
                  <blockquote>{quote_text}</blockquote>
                </div>"""

            has_tariff = f.get("has_tariff_discussion", False)
            card_cls = "card-tariff" if has_tariff else "card-none"
            quant_badge = '<span class="badge ok">quantified</span>' if f.get("quantified_impact") else ""
            mit_badge = '<span class="badge mit">mitigation</span>' if f.get("mitigation_flag") else ""
            pass_badge = '<span class="badge pass">pass-through</span>' if f.get("pass_through_flag") else ""

            cards.append(f"""
            <div class="card {card_cls}" data-ticker="{f.get('ticker','')}" data-dir="{direction}">
              <div class="card-header">
                <span class="ticker">{f.get('ticker','')}</span>
                <span class="issuer">{f.get('issuer_name','')}</span>
                <span class="date">{f.get('filing_date','')}</span>
                <span class="dir-badge {dir_cls}">{direction}</span>
                <span class="scores">{scores}</span>
                <span class="qual">{qual}</span>
                {quant_badge}{mit_badge}{pass_badge}
              </div>
              {f'<div class="summary">{summary}</div>' if summary else ''}
              {f'<div class="programs">{programs_html}</div>' if programs_html else ''}
              {f'<div class="quotes">{quotes_html}</div>' if quotes_html else ''}
            </div>""")

        body_html = f"""
        <div class="summary-bar">
          <div class="stat"><span class="n">{total_filings}</span><span class="lbl">filings analysed</span></div>
          <div class="stat"><span class="n">{tariff_filings}</span><span class="lbl">with tariff signal</span></div>
          <div class="stat"><span class="n">{quant_filings}</span><span class="lbl">quantified</span></div>
          <div class="stat">{status_html}</div>
        </div>
        <div class="controls">
          <input id="filter" placeholder="Filter by ticker or issuer…" oninput="filterCards(this.value)">
          <select id="dir-filter" onchange="filterCards(document.getElementById('filter').value)">
            <option value="">All directions</option>
            <option value="COST_INCREASE">COST_INCREASE</option>
            <option value="REVENUE_DECREASE">REVENUE_DECREASE</option>
            <option value="MIXED">MIXED</option>
            <option value="PASS_THROUGH">PASS_THROUGH</option>
            <option value="MINIMAL">MINIMAL</option>
            <option value="NONE">NONE</option>
          </select>
          <label><input type="checkbox" id="tariff-only" onchange="filterCards(document.getElementById('filter').value)" checked> Tariff signals only</label>
        </div>
        <div id="cards">{''.join(cards)}</div>"""

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>Tariff Signal Evidence Viewer</title>
<style>
  * {{ box-sizing: border-box; margin: 0; padding: 0; }}
  body {{ font-family: system-ui, -apple-system, sans-serif; background: #0f1117; color: #e0e0e0; }}
  header {{ background: #1a1d27; border-bottom: 1px solid #2d3148; padding: 14px 24px; display:flex; align-items:center; gap:16px; }}
  header h1 {{ font-size: 1.1rem; color: #7eb8f7; font-weight:600; }}
  header small {{ color:#888; font-size:.8rem; }}
  .summary-bar {{ display:flex; gap:24px; padding:16px 24px; background:#141720; border-bottom:1px solid #2d3148; flex-wrap:wrap; align-items:center; }}
  .stat {{ display:flex; flex-direction:column; align-items:center; }}
  .stat .n {{ font-size:1.6rem; font-weight:700; color:#7eb8f7; }}
  .stat .lbl {{ font-size:.7rem; color:#888; text-transform:uppercase; }}
  .badge {{ display:inline-block; padding:2px 8px; border-radius:4px; font-size:.72rem; font-weight:600; margin:2px; }}
  .badge.ok {{ background:#1a4731; color:#4ade80; }}
  .badge.missing {{ background:#4b1a1a; color:#f87171; }}
  .badge.mit {{ background:#1a3a4b; color:#7dd3fc; }}
  .badge.pass {{ background:#2d2a1a; color:#fcd34d; }}
  .controls {{ padding:12px 24px; display:flex; gap:12px; align-items:center; background:#141720; }}
  #filter {{ background:#1e2235; border:1px solid #3d4268; color:#e0e0e0; padding:6px 12px; border-radius:6px; width:280px; font-size:.85rem; }}
  select {{ background:#1e2235; border:1px solid #3d4268; color:#e0e0e0; padding:6px 10px; border-radius:6px; font-size:.85rem; }}
  label {{ font-size:.82rem; color:#aaa; display:flex; align-items:center; gap:6px; }}
  #cards {{ padding:16px 24px; display:flex; flex-direction:column; gap:12px; }}
  .card {{ background:#161924; border:1px solid #2d3148; border-radius:8px; overflow:hidden; }}
  .card-tariff {{ border-left:3px solid #f59e0b; }}
  .card-none {{ border-left:3px solid #374151; opacity:.8; }}
  .card-header {{ padding:10px 16px; display:flex; flex-wrap:wrap; gap:8px; align-items:center; background:#1a1e2e; }}
  .ticker {{ font-weight:700; color:#7eb8f7; font-size:.95rem; }}
  .issuer {{ color:#ccc; font-size:.82rem; max-width:280px; overflow:hidden; text-overflow:ellipsis; white-space:nowrap; }}
  .date {{ color:#888; font-size:.78rem; }}
  .scores {{ color:#aaa; font-size:.78rem; font-family:monospace; }}
  .qual {{ color:#9ca3af; font-size:.75rem; background:#252a3d; padding:2px 8px; border-radius:4px; }}
  .dir-badge {{ font-size:.75rem; font-weight:700; padding:2px 10px; border-radius:12px; }}
  .dir-cost {{ background:#4b1f1f; color:#f87171; }}
  .dir-rev  {{ background:#3b2a1a; color:#fb923c; }}
  .dir-mixed {{ background:#3a2f1a; color:#fbbf24; }}
  .dir-pass {{ background:#1f3a2a; color:#34d399; }}
  .dir-min  {{ background:#1e2535; color:#93c5fd; }}
  .dir-none {{ background:#2a2a2a; color:#6b7280; }}
  .summary {{ padding:8px 16px; font-size:.82rem; color:#bbb; border-top:1px solid #252a3d; font-style:italic; }}
  .programs {{ padding:6px 16px; display:flex; flex-wrap:wrap; gap:4px; }}
  .tag {{ background:#1e2a3d; color:#93c5fd; font-size:.72rem; padding:2px 8px; border-radius:12px; }}
  .quotes {{ padding:8px 16px; display:flex; flex-direction:column; gap:8px; }}
  .quote {{ background:#0f1117; border-radius:6px; padding:8px 12px; border:1px solid #252a3d; }}
  .sig {{ font-size:.7rem; font-weight:700; padding:1px 7px; border-radius:4px; margin-right:6px; }}
  .sig-cost {{ background:#4b1f1f; color:#f87171; }}
  .sig-rev  {{ background:#3b2a1a; color:#fb923c; }}
  .sig-other {{ background:#252a3d; color:#93c5fd; }}
  .page-link {{ font-size:.75rem; color:#7eb8f7; margin-right:8px; text-decoration:none; }}
  .page-link:hover {{ text-decoration:underline; }}
  .sec-path {{ font-size:.7rem; color:#6b7280; }}
  blockquote {{ margin-top:4px; font-size:.82rem; color:#ccc; border-left:2px solid #374151; padding-left:8px; font-style:italic; }}
  .empty-state {{ padding:60px 24px; text-align:center; color:#888; }}
  .empty-state h2 {{ color:#ccc; margin-bottom:12px; }}
  .empty-state a {{ color:#7eb8f7; }}
  a {{ color: #7eb8f7; }}
</style>
</head>
<body>
<header>
  <h1>Tariff Signal Evidence Viewer</h1>
  <small>SEDAR+ MD&A Pipeline &mdash; {total_filings if filings else 0} filings &mdash; <a href="/data.json">raw JSON</a> &mdash; <a href="javascript:location.reload()">reload</a></small>
</header>
{body_html}
<script>
function filterCards(q) {{
  q = (q || '').toLowerCase();
  const dirF = document.getElementById('dir-filter')?.value || '';
  const tariffOnly = document.getElementById('tariff-only')?.checked;
  document.querySelectorAll('#cards .card').forEach(c => {{
    const ticker = (c.dataset.ticker || '').toLowerCase();
    const dir = c.dataset.dir || '';
    const text = c.innerText.toLowerCase();
    const matchQ = !q || ticker.includes(q) || text.includes(q);
    const matchDir = !dirF || dir === dirF;
    const matchTariff = !tariffOnly || c.classList.contains('card-tariff');
    c.style.display = (matchQ && matchDir && matchTariff) ? '' : 'none';
  }});
}}
// Auto-hide NONE on load
filterCards('');
</script>
</body>
</html>"""


class Handler(BaseHTTPRequestHandler):
    def log_message(self, fmt, *args):  # suppress access log noise
        pass

    def _send(self, code: int, ct: str, body: bytes):
        self.send_response(code)
        self.send_header("Content-Type", ct)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        parsed = urllib.parse.urlparse(self.path)
        path = parsed.path

        if path == "/" or path == "/index.html":
            data = _load_data()
            html = _html_page(data)
            self._send(200, "text/html; charset=utf-8", html.encode())

        elif path == "/data.json":
            data = _load_data()
            body = json.dumps(data, default=str, ensure_ascii=False, indent=2).encode()
            self._send(200, "application/json", body)

        elif path.startswith("/pdf/"):
            # Serve PDF file from the filings directory
            rel = urllib.parse.unquote(path[5:])  # strip /pdf/
            # Try PDF root from env, fall back to default
            pdf_root_env = os.environ.get(
                "FILINGS_PDF_ROOT",
                "/mnt/c/Users/casey/ISF/greenyield/sedar_scrape_portable/sedar_scrape_portable/data/prateek/filings",
            )
            pdf_path = Path(pdf_root_env) / rel
            if pdf_path.exists() and pdf_path.suffix.lower() == ".pdf":
                data = pdf_path.read_bytes()
                self._send(200, "application/pdf", data)
            else:
                self._send(404, "text/plain", b"PDF not found")
        else:
            self._send(404, "text/plain", b"Not found")


def main():
    ap = argparse.ArgumentParser(description="Evidence viewer for tariff signal pipeline")
    ap.add_argument("--port", type=int, default=7070)
    ap.add_argument("--no-open", action="store_true", help="Don't open browser automatically")
    args = ap.parse_args()

    os.chdir(ROOT)
    srv = HTTPServer(("127.0.0.1", args.port), Handler)
    url = f"http://127.0.0.1:{args.port}/"
    print(f"Evidence viewer: {url}")
    print("Press Ctrl+C to stop.\n")
    if not args.no_open:
        threading.Timer(0.8, lambda: webbrowser.open(url)).start()
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        print("\nStopped.")


if __name__ == "__main__":
    main()
