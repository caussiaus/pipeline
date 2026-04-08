"""Thread persistence — each analysis run is a "thread" stored as JSON.

Threads live in output/threads/<thread_id>.json and hold all state needed
to resume a pipeline session: corpus info, chat history, schema, rows, log.
"""
from __future__ import annotations

import json
import uuid
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

_ROOT = Path(__file__).resolve().parents[1]
_THREADS_DIR = _ROOT / "output" / "threads"

STATUS_COLORS = {
    "new":              "#7A6652",
    "ingesting":        "#E6C97A",
    "schema":           "#9DC8A0",
    "extracting":       "#E6C97A",
    "preview":          "#9DC8A0",
    "full_ingesting":   "#E6C97A",
    "full_extracting":  "#E6C97A",
    "done":             "#9DC8A0",
    "failed":           "#E88080",
}


@dataclass
class Thread:
    thread_id: str
    title: str
    created_at: str
    status: str       # new | ingesting | schema | extracting | preview | full_ingesting | full_extracting | done | failed
    docs_dir: str
    corpus_id: str
    corpus_name: str
    topic: str
    trial_n: int = 7
    step: str = "new"
    schema_cols: list = field(default_factory=list)
    rows: list = field(default_factory=list)
    chat: list = field(default_factory=list)
    log: list = field(default_factory=list)
    field_notes: dict = field(default_factory=dict)   # field_name → free-text note
    proc_done: bool = True
    proc_rc: int = 0
    dataset_path: str = ""
    error_msg: str = ""

    @classmethod
    def create(cls, docs_dir: str, corpus_name: str, topic: str, trial_n: int = 7) -> "Thread":
        from tariff_agent.corpus.config import _slugify
        tid = uuid.uuid4().hex[:10]
        cid = _slugify(corpus_name) or "corpus"
        title = (corpus_name or topic)[:42]
        return cls(
            thread_id=tid,
            title=title,
            created_at=datetime.now(timezone.utc).isoformat(),
            status="new",
            docs_dir=docs_dir,
            corpus_id=cid,
            corpus_name=corpus_name,
            topic=topic,
            trial_n=trial_n,
        )

    def add_log(self, line: str) -> None:
        self.log.append(line)

    def add_chat(self, role: str, content: str) -> None:
        self.chat.append({"role": role, "content": content})

    def save(self) -> None:
        save_thread(self)

    @property
    def status_color(self) -> str:
        return STATUS_COLORS.get(self.status, "#7A6652")

    @property
    def age_label(self) -> str:
        try:
            dt = datetime.fromisoformat(self.created_at)
            delta = datetime.now(timezone.utc) - dt
            s = int(delta.total_seconds())
            if s < 60:
                return "just now"
            if s < 3600:
                return f"{s//60}m ago"
            if s < 86400:
                return f"{s//3600}h ago"
            return f"{s//86400}d ago"
        except Exception:
            return ""


def _dir() -> Path:
    _THREADS_DIR.mkdir(parents=True, exist_ok=True)
    return _THREADS_DIR


def save_thread(t: Thread) -> None:
    (_dir() / f"{t.thread_id}.json").write_text(
        json.dumps(asdict(t), indent=2, default=str), encoding="utf-8"
    )


def load_thread(thread_id: str) -> Thread | None:
    path = _dir() / f"{thread_id}.json"
    if not path.exists():
        return None
    try:
        d = json.loads(path.read_text(encoding="utf-8"))
        return Thread(**{k: d[k] for k in Thread.__dataclass_fields__ if k in d})
    except Exception:
        return None


def list_threads() -> list[Thread]:
    threads: list[Thread] = []
    for p in sorted(_dir().glob("*.json"), key=lambda f: f.stat().st_mtime, reverse=True):
        try:
            d = json.loads(p.read_text(encoding="utf-8"))
            threads.append(Thread(**{k: d[k] for k in Thread.__dataclass_fields__ if k in d}))
        except Exception:
            pass
    return threads


def delete_thread(thread_id: str) -> None:
    p = _dir() / f"{thread_id}.json"
    if p.exists():
        p.unlink()
