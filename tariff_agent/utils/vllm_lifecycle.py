"""Start vLLM via systemd (or direct script) after Docling when PIPELINE_VLLM_SYSTEMD_UNIT is set."""

from __future__ import annotations

import logging
import os
import subprocess
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from tariff_agent.utils.config import Settings

logger = logging.getLogger(__name__)


def _vllm_models_url(settings: Settings) -> str:
    base = settings.vllm_base_url.rstrip("/")
    if base.endswith("/v1"):
        return f"{base}/models"
    return f"{base}/v1/models"


def _http_models_ok(url: str, timeout: float, *, authorization_bearer: str | None = None) -> bool:
    """GET /models; optional Bearer when vLLM is started with ``--api-key``."""
    headers: dict[str, str] = {"Accept": "application/json"}
    b = (authorization_bearer or "").strip()
    if b and b.upper() != "EMPTY":
        headers["Authorization"] = f"Bearer {b}"
    try:
        req = urllib.request.Request(url, headers=headers)
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return 200 <= resp.status < 300
    except (urllib.error.URLError, OSError, ValueError):
        return False


def _systemctl_is_active(unit: str, *, user: bool) -> bool:
    cmd = ["systemctl", "--user", "is-active", unit] if user else ["systemctl", "is-active", unit]
    r = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
    return r.stdout.strip() == "active"


def _systemctl_start(unit: str, *, user: bool) -> bool:
    base = ["systemctl", "--user"] if user else ["systemctl"]
    r = subprocess.run([*base, "start", unit], capture_output=True, text=True, timeout=120)
    if r.returncode == 0:
        return True
    if not user and r.stderr:
        logger.debug("systemctl start stderr: %s", r.stderr.strip())
    if not user:
        r2 = subprocess.run(
            ["sudo", "systemctl", "start", unit],
            capture_output=True,
            text=True,
            timeout=120,
        )
        if r2.returncode == 0:
            return True
        if r2.stderr:
            logger.warning("sudo systemctl start: %s", r2.stderr.strip())
    return False


_VLLM_DIRECT_SCRIPT = Path.home() / "thomas/scripts/start_vllm_qwen3_14b_awq.sh"
_vllm_direct_proc: subprocess.Popen | None = None  # kept alive for duration of process


def _launch_vllm_direct(env: dict | None = None) -> bool:
    """Launch vLLM directly via Thomas start script when systemctl is unavailable."""
    global _vllm_direct_proc
    if not _VLLM_DIRECT_SCRIPT.exists():
        logger.warning("pipeline: direct launch script not found at %s", _VLLM_DIRECT_SCRIPT)
        return False
    try:
        # Inherit env, add HF_HOME if not set
        child_env = os.environ.copy()
        if env:
            child_env.update(env)
        child_env.setdefault("HF_HOME", str(Path.home() / ".cache/huggingface"))
        _vllm_direct_proc = subprocess.Popen(
            ["bash", str(_VLLM_DIRECT_SCRIPT)],
            env=child_env,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            start_new_session=True,  # detach so it survives if pipeline restarts
        )
        logger.info("pipeline: launched vLLM direct (pid=%s) via %s", _vllm_direct_proc.pid, _VLLM_DIRECT_SCRIPT)
        return True
    except Exception as exc:
        logger.warning("pipeline: direct vLLM launch failed: %s", exc)
        return False


def maybe_start_vllm_after_parse(settings: Settings | None = None) -> None:
    """Start vLLM after Docling finishes.

    Strategy (in order):
    1. Already reachable → done.
    2. systemctl (user scope, then system scope via sudo) → start + wait.
    3. Direct script fallback: ~/thomas/scripts/start_vllm_qwen3_14b_awq.sh
    4. Log warning and return — wait_for_vllm_http() in LLM stages will block/raise.
    """
    from tariff_agent.utils.config import get_settings

    s = settings or get_settings()
    unit = (s.pipeline_vllm_systemd_unit or "").strip()

    timeout = max(30.0, float(s.pipeline_vllm_start_timeout_sec))
    poll = max(2.0, min(10.0, timeout / 60.0))
    models_url = _vllm_models_url(s)

    if _http_models_ok(models_url, timeout=5.0, authorization_bearer=s.vllm_api_key):
        logger.info("pipeline: vLLM already reachable at %s", models_url)
        return

    launched = False
    if unit:
        active = _systemctl_is_active(unit, user=False) or _systemctl_is_active(unit, user=True)
        if not active:
            logger.info("pipeline: attempting systemd start of %r", unit)
            launched = _systemctl_start(unit, user=False) or _systemctl_start(unit, user=True)
        else:
            logger.info("pipeline: %r already active via systemd; waiting for /v1/models", unit)
            launched = True

    # Fallback: run the Thomas start script directly (works without sudo)
    if not launched:
        logger.info("pipeline: systemd unavailable or no unit set; launching vLLM via direct script")
        launched = _launch_vllm_direct()

    if not launched:
        logger.warning(
            "pipeline: could not start vLLM automatically. "
            "Run: bash ~/thomas/scripts/start_vllm_qwen3_14b_awq.sh &"
        )
        return

    logger.info("pipeline: waiting for vLLM to become ready (up to %ss)…", int(timeout))
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if _http_models_ok(models_url, timeout=min(30.0, poll * 2), authorization_bearer=s.vllm_api_key):
            logger.info("pipeline: vLLM ready (%s)", models_url)
            return
        time.sleep(poll)

    logger.warning(
        "pipeline: vLLM not ready after %ss — check %s",
        int(timeout),
        models_url,
    )


def vllm_http_reachable(settings: Settings | None = None, *, timeout: float = 5.0) -> bool:
    """Return True if ``GET {VLLM_BASE_URL}/v1/models`` (or /models) returns 2xx."""
    from tariff_agent.utils.config import get_settings

    s = settings or get_settings()
    return _http_models_ok(_vllm_models_url(s), timeout=timeout, authorization_bearer=s.vllm_api_key)


def wait_for_vllm_http_soft(
    settings: Settings | None = None,
    *,
    total_timeout_sec: float = 600.0,
    poll_sec: float = 15.0,
) -> bool:
    """Poll until vLLM responds or ``total_timeout_sec`` elapses. Returns True if reachable."""
    from tariff_agent.utils.config import get_settings

    s = settings or get_settings()
    url = _vllm_models_url(s)
    deadline = time.monotonic() + max(1.0, float(total_timeout_sec))
    poll = max(2.0, float(poll_sec))
    while time.monotonic() < deadline:
        if _http_models_ok(url, timeout=min(30.0, poll), authorization_bearer=s.vllm_api_key):
            return True
        time.sleep(poll)
    return False


def wait_for_vllm_http(settings: Settings | None = None, *, timeout_sec: float | None = None) -> None:
    """Block until ``GET {VLLM_BASE_URL}/models`` succeeds. Raises ``RuntimeError`` if it never does."""
    from tariff_agent.utils.config import get_settings

    s = settings or get_settings()
    url = _vllm_models_url(s)
    limit = timeout_sec
    if limit is None:
        limit = max(120.0, float(s.pipeline_vllm_start_timeout_sec))
    poll = max(2.0, min(15.0, limit / 80.0))
    deadline = time.monotonic() + limit
    logger.info("pipeline: waiting for vLLM at %s (timeout %ss)", url, int(limit))
    while time.monotonic() < deadline:
        if _http_models_ok(url, timeout=min(45.0, poll * 2), authorization_bearer=s.vllm_api_key):
            logger.info("pipeline: vLLM OK (%s)", url)
            return
        time.sleep(poll)
    raise RuntimeError(
        f"vLLM not reachable at {url} after {int(limit)}s. "
        "After Docling, start Thomas (e.g. sudo systemctl start thomas-vllm) or fix VLLM_BASE_URL."
    )
