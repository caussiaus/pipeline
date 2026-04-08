#!/usr/bin/env python3
"""Find what is bound to VLLM_BASE_URL (default :8000) and optionally stop it to free GPU VRAM for Docling.

If you use **Thomas** (`~/thomas`), vLLM is normally `thomas-vllm.service` →
`scripts/start_vllm_qwen3_14b_awq.sh`. Stopping the **systemd unit** first avoids
`Restart=always` bringing the process back after a naive kill.

Run with **python3** (or `source .venv/bin/activate` if your venv provides `python`).
"""
from __future__ import annotations

import argparse
import os
import re
import subprocess
import sys
from pathlib import Path
from urllib.parse import urlparse

_THOMAS = Path.home() / "thomas"
# Repo-relative hints (may exist on developer machines; ignored if missing).
_RESTART_HINT_PATHS = [
    _THOMAS / "docs/VLLM_OPTION2_SETUP.md",
    _THOMAS / "scripts/start_vllm_qwen3_14b_awq.sh",
    _THOMAS / "thomas-vllm.service",
    Path.home() / "claw/docs/OPERATORS_README.md",
    Path.home() / "ollama-mcp/configs/docker-compose.awq.yaml",
    Path.home() / "vllm-tutorial/deployment/docker-compose.yml",
]

_THOMAS_UNIT = "thomas-vllm.service"


def _systemd_unit_active(unit: str, *, user: bool) -> bool:
    cmd = ["systemctl", "--user", "is-active", unit] if user else ["systemctl", "is-active", unit]
    return _run(cmd).stdout.strip() == "active"


def _stop_thomas_vllm_systemd() -> bool:
    """Stop Thomas vLLM if the systemd unit is active. Returns True if a stop was attempted successfully."""
    import time

    for user in (False, True):
        if not _systemd_unit_active(_THOMAS_UNIT, user=user):
            continue
        scope = "--user" if user else "system"
        base = ["systemctl", "--user"] if user else ["systemctl"]
        print(f"Stopping {_THOMAS_UNIT} ({scope}) …")
        r = subprocess.run([*base, "stop", _THOMAS_UNIT], capture_output=True, text=True, timeout=120)
        if r.returncode == 0:
            time.sleep(2)
            return True
        if not user and r.stderr:
            print(r.stderr.strip())
        if not user:
            print("  Trying: sudo systemctl stop thomas-vllm …")
            r2 = subprocess.run(
                ["sudo", "systemctl", "stop", "thomas-vllm"],
                capture_output=True,
                text=True,
                timeout=120,
            )
            if r2.returncode == 0:
                time.sleep(2)
                return True
            if r2.stderr:
                print(r2.stderr.strip())
    return False


def _print_thomas_block() -> None:
    print("Thomas vLLM (~/thomas) — canonical Option 2:")
    svc = _THOMAS / "thomas-vllm.service"
    start_sh = _THOMAS / "scripts/start_vllm_qwen3_14b_awq.sh"
    if svc.is_file():
        print(f"  systemd unit file: {svc}")
    if start_sh.is_file():
        print(f"  manual start: cd {_THOMAS} && bash scripts/start_vllm_qwen3_14b_awq.sh")
    sys_active = _systemd_unit_active(_THOMAS_UNIT, user=False)
    user_active = _systemd_unit_active(_THOMAS_UNIT, user=True)
    if sys_active or user_active:
        print(f"  systemd status: system={sys_active!r} user={user_active!r}")
        print("  stop GPU:  sudo systemctl stop thomas-vllm")
        print("  start again: sudo systemctl start thomas-vllm")
    else:
        print(f"  systemd: `{_THOMAS_UNIT}` not active (or not installed). If you use it elsewhere:")
        print("    sudo systemctl stop thomas-vllm && sudo systemctl start thomas-vllm")
    print()


def _project_root() -> Path:
    return Path(__file__).resolve().parents[1]


def _load_env_url() -> str:
    try:
        from dotenv import load_dotenv
    except ImportError:
        load_dotenv = None  # type: ignore[assignment]
    if load_dotenv is not None:
        load_dotenv(_project_root() / ".env", override=False)
    return (os.environ.get("VLLM_BASE_URL") or "http://127.0.0.1:8000/v1").strip()


def _parse_host_port(url_s: str) -> tuple[str, int]:
    u = urlparse(url_s if "://" in url_s else f"http://{url_s}")
    host = u.hostname or "127.0.0.1"
    port = u.port or (8000 if u.scheme in ("http", "") else 443)
    return host, port


def _run(cmd: list[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(cmd, capture_output=True, text=True, timeout=60)


def _pids_listening_on_port(port: int) -> dict[int, str]:
    """Parse `ss -ltnp` for TCP listeners on port."""
    out: dict[int, str] = {}
    for ss_bin in ("ss",):
        proc = _run(["ss", "-ltnp"])
        if proc.returncode != 0:
            continue
        # Listens on *:8000 or 0.0.0.0:8000 or [::]:8000
        pat = re.compile(rf":{port}\s+.*\busers:\(\(\"([^\"]+)\"\,pid=(\d+)")
        for line in proc.stdout.splitlines():
            if f":{port}" not in line:
                continue
            m = pat.search(line)
            if m:
                out[int(m.group(2))] = m.group(1)
    if not out:
        proc = _run(["lsof", "-i", f"TCP:{port}", "-sTCP:LISTEN", "-t"])
        if proc.returncode == 0 and proc.stdout.strip():
            for pid_s in proc.stdout.strip().split():
                out[int(pid_s)] = "?"
    return out


def _cmdline(pid: int) -> str:
    try:
        raw = Path(f"/proc/{pid}/cmdline").read_bytes()
        return raw.replace(b"\0", b" ").decode(errors="replace").strip()
    except OSError:
        proc = _run(["ps", "-p", str(pid), "-o", "args="])
        return proc.stdout.strip() if proc.returncode == 0 else ""


def _docker_containers_for_port(port: int) -> list[tuple[str, str]]:
    proc = _run(["docker", "ps", "--format", "{{.ID}}\t{{.Names}}\t{{.Ports}}"])
    if proc.returncode != 0:
        return []
    hits: list[tuple[str, str]] = []
    port_markers = (f":{port}->", f"0.0.0.0:{port}->", f"[::]:{port}->")
    for line in proc.stdout.splitlines():
        if not line.strip():
            continue
        parts = line.split("\t", 2)
        if len(parts) < 3:
            continue
        cid, name, ports = parts[0], parts[1], parts[2]
        if any(m in ports for m in port_markers):
            hits.append((cid, name))
    return hits


def _pgrep_vllm() -> list[tuple[int, str]]:
    proc = _run(["pgrep", "-af", "vllm"])
    if proc.returncode != 0 and not proc.stdout.strip():
        return []
    rows: list[tuple[int, str]] = []
    skip = ("cursor_sandbox", "dump_bash_state", "vllm_release_gpu.py", "COMMAND_EXIT_CODE")
    for line in proc.stdout.splitlines():
        line = line.strip()
        if not line or any(s in line for s in skip):
            continue
        m = re.match(r"^(\d+)\s+(.*)$", line)
        if m:
            rows.append((int(m.group(1)), m.group(2).strip()))
    return rows


def _nvidia_compute_pids() -> list[str]:
    proc = _run(
        [
            "nvidia-smi",
            "--query-compute-apps=pid,process_name,used_gpu_memory",
            "--format=csv,noheader",
        ]
    )
    if proc.returncode != 0:
        return []
    return [ln.strip() for ln in proc.stdout.splitlines() if ln.strip()]


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument(
        "--url",
        default="",
        help="Override VLLM OpenAI base URL (else VLLM_BASE_URL from .env)",
    )
    ap.add_argument(
        "--stop",
        action="store_true",
        help="Stop listeners: docker containers mapping this port first, else SIGTERM PIDs from ss/lsof",
    )
    ap.add_argument(
        "--yes",
        action="store_true",
        help="With --stop, do not prompt",
    )
    ap.add_argument(
        "--signal",
        choices=("TERM", "KILL"),
        default="TERM",
        help="Signal for non-Docker PIDs (default TERM)",
    )
    ap.add_argument(
        "--force-kill-port",
        action="store_true",
        help="With --stop, SIGTERM any PID still listening on the port (careful if port is not vLLM)",
    )
    ap.add_argument(
        "--skip-thomas-systemd",
        action="store_true",
        help="With --stop, do not try systemctl stop thomas-vllm (only docker + signals)",
    )
    args = ap.parse_args()

    url = args.url.strip() or _load_env_url()
    host, port = _parse_host_port(url)

    print(f"VLLM_BASE_URL → host={host!r} port={port}")
    print()

    if _THOMAS.is_dir():
        _print_thomas_block()

    dc = _docker_containers_for_port(port)
    if dc:
        print("Docker containers publishing this port on the host:")
        for cid, name in dc:
            print(f"  {cid[:12]}  name={name!r}")
        print("  Stop: docker stop " + " ".join(n for _, n in dc))
    else:
        print("Docker: no `docker ps` match for this port (or docker not available).")
    print()

    lp = _pids_listening_on_port(port)
    if lp:
        print(f"Processes listening on TCP {port}:")
        for pid, exe in sorted(lp.items()):
            cmd = _cmdline(pid)
            print(f"  pid={pid} program={exe!r}")
            if cmd:
                print(f"       cmd: {cmd[:500]}{'…' if len(cmd) > 500 else ''}")
    else:
        print(f"No TCP listener found on port {port} (ss/lsof).")
    print()

    vllm_procs = _pgrep_vllm()
    if vllm_procs:
        print("pgrep processes matching 'vllm':")
        for pid, cmd in vllm_procs:
            print(f"  pid={pid}: {cmd[:500]}{'…' if len(cmd) > 500 else ''}")
    else:
        print("pgrep: no process matching pattern 'vllm'.")
    print()

    nv = _nvidia_compute_pids()
    if nv:
        print("nvidia-smi compute apps (pid, name, VRAM):")
        for ln in nv[:40]:
            print(f"  {ln}")
        if len(nv) > 40:
            print(f"  … ({len(nv) - 40} more)")
    else:
        print("nvidia-smi: no compute apps or nvidia-smi unavailable.")
    print()

    print("Docs / compose files that may describe how you start vLLM (if present):")
    for p in _RESTART_HINT_PATHS:
        if p.is_file():
            print(f"  {p}")
    print("  (native) often: python3 -m vllm.entrypoints.openai.api_server ... --port " + str(port))
    claw = Path.home() / "claw"
    if (claw / "claw").is_file() or (claw / "claw").is_symlink():
        print(f"  (CLAW) {claw}: ./claw stop slot_b / ./claw start vllm — see {claw / 'docs/OPERATORS_README.md'}")
    print()

    thomas_launcher = _THOMAS / "scripts/start_vllm_qwen3_14b_awq.sh"
    for pid in sorted(lp):
        cmd = _cmdline(pid)
        if "vllm" in cmd.lower() and ("serve" in cmd or "api_server" in cmd or "openai" in cmd):
            if "thomas" in cmd.lower() or "start_vllm_qwen3" in cmd:
                print("Listener matches Thomas launcher — use systemd when possible:")
                if thomas_launcher.is_file():
                    print(f"  bash {thomas_launcher}")
            else:
                print("Current vLLM listener command (manual / non-Thomas start):")
                print(f"  {cmd}")
            print()
            break

    if not args.stop:
        print("To free VRAM:")
        print("  python3 scripts/vllm_release_gpu.py --stop --yes")
        print("Thomas: prefer `sudo systemctl stop thomas-vllm` (see block above).")
        print("  Optional: --force-kill-port, --skip-thomas-systemd")
        return 0

    if not args.yes:
        try:
            r = input("Proceed with --stop? [y/N] ")
        except EOFError:
            r = ""
        if r.strip().lower() not in ("y", "yes"):
            print("Aborted.")
            return 1

    import signal
    import time

    sig = signal.SIGTERM if args.signal == "TERM" else signal.SIGKILL

    if not args.skip_thomas_systemd and (
        _systemd_unit_active(_THOMAS_UNIT, user=False)
        or _systemd_unit_active(_THOMAS_UNIT, user=True)
    ):
        _stop_thomas_vllm_systemd()
        time.sleep(1)

    for _cid, name in dc:
        print(f"docker stop {name!r} …")
        dr = _run(["docker", "stop", name])
        if dr.returncode != 0 and dr.stderr:
            print(dr.stderr.strip())

    if dc:
        print("Waiting 3s for port to clear…")
        time.sleep(3)

    remaining = _pids_listening_on_port(port)
    for pid in sorted(remaining):
        cmd = _cmdline(pid)
        looks_vllm = "vllm" in cmd.lower() or "api_server" in cmd.lower()
        if not args.force_kill_port and not looks_vllm:
            print(
                f"skip pid={pid} (not vLLM-like); cmd: {cmd[:120]}… — use --force-kill-port to stop anyway"
            )
            continue
        try:
            print(f"kill -{args.signal} {pid}")
            os.kill(pid, sig)
        except ProcessLookupError:
            print(f"  (pid {pid} already gone)")
        except PermissionError:
            print(f"  (pid {pid} permission denied — try sudo)")

    time.sleep(1)
    print("Done. Verify: nvidia-smi")
    print(f"  curl -s http://{host}:{port}/v1/models  # should fail while server is down")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
