"""
update_all.py — Executa tots els scrapers de normativa en ordre,
gestiona errors individualment i mostra un resum final.

Usage:
    python update_all.py              # incremental (per defecte)
    python update_all.py --full       # re-scraping complet
    python update_all.py --only une   # només un scraper específic
    python update_all.py --only adif iso  # diversos scrapers
    python update_all.py --dry-run    # mostra què faria sense executar
    python update_all.py --help       # ajuda
"""
from __future__ import annotations

import io
import sys

# Ensure UTF-8 output on Windows consoles
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
else:
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

import argparse
import json
import os
import re
import subprocess
import time
from datetime import datetime

# ─── Constants ────────────────────────────────────────────────────────────────

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

STATE_FILE = os.path.join(BASE_DIR, "data", "normativa_update_state.json")

SCRAPERS = [
    {
        "name":   "dgc",
        "script": os.path.join("scrapers", "norm_scraper.py"),
        "label":  "DGC Ministeri Transport",
        "days":   30,
    },
    {
        "name":   "adif",
        "script": os.path.join("scrapers", "adif_scraper.py"),
        "label":  "ADIF NTEs",
        "days":   60,
    },
    {
        "name":   "iso",
        "script": os.path.join("scrapers", "iso_catalog.py"),
        "label":  "ISO open data",
        "days":   7,
    },
    {
        "name":   "une",
        "script": os.path.join("scrapers", "une_catalog.py"),
        "label":  "UNE (curl_cffi)",
        "days":   14,
    },
    {
        "name":   "industria",
        "script": os.path.join("scrapers", "industria_scraper.py"),
        "label":  "Industria.gob.es",
        "days":   30,
    },
    {
        "name":   "aca",
        "script": os.path.join("scrapers", "aca_scraper.py"),
        "label":  "ACA (Ag. Catalana Aigua)",
        "days":   60,
    },
    {
        "name":   "era",
        "script": os.path.join("scrapers", "era_scraper.py"),
        "label":  "ERA / CENELEC Ferroviari",
        "days":   60,
    },
    {
        "name":   "mitma",
        "script": os.path.join("scrapers", "industria_scraper.py"),
        "label":  "MITMA Ferroviari",
        "days":   90,
    },
]

VALID_NAMES = {s["name"] for s in SCRAPERS}

SCRAPER_TIMEOUT = 3600  # 1 hour max per scraper


# ─── State helpers ────────────────────────────────────────────────────────────

def _load_state() -> dict:
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            pass
    return {}


def _save_state(state: dict) -> None:
    with open(STATE_FILE, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)


def _days_since(iso_str: str) -> float:
    """Days elapsed since an ISO-8601 timestamp."""
    try:
        last = datetime.fromisoformat(iso_str)
        return (datetime.now() - last).total_seconds() / 86400
    except Exception:
        return float("inf")


# ─── Output parsing ──────────────────────────────────────────────────────────

_ENTRY_PATTERNS = [
    re.compile(r"(\d[\d.,]+)\s+documents?", re.I),
    re.compile(r"[Tt]otal[:\s]+(\d[\d.,]+)"),
    re.compile(r"[Ll]oaded\s+(\d[\d.,]+)"),
    re.compile(r"(\d[\d.,]+)\s+entrades?", re.I),
    re.compile(r"(\d[\d.,]+)\s+normes?", re.I),
    re.compile(r"Total UNE[^:]*:\s*(\d[\d.,]+)", re.I),
    re.compile(r"Total entrades:\s*(\d[\d.,]+)", re.I),
    re.compile(r"Guardat\s+(\d[\d.,]+)", re.I),
    re.compile(r"Total normes?\s+\w+:\s*(\d[\d.,]+)", re.I),
]


def _parse_entries_from_output(output: str) -> int:
    """Extract entry count from scraper stdout."""
    best = 0
    for pat in _ENTRY_PATTERNS:
        for m in pat.finditer(output):
            raw = m.group(1).replace(".", "").replace(",", "")
            try:
                val = int(raw)
                if val > best:
                    best = val
            except ValueError:
                pass
    return best


# ─── Scraper execution ───────────────────────────────────────────────────────

# Lines containing these patterns are always shown (even in quiet mode)
# to give a sense of progress without flooding the console.
_PROGRESS_PATTERNS = [
    re.compile(r"^\s*\[", re.I),                    # [UNE], [ADIF], …
    re.compile(r"total", re.I),                      # "Total: 1496"
    re.compile(r"guardat|saved|wrote", re.I),        # file saved
    re.compile(r"completat|finished|done", re.I),    # finished
    re.compile(r"\d+\s+(normes?|entrades?|documents?)", re.I),
    re.compile(r"ICS\s+\d+", re.I),                  # UNE ICS progress
    re.compile(r"pag(ina)?\s*\d+", re.I),            # pagination
    re.compile(r"error|warning|timeout", re.I),       # problems
]


def _is_progress_line(line: str) -> bool:
    """True if this line is interesting enough to show in quiet mode."""
    return any(p.search(line) for p in _PROGRESS_PATTERNS)


def _run_scraper(
    name: str,
    script: str,
    label: str,
    verbose: bool = False,
) -> dict:
    """Run a scraper as a subprocess with real-time log streaming.

    Three output modes:
      --verbose : every line printed as-is
      default   : only progress lines shown (prefixed with scraper tag)
      (neither streams nor captures — we always stream + capture)
    """
    script_path = os.path.join(BASE_DIR, script)
    if not os.path.exists(script_path):
        return {
            "name": name, "label": label,
            "status": "missing", "duration_s": 0,
            "entries": 0, "error": f"Script no trobat: {script}",
        }

    tag = f"  [{name.upper()}]"
    cmd = [sys.executable, "-u", script_path]   # -u = unbuffered
    env = os.environ.copy()
    env["PYTHONIOENCODING"] = "utf-8"

    captured_lines: list[str] = []
    start = time.time()

    try:
        proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            encoding="utf-8",
            errors="replace",
            cwd=BASE_DIR,
            env=env,
        )

        assert proc.stdout is not None
        for raw_line in proc.stdout:
            line = raw_line.rstrip("\n\r")
            captured_lines.append(line)
            if verbose:
                print(f"{tag} {line}", flush=True)
            elif _is_progress_line(line):
                print(f"{tag} {line}", flush=True)

        proc.wait(timeout=SCRAPER_TIMEOUT)
        duration = time.time() - start
        all_output = "\n".join(captured_lines)

        if proc.returncode != 0:
            err_snippet = all_output[-500:]
            return {
                "name": name, "label": label,
                "status": "error", "duration_s": duration,
                "entries": 0, "error": err_snippet,
            }

        entries = _parse_entries_from_output(all_output)
        return {
            "name": name, "label": label,
            "status": "ok", "duration_s": duration,
            "entries": entries, "error": None,
        }

    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait()
        return {
            "name": name, "label": label,
            "status": "timeout", "duration_s": SCRAPER_TIMEOUT,
            "entries": 0, "error": "Timeout (>1h)",
        }
    except Exception as exc:
        return {
            "name": name, "label": label,
            "status": "error", "duration_s": time.time() - start,
            "entries": 0, "error": str(exc),
        }


# ─── Formatting helpers ──────────────────────────────────────────────────────

def _fmt_duration(secs: float) -> str:
    if secs <= 0:
        return "-"
    if secs < 60:
        return f"{secs:.0f}s"
    m, s = divmod(int(secs), 60)
    return f"{m}m {s:02d}s"


def _fmt_entries(n: int) -> str:
    if n <= 0:
        return "-"
    return f"{n:,}"


def _fmt_days(d: float) -> str:
    if d == float("inf"):
        return "mai"
    if d < 0.04:  # < 1 hour
        return "ara"
    if d < 1:
        return f"fa {d * 24:.0f}h"
    return f"fa {d:.0f} dies"


# ─── Main ────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Actualitza tots els catalegs normatius del projecte.",
    )
    parser.add_argument(
        "--full", action="store_true",
        help="Forca re-scraping complet (ignora cache incremental)",
    )
    parser.add_argument(
        "--only", nargs="+", metavar="NAME",
        help=f"Executa nomes el/s scraper/s indicat/s.  "
             f"Noms valids: {', '.join(sorted(VALID_NAMES))}",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Mostra que faria sense executar cap scraper",
    )
    parser.add_argument(
        "--verbose", action="store_true",
        help="Mostra output complet de cada scraper",
    )
    parser.add_argument(
        "--no-index", action="store_true",
        help="No executa norm_indexer.py al final",
    )
    args = parser.parse_args()

    # Validate --only names
    if args.only:
        bad = [n for n in args.only if n not in VALID_NAMES]
        if bad:
            parser.error(
                f"Nom(s) desconegut(s): {', '.join(bad)}.  "
                f"Valids: {', '.join(sorted(VALID_NAMES))}"
            )

    mode = "COMPLET" if args.full else "INCREMENTAL"
    now = datetime.now()

    print("=" * 60)
    print(f"  ACTUALITZACIO CATALEGS NORMATIUS")
    print(f"  Mode: {mode}  |  Data: {now.strftime('%Y-%m-%d %H:%M')}")
    print("=" * 60)

    state = _load_state()
    results: list[dict] = []
    total_start = time.time()

    scrapers = SCRAPERS
    if args.only:
        scrapers = [s for s in SCRAPERS if s["name"] in args.only]

    for i, scraper in enumerate(scrapers, 1):
        name   = scraper["name"]
        script = scraper["script"]
        label  = scraper["label"]
        days   = scraper["days"]

        prefix = f"[{i}/{len(scrapers)}]"

        # Check if script exists
        script_path = os.path.join(BASE_DIR, script)
        if not os.path.exists(script_path):
            print(f"\n{prefix} \u26a0 {label} — Script no trobat ({script}), omes")
            results.append({
                "name": name, "label": label,
                "status": "missing", "duration_s": 0,
                "entries": 0, "error": "Script no trobat",
            })
            continue

        # Incremental check
        prev = state.get(name, {})
        last_run = prev.get("last_run", "")
        prev_entries = prev.get("entries", 0)
        elapsed = _days_since(last_run)

        if not args.full and elapsed < days:
            print(
                f"\n{prefix} \u2713 {label} — Al dia "
                f"(executat {_fmt_days(elapsed)}, "
                f"{_fmt_entries(prev_entries)} entrades)"
            )
            results.append({
                "name": name, "label": label,
                "status": "skip", "duration_s": 0,
                "entries": prev_entries, "error": None,
                "_elapsed": elapsed,
            })
            continue

        if args.dry_run:
            reason = "sempre" if args.full else f"caducat ({_fmt_days(elapsed)})"
            print(f"\n{prefix} [DRY-RUN] {label} — S'executaria ({reason})")
            results.append({
                "name": name, "label": label,
                "status": "dry-run", "duration_s": 0,
                "entries": prev_entries, "error": None,
            })
            continue

        # Run the scraper
        print(f"\n{prefix} \U0001f504 {label}...", flush=True)
        res = _run_scraper(name, script, label, verbose=args.verbose)
        results.append(res)

        if res["status"] == "ok":
            print(
                f"  \u2705 Completat en {_fmt_duration(res['duration_s'])} "
                f"— {_fmt_entries(res['entries'])} entrades"
            )
            # Update state
            entries = res["entries"] if res["entries"] > 0 else prev_entries
            state[name] = {
                "last_run": datetime.now().isoformat(),
                "entries": entries,
                "status": "ok",
            }
            _save_state(state)
        elif res["status"] == "timeout":
            print(
                f"  \u274c Timeout — el cataleg anterior es mante "
                f"({_fmt_entries(prev_entries)} entrades)"
            )
        else:
            err_short = (res.get("error") or "desconegut")[:120]
            print(f"  \u274c Error: {err_short}")
            if prev_entries:
                print(
                    f"      El cataleg anterior es mante "
                    f"({_fmt_entries(prev_entries)} entrades)"
                )

    # ── Optional: re-index ──
    indexer = os.path.join(BASE_DIR, "norm_indexer.py")
    if not args.no_index and not args.dry_run and os.path.exists(indexer):
        print(f"\n\U0001f504 Actualitzant index normatiu (norm_indexer.py)...")
        idx_res = _run_scraper("index", "norm_indexer.py", "Index normatiu",
                               verbose=args.verbose)
        results.append(idx_res)
        if idx_res["status"] == "ok":
            print(f"  \u2705 Completat en {_fmt_duration(idx_res['duration_s'])}")
        else:
            err_short = (idx_res.get("error") or "desconegut")[:120]
            print(f"  \u274c Error: {err_short}")

    total_duration = time.time() - total_start

    # ── Summary table ──
    print()
    print("=" * 60)
    print("  RESUM")
    print("=" * 60)

    hdr = f"{'Scraper':<22} | {'Estat':<10} | {'Entrades':>10} | {'Temps':>7} | Ultima exec."
    print(hdr)
    print("-" * len(hdr))

    total_entries = 0
    for r in results:
        name  = r["label"][:22]
        st    = r["status"]
        ent   = r.get("entries", 0)
        dur   = r.get("duration_s", 0)

        # Determine display values
        if st == "ok":
            estat_str = "\u2705 OK"
            last_str  = "ara"
        elif st == "skip":
            estat_str = "\u23ed Skip"
            last_str  = _fmt_days(r.get("_elapsed", float("inf")))
        elif st == "dry-run":
            estat_str = "\U0001f4cb Pendent"
            last_str  = "-"
        elif st == "missing":
            estat_str = "\u26a0 Absent"
            last_str  = "-"
        elif st == "timeout":
            estat_str = "\u274c Timeout"
            last_str  = "-"
        else:
            estat_str = "\u274c Error"
            last_str  = "-"

        ent_str = _fmt_entries(ent) if ent > 0 else "-"
        dur_str = _fmt_duration(dur) if st == "ok" else "-"

        if st in ("error", "timeout") and ent > 0:
            ent_str += "*"

        total_entries += ent if ent > 0 else 0

        print(f"{name:<22} | {estat_str:<10} | {ent_str:>10} | {dur_str:>7} | {last_str}")

    if any(r["status"] in ("error", "timeout") for r in results):
        print("(* cataleg anterior mantingut)")

    print()
    print(f"  Total entrades als catalegs: {total_entries:,}")
    print(f"  Temps total: {_fmt_duration(total_duration)}")
    print("=" * 60)


if __name__ == "__main__":
    main()
