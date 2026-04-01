"""
update_all.py — Executa tots els scrapers de normativa en ordre,
gestiona errors individualment i mostra un resum final.

Usage:
    python update_all.py                    # incremental (per defecte)
    python update_all.py --full             # re-scraping complet
    python update_all.py --only adif        # nomes un scraper
    python update_all.py --only adif iso    # diversos scrapers
    python update_all.py --exclude adif une # tots menys els indicats
    python update_all.py --dry-run          # mostra que faria sense executar
    python update_all.py --help             # ajuda
"""
from __future__ import annotations

import io
import sys

# Assegurar UTF-8 en consoles Windows
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
else:
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

import argparse
import json
import time
import traceback
from datetime import datetime
from pathlib import Path

from config import PROJECT_ROOT, CATALOGS_DIR

# ─── Scrapers disponibles ────────────────────────────────────────────────────
# Cada entrada: (clau, label, funcio_import, dies_entre_execucions)
# La funcio s'importa lazily per evitar carregar tots els moduls al inici.

SCRAPER_DEFS = [
    ("dgc",       "DGC Ministeri Transport",    30),
    ("aca",       "ACA (Ag. Catalana Aigua)",   60),
    ("boe",       "BOE OpenData",               30),
    ("cte",       "CTE Edificacio",             90),
    ("era",       "ERA / CENELEC Ferroviari",   60),
    ("industria", "Industria.gob.es",           30),
    ("iso",       "ISO Open Data",               7),
    ("une",       "UNE (curl_cffi)",            14),
    ("pjcat",     "Portal Juridic Catalunya",   30),
    ("adif",      "ADIF NTEs",                  60),
    ("rebt",      "REBT / RITE Electric",       60),
]

VALID_NAMES = {s[0] for s in SCRAPER_DEFS}

# ─── State helpers (cache incremental) ───────────────────────────────────────

STATE_FILE = PROJECT_ROOT / "data" / "normativa_update_state.json"


def _load_state() -> dict:
    if STATE_FILE.exists():
        try:
            with open(STATE_FILE, encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            pass
    return {}


def _save_state(state: dict) -> None:
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(STATE_FILE, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)


def _days_since(iso_str: str) -> float:
    try:
        last = datetime.fromisoformat(iso_str)
        return (datetime.now() - last).total_seconds() / 86400
    except Exception:
        return float("inf")


# ─── Lazy import de cada scraper ─────────────────────────────────────────────

def _get_scraper_func(name: str):
    """Importa i retorna la funcio principal de l'scraper indicat."""
    if name == "dgc":
        from scrapers.norm_scraper import scrape_all
        return scrape_all
    elif name == "aca":
        from scrapers.aca_scraper import main
        return main
    elif name == "boe":
        from scrapers.boe_scraper import main
        return main
    elif name == "cte":
        from scrapers.cte_scraper import build_catalog
        return build_catalog
    elif name == "era":
        from scrapers.era_scraper import build_catalog
        return build_catalog
    elif name == "industria":
        from scrapers.industria_scraper import scrape_all
        return scrape_all
    elif name == "iso":
        from scrapers.iso_catalog import main
        return main
    elif name == "une":
        from scrapers.une_catalog import main
        return main
    elif name == "pjcat":
        from scrapers.pjcat_scraper import main
        return main
    elif name == "adif":
        from scrapers.adif_scraper import scrape_all
        return scrape_all
    elif name == "rebt":
        from scrapers.rebt_rite_scraper import main
        return main
    else:
        raise ValueError(f"Scraper desconegut: {name}")


# ─── Format helpers ──────────────────────────────────────────────────────────

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
    if d < 0.04:
        return "ara"
    if d < 1:
        return f"fa {d * 24:.0f}h"
    return f"fa {d:.0f} dies"


def _count_catalog_entries(name: str) -> int:
    """Compta entrades al cataleg JSON d'un scraper."""
    source_key = name if name != "rebt" else "industria"
    catalog_dir = CATALOGS_DIR / source_key
    if not catalog_dir.exists():
        return 0
    jsons = list(catalog_dir.glob("catalogo_*.json"))
    if not jsons:
        return 0
    try:
        with open(jsons[0], encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, list):
            return len(data)
        elif isinstance(data, dict):
            for k in ("documents", "entries", "norms", "normes", "catalogo", "data"):
                if k in data and isinstance(data[k], list):
                    return len(data[k])
            return len(data)
    except Exception:
        return 0


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
        help=f"Executa nomes el/s scraper/s indicat/s. "
             f"Noms valids: {', '.join(sorted(VALID_NAMES))}",
    )
    parser.add_argument(
        "--exclude", nargs="+", metavar="NAME",
        help=f"Exclou el/s scraper/s indicat/s. "
             f"Noms valids: {', '.join(sorted(VALID_NAMES))}",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Mostra que faria sense executar cap scraper",
    )
    parser.add_argument(
        "--verbose", action="store_true",
        help="Mostra tracebacks complets en cas d'error",
    )
    args = parser.parse_args()

    # Validar noms
    if args.only:
        bad = [n for n in args.only if n not in VALID_NAMES]
        if bad:
            parser.error(
                f"Nom(s) desconegut(s): {', '.join(bad)}. "
                f"Valids: {', '.join(sorted(VALID_NAMES))}"
            )
    if args.exclude:
        bad = [n for n in args.exclude if n not in VALID_NAMES]
        if bad:
            parser.error(
                f"Nom(s) desconegut(s): {', '.join(bad)}. "
                f"Valids: {', '.join(sorted(VALID_NAMES))}"
            )

    # Filtrar scrapers
    scrapers = list(SCRAPER_DEFS)
    if args.only:
        only_set = set(args.only)
        scrapers = [s for s in scrapers if s[0] in only_set]
    if args.exclude:
        exclude_set = set(args.exclude)
        scrapers = [s for s in scrapers if s[0] not in exclude_set]

    mode = "COMPLET" if args.full else "INCREMENTAL"
    now = datetime.now()

    print("=" * 60)
    print(f"  ACTUALITZACIO CATALEGS NORMATIUS")
    print(f"  Mode: {mode}  |  Data: {now.strftime('%Y-%m-%d %H:%M')}")
    print(f"  Scrapers: {len(scrapers)}")
    print("=" * 60)

    state = _load_state()
    results: list[tuple[str, str, str, str]] = []  # (name, label, status, detail)
    total_start = time.time()

    for i, (name, label, days) in enumerate(scrapers, 1):
        prefix = f"[{i}/{len(scrapers)}]"

        # Check incremental cache
        prev = state.get(name, {})
        last_run = prev.get("last_run", "")
        prev_entries = prev.get("entries", 0)
        elapsed = _days_since(last_run)

        if not args.full and elapsed < days:
            print(
                f"\n{prefix} [SKIP] {label} — Al dia "
                f"(executat {_fmt_days(elapsed)}, "
                f"{_fmt_entries(prev_entries)} entrades)"
            )
            results.append((name, label, "SKIP", f"{_fmt_entries(prev_entries)} entrades"))
            continue

        if args.dry_run:
            reason = "sempre" if args.full else f"caducat ({_fmt_days(elapsed)})"
            print(f"\n{prefix} [DRY-RUN] {label} — S'executaria ({reason})")
            results.append((name, label, "DRY-RUN", reason))
            continue

        # Executar scraper
        print(f"\n{'='*60}")
        print(f"{prefix} [SCRAPING] {label}")
        print(f"{'='*60}")

        t0 = time.time()
        try:
            func = _get_scraper_func(name)
            func()
            elapsed_s = time.time() - t0
            entries = _count_catalog_entries(name)

            print(f"\n[OK] {label} completat en {_fmt_duration(elapsed_s)}"
                  f" — {_fmt_entries(entries)} entrades")

            # Actualitzar state
            state[name] = {
                "last_run": datetime.now().isoformat(),
                "entries": entries if entries > 0 else prev_entries,
                "status": "ok",
            }
            _save_state(state)

            results.append((
                name, label, "OK",
                f"{_fmt_duration(elapsed_s)}, {_fmt_entries(entries)} entrades"
            ))

        except Exception as exc:
            elapsed_s = time.time() - t0
            err_msg = str(exc)[:120]
            print(f"\n[ERROR] {label}: {err_msg}")
            if args.verbose:
                traceback.print_exc()
            if prev_entries:
                print(f"  El cataleg anterior es mante ({_fmt_entries(prev_entries)} entrades)")

            results.append((name, label, "ERROR", err_msg))

    total_duration = time.time() - total_start

    # ── Resum final ──
    print()
    print("=" * 60)
    print("RESUM SCRAPE COMPLET")
    print("=" * 60)
    for name, label, status, detail in results:
        print(f"  {label:30s} {status:8s} {detail}")
    print(f"\n  Temps total: {_fmt_duration(total_duration)}")
    print("=" * 60)


if __name__ == "__main__":
    main()
