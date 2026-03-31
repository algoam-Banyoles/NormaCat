"""
une_catalog.py — Scrapes une.org for ICS codes relevant to civil engineering
and saves normativa_une/_catalogo/catalogo_une.json.

Real search mechanism (discovered via JS analysis):
  - The site uses a KQL (Keyword Query Language) engine, NOT ASP.NET form postback.
  - Button `idButton` calls JS Search() which posts `form1` to `encuentra-tu-norma`
    with a KQL query: e.g. (g:UNE) AND (e:VI) AND (i:91*)
  - Pagination: repost same params to `form2` with n=<page_number>

Key parameters:
  k   = KQL query string
  n   = page number (1 = first, 2 = second, …)
  m   = total results (from <span id="totalElementos"> in first response)
  p1  = "UNE@@" (norm type filter)
  p4  = "VI" (Vigente) | "AN" (Anulada)
  p7  = "<ics_code>@@<ics_display_name>"
  ptit = "" (free-text title, unused here)

Usage:
    python une_catalog.py [output_dir]   (default: normativa_une)

Dependencies: curl_cffi + beautifulsoup4 + stdlib.
"""

from __future__ import annotations

import io
import json
import sys

# Ensure UTF-8 output on Windows consoles
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
else:
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

import os
import re
import time

from curl_cffi import requests
from bs4 import BeautifulSoup

# ─── Constants ────────────────────────────────────────────────────────────────

BASE_URL   = "https://www.une.org"
SEARCH_URL = f"{BASE_URL}/encuentra-tu-norma"

OUTPUT_DIR   = "normativa_une"
CATALOG_PATH = os.path.join(OUTPUT_DIR, "_catalogo", "catalogo_une.json")
DELAY        = 1.5
IMPERSONATE  = "chrome120"

# ICS targets: (dropdown_value, ics_display_name_suffix_for_p7)
# The p7 parameter is "{value}@@{display_text from dropdown option}"
# Full civil engineering set (~40 codes, covers all disciplines)
ICS_TARGETS = [
    # ── Construcció i edificació ──────────────────────────────────────────
    ("91",     "EDIFICACION Y MATERIALES DE CONSTRUCCION"),
    ("9101",   "Industria de la construccion en general"),
    ("9104",   "Urbanismo. Planificacion urbana"),
    ("9106",   "Elementos constructivos"),
    ("9108",   "Estructuras de construccion"),
    ("91080",  "Estructuras de construccion. Cargas. Acciones exteriores"),
    ("91085",  "Estructuras metalicas"),
    ("9110",   "Materiales de construccion"),
    ("91100",  "Materiales de construccion en general"),
    ("91110",  "Madera. Madera aserrada y madera redonda"),
    ("91120",  "Proteccion de edificios"),
    ("91130",  "Construccion de edificios"),
    ("9114",   "Instalaciones de edificios"),
    ("91160",  "Iluminacion"),
    ("9118",   "Practica constructiva"),
    # ── Ingenieria civil ──────────────────────────────────────────────────
    ("93",     "INGENIERIA CIVIL"),
    ("9301",   "Ingenieria civil en general"),
    ("9302",   "Movimientos de tierras. Excavaciones. Cimentaciones. Obras subterraneas"),
    ("9303",   "Canalizacion de aguas exteriores"),
    ("9304",   "Alcantarillas exteriores"),
    ("9306",   "Construccion de carreteras"),
    ("93060",  "Carreteras en general"),
    ("93080",  "Ingenieria de carreteras"),
    ("9308",   "Construccion de vias ferreas"),
    ("93100",  "Construccion de vias ferreas"),
    ("9311",   "Construccion de aeropuertos"),
    ("9316",   "Construccion hidraulica"),
    ("9318",   "Ingenieria de trafico"),
    # ── Ferroviaria ───────────────────────────────────────────────────────
    ("45",     "INGENIERIA FERROVIARIA"),
    # ── Geotecnia i medi ambient ──────────────────────────────────────────
    ("13080",  "Calidad del suelo. Pedologia"),
    ("13060",  "Calidad del agua"),
    ("13030",  "Contaminacion del suelo"),
    ("1302",   "Calidad del aire"),
    # ── Acers i metalls ───────────────────────────────────────────────────
    ("77140",  "Productos de acero"),
    ("77",     "METALURGIA"),
    # ── Electrotecnia i instal·lacions ────────────────────────────────────
    ("29",     "INGENIERIA ELECTRICA"),
    ("2306",   "Canalizaciones y accesorios"),
    # ── Seguretat i salut ─────────────────────────────────────────────────
    ("1314",   "Seguridad. Proteccion contra incendios"),
    ("1322",   "Proteccion del suelo"),
    ("1342",   "Calidad del aire. Contaminacion del aire"),
]

# UNE reference regex (for result parsing)
UNE_REF_PAT = re.compile(
    r"(UNE(?:-EN)?(?:-ISO)?(?:/IEC)?\s+[\d][\w\s\-/:\.]+?:\d{4}(?:/\w+:\d{4})?)",
    re.IGNORECASE,
)

RESULTS_DIV_PATTERN = re.compile(r"divResultados")

# Max online successor lookups per run (avoid throttling)
MAX_ONLINE_LOOKUPS = 50


# ─── Base-ref helpers ────────────────────────────────────────────────────────

def _extract_base_ref(ref: str) -> str:
    """Strip year/edition suffix: 'UNE-EN 10025-2:2004' → 'UNE-EN 10025-2'."""
    # Strip +A1:YYYY, :YYYY, /YYYY and everything after
    base = re.sub(r"(?:\+[A-Z]\d+)?(?:[:/]\d{4}).*$", "", ref).strip()
    return base


def _extract_year(ref: str) -> int:
    """Extract publication year from reference, 0 if none."""
    m = re.search(r"[:/](\d{4})", ref)
    return int(m.group(1)) if m else 0


# ─── Session ──────────────────────────────────────────────────────────────────

def make_session() -> requests.Session:
    """Create a curl_cffi session with browser fingerprint."""
    session = requests.Session(impersonate=IMPERSONATE)
    session.headers.update({"Accept-Language": "es-ES,es;q=0.9,ca;q=0.8"})
    # Warm up: visit homepage to collect initial cookies
    try:
        session.get(BASE_URL, timeout=30)
        time.sleep(1)
    except Exception:
        pass
    return session


def get_ics_display_names(session: requests.Session) -> dict[str, str]:
    """
    GET the search page and extract the drpClasificacion dropdown option texts.
    Returns {value: display_text} for all ICS targets we care about.
    """
    resp = session.get(SEARCH_URL, timeout=30)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")
    sel  = soup.find("select", id=re.compile("drpClasificacion", re.I))
    if not sel:
        return {}
    names = {}
    for opt in sel.find_all("option"):
        val  = opt.get("value", "")
        text = opt.get_text(strip=True)
        if val:
            names[val] = text
    return names


# ─── Result parser ────────────────────────────────────────────────────────────

def parse_results_from_html(html: str) -> list[dict]:
    """
    Extract norm metadata.  Tries 3 strategies in order:
    1. Individual result item divs/lis (most structured)
    2. divResultados text split (original approach)
    3. Full-page regex scan (last resort)
    """
    soup = BeautifulSoup(html, "html.parser")
    results: list[dict] = []
    seen: set[str] = set()

    # --- Strategy 1: individual result elements ---
    items = (
        soup.find_all("div", class_=re.compile(r"resultado|result|norma", re.I))
        or soup.find_all("li", class_=re.compile(r"resultado|result|norma", re.I))
        or soup.find_all("article")
    )
    for item in items:
        text  = item.get_text("\n", strip=True)
        m_ref = UNE_REF_PAT.search(text)
        if not m_ref:
            continue
        ref = m_ref.group(0).strip()
        if ref in seen:
            continue
        seen.add(ref)
        m_estat = re.search(r"Estado[:\s]+(Vigente|Anulada)", text, re.I)
        m_data  = re.search(r"(\d{4}-\d{2}-\d{2})", text)
        estat = ""
        if m_estat:
            estat = "VIGENT" if m_estat.group(1).lower() == "vigente" else "ANULADA"
        lines      = [l.strip() for l in text.split("\n") if l.strip()]
        descripcio = ""
        ctn        = ""
        for j, line in enumerate(lines):
            if "estado" in line.lower() and j + 1 < len(lines):
                candidate = lines[j + 1]
                if not re.match(r"(UNE|CTN|Estado)", candidate, re.I):
                    descripcio = candidate[:200]
            if line.upper().startswith("CTN"):
                ctn = line
        results.append({
            "referencia":      ref,
            "estat":           estat,
            "data_publicacio": m_data.group(1) if m_data else "",
            "descripcio":      descripcio,
            "ctn":             ctn,
            "font":            "UNE scraping",
        })
    if results:
        return results

    # --- Strategy 2: divResultados text split (original) ---
    div_res = soup.find("div", id=re.compile("divResultados", re.I))
    if div_res:
        text   = div_res.get_text("\n")
        blocks = UNE_REF_PAT.split(text)
        i = 1
        while i < len(blocks) - 1:
            ref     = blocks[i].strip()
            content = blocks[i + 1] if i + 1 < len(blocks) else ""
            if ref and ref not in seen:
                seen.add(ref)
                m_estat = re.search(r"Estado:\s*(Vigente|Anulada)", content)
                m_data  = re.search(r"(\d{4}-\d{2}-\d{2})", content)
                estat = ""
                if m_estat:
                    estat = "VIGENT" if m_estat.group(1) == "Vigente" else "ANULADA"
                lines      = [l.strip() for l in content.split("\n") if l.strip()]
                descripcio = ""
                ctn        = ""
                for j, line in enumerate(lines):
                    if "Estado:" in line:
                        if j + 1 < len(lines) and not lines[j + 1].startswith("CTN"):
                            descripcio = lines[j + 1]
                    if line.startswith("CTN"):
                        ctn = line
                        break
                results.append({
                    "referencia":      ref,
                    "estat":           estat,
                    "data_publicacio": m_data.group(1) if m_data else "",
                    "descripcio":      descripcio[:200],
                    "ctn":             ctn,
                    "font":            "UNE scraping (div fallback)",
                })
            i += 2
    if results:
        return results

    # --- Strategy 3: full-page regex (last resort) ---
    refs = UNE_REF_PAT.findall(html)
    for ref in refs:
        ref = ref.strip()
        if ref and ref not in seen:
            seen.add(ref)
            results.append({
                "referencia":      ref,
                "estat":           "",
                "data_publicacio": "",
                "descripcio":      "",
                "ctn":             "",
                "font":            "UNE scraping (regex fallback)",
            })
    return results


# ─── Core search ──────────────────────────────────────────────────────────────

def _build_kql(ics_val: str, estat_filter: str) -> str:
    """Build KQL query: always UNE type + optionally estado + ICS."""
    parts = ["(g:UNE)"]
    if estat_filter == "V":
        parts.append("(e:VI)")
    elif estat_filter == "A":
        parts.append("(e:AN)")
    parts.append(f"(i:{ics_val}*)")
    return " AND ".join(parts)


def search_ics(
    session:      requests.Session,
    ics_val:      str,
    ics_name:     str,
    estat_filter: str,   # "V" | "A" | ""
) -> list[dict]:
    """
    Fetch all pages of results for one ICS code + estado.
    Uses KQL query posted to form1 / form2.
    Returns deduplicated list of norm dicts.
    """
    kql      = _build_kql(ics_val, estat_filter)
    p4_val   = {"V": "VI", "A": "AN"}.get(estat_filter, "")
    p7_val   = f"{ics_val}@@{ics_name}"

    all_results: list[dict] = []
    seen_refs:   set[str]   = set()
    total_items  = 0
    page         = 1

    while True:
        data = {
            "k":    kql,
            "n":    str(page),
            "m":    str(total_items),
            "v":    "",
            "p1":   "UNE@@",
            "p4":   p4_val,
            "p7":   p7_val,
            "ptit": "",
        }

        try:
            resp = session.post(SEARCH_URL, data=data, timeout=30)
            resp.raise_for_status()
        except Exception as exc:
            print(f"\n      Error p{page}: {exc}")
            break

        # On first page, read total item count
        if page == 1:
            soup = BeautifulSoup(resp.text, "html.parser")
            te = soup.find(id="totalElementos")
            if te:
                raw = te.get_text(strip=True).replace(".", "").replace(",", "")
                try:
                    total_items = int(raw)
                    print(f"\n      total web: {total_items:,}", end="")
                except ValueError:
                    pass

        results = parse_results_from_html(resp.text)
        if not results:
            break

        new = [r for r in results if r["referencia"] not in seen_refs]
        seen_refs.update(r["referencia"] for r in new)
        all_results.extend(new)
        print(f"  p{page}:{len(new)}", end="", flush=True)

        # Check for next page: presence of a link with id=pag{page+1}
        soup     = BeautifulSoup(resp.text, "html.parser")
        next_pag = f"pag{page + 1}"
        if not soup.find("a", id=next_pag):
            break

        page += 1
        time.sleep(DELAY)

    print()
    return all_results


# ─── Post-processing: successor enrichment ───────────────────────────────────

def _enrich_with_successor(entries: list[dict]) -> list[dict]:
    """
    Per cada entrada ANULADA, comprova si existeix al mateix catàleg
    una versió posterior vigent de la mateixa referència base.
    Si sí → afegeix camp 'successor': 'UNE-EN XXXX:YYYY'.
    """
    # Build index: base → list of entries
    by_base: dict[str, list[dict]] = {}
    for e in entries:
        base = e.get("referencia_base", "")
        if base:
            by_base.setdefault(base, []).append(e)

    _ANULADA_STATES = {"ANULADA", "RETIRADA", "CANCELADA"}
    _VIGENT_STATES  = {"VIGENT", "VIGENTE", "ACTIVA", "PUBLICADA"}

    for e in entries:
        estat = (e.get("estat") or "").upper()
        if estat not in _ANULADA_STATES:
            e.setdefault("successor", None)
            continue
        base = e.get("referencia_base", "")
        candidates = by_base.get(base, [])
        vigents = [
            c for c in candidates
            if (c.get("estat") or "").upper() in _VIGENT_STATES
            and c["referencia"] != e["referencia"]
        ]
        if vigents:
            # Pick the most recent by year
            vigents.sort(key=lambda x: _extract_year(x.get("referencia", "")), reverse=True)
            e["successor"] = vigents[0]["referencia"]
        else:
            e["successor"] = None

    return entries


def _search_successor_online(
    session: requests.Session,
    base_ref: str,
    ics_names: dict[str, str],
) -> dict | None:
    """
    Cerca la versió vigent d'una norma a partir de la referència base.
    Retorna l'entrada dict si la troba, None si no.
    """
    # Build a KQL query for this specific base ref, vigentes only
    # Escape the ref for KQL: use quotes
    kql = f'(g:UNE) AND (e:VI) AND ("{base_ref}")'
    data = {
        "k":    kql,
        "n":    "1",
        "m":    "0",
        "v":    "",
        "p1":   "UNE@@",
        "p4":   "VI",
        "p7":   "",
        "ptit": "",
    }
    try:
        resp = session.post(SEARCH_URL, data=data, timeout=30)
        resp.raise_for_status()
    except Exception as exc:
        print(f" error: {exc}")
        return None

    results = parse_results_from_html(resp.text)
    if not results:
        return None

    # Filter to matching base ref
    for r in results:
        r["referencia_base"] = _extract_base_ref(r["referencia"])
        if r["referencia_base"].upper() == base_ref.upper():
            estat = (r.get("estat") or "").upper()
            if estat in ("VIGENT", "VIGENTE"):
                return r
    return None


# ─── Diagnostic ───────────────────────────────────────────────────────────────

def run_diagnostic(session: requests.Session) -> bool:
    """Sanity check: ICS 93 vigentes. Returns True if > 0 results."""
    print("\n[DIAGNOSTIC] Testing ICS 93 (INGENIERIA CIVIL) vigentes...")
    results = search_ics(session, "93", "INGENIERIA CIVIL", "V")
    if results:
        print(f"  OK: {len(results)} normes trobades al diagnostic")
        print(f"  Exemple: {results[0]['referencia']} | {results[0]['estat']}")
        return True
    else:
        print("  WARNING: 0 resultats. Revisa curl_cffi i connexio a une.org")
        return False


# ─── Main ─────────────────────────────────────────────────────────────────────

def main(output_dir: str = OUTPUT_DIR) -> None:
    global CATALOG_PATH
    if output_dir != OUTPUT_DIR:
        CATALOG_PATH = os.path.join(output_dir, "_catalogo", "catalogo_une.json")

    print("=== UNE Catalog Builder (KQL) ===")
    print("Iniciant sessio...")

    session = make_session()

    if not run_diagnostic(session):
        print("Diagnostic failed — aborting. Comprova curl_cffi i connexio.")
        return

    print("Llegint noms ICS del formulari...")
    ics_names = get_ics_display_names(session)
    print(f"  {len(ics_names)} opcions ICS trobades")
    time.sleep(1)

    catalog:   list[dict] = []
    seen_refs: set[str]   = set()

    for ics_val, ics_name_fallback in ICS_TARGETS:
        # Use dropdown display text if found, otherwise use our hardcoded fallback
        ics_name = ics_names.get(ics_val, ics_name_fallback)
        print(f"\n  ICS {ics_val} ({ics_name_fallback}):")

        for estat_filter, estat_label in [("V", "vigentes"), ("A", "anuladas")]:
            print(f"    [{estat_label}]...", end="", flush=True)

            try:
                results = search_ics(session, ics_val, ics_name, estat_filter)
            except Exception as exc:
                print(f" error: {exc}")
                time.sleep(DELAY)
                continue

            new = [r for r in results if r["referencia"] not in seen_refs]
            seen_refs.update(r["referencia"] for r in new)
            catalog.extend(new)
            print(f" -> {len(new)} noves")
            time.sleep(DELAY)

    # ── Fix 1: add referencia_base to every entry ──
    for entry in catalog:
        entry["referencia_base"] = _extract_base_ref(entry["referencia"])

    # ── Fix 2: enrich ANULADA entries with successor from catalog ──
    print("\n[UNE] Enriquint anulades amb successors del cataleg...")
    catalog = _enrich_with_successor(catalog)

    n_amb_successor  = sum(1 for e in catalog if e.get("successor"))
    n_anulades_total = sum(
        1 for e in catalog
        if (e.get("estat") or "").upper() in ("ANULADA", "RETIRADA", "CANCELADA")
    )
    n_sense = n_anulades_total - n_amb_successor
    print(f"  Anulades amb successor al cataleg: {n_amb_successor}")
    print(f"  Anulades sense successor:          {n_sense}")

    # ── Fix 4: online lookup for ANULADA without successor (UNE-EN only) ──
    orphans = [
        e for e in catalog
        if (e.get("estat") or "").upper() in ("ANULADA", "RETIRADA", "CANCELADA")
        and not e.get("successor")
        and e.get("referencia_base", "").upper().startswith("UNE-EN")
    ]
    if orphans:
        n_lookups = min(len(orphans), MAX_ONLINE_LOOKUPS)
        print(f"\n[UNE] Cercant successors online per {n_lookups} normes UNE-EN anulades...")
        found_online = 0
        for i, entry in enumerate(orphans[:n_lookups]):
            base = entry["referencia_base"]
            print(f"  [{i+1}/{n_lookups}] Cercant successor de {base}...", end="", flush=True)
            try:
                result = _search_successor_online(session, base, ics_names)
            except Exception as exc:
                print(f" error: {exc}")
                time.sleep(DELAY)
                continue
            if result:
                entry["successor"] = result["referencia"]
                # Also add the vigent entry to catalog if not already present
                if result["referencia"] not in seen_refs:
                    result["referencia_base"] = _extract_base_ref(result["referencia"])
                    result.setdefault("successor", None)
                    catalog.append(result)
                    seen_refs.add(result["referencia"])
                found_online += 1
                print(f" -> {result['referencia']}")
            else:
                print(" no trobat")
            time.sleep(DELAY)
        print(f"  Successors trobats online: {found_online}/{n_lookups}")

    # ── Save ──
    os.makedirs(os.path.dirname(CATALOG_PATH), exist_ok=True)
    with open(CATALOG_PATH, "w", encoding="utf-8") as f:
        json.dump(catalog, f, ensure_ascii=False, indent=2)

    # ── Fix 5: final stats ──
    total    = len(catalog)
    vigents  = sum(1 for d in catalog if (d.get("estat") or "").upper() in ("VIGENT", "VIGENTE"))
    anulades = sum(1 for d in catalog if (d.get("estat") or "").upper() in ("ANULADA", "RETIRADA", "CANCELADA"))
    amb_succ = sum(1 for d in catalog if d.get("successor"))
    sense    = anulades - amb_succ

    print(f"\n[UNE] Total entrades: {total}")
    print(f"[UNE] Vigents: {vigents}")
    print(f"[UNE] Anulades: {anulades}")
    print(f"[UNE]   - Amb successor trobat: {amb_succ}")
    print(f"[UNE]   - Sense successor: {sense}")
    print(f"[UNE] Cataleg guardat: {CATALOG_PATH}")


if __name__ == "__main__":
    out = sys.argv[1] if len(sys.argv) > 1 else OUTPUT_DIR
    main(out)
