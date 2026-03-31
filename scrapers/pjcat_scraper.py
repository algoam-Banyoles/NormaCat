"""
pjcat_scraper.py — Obté normativa del Portal Jurídic de Catalunya (PJCAT)
usant l'API oficial ELI (European Legislation Identifier).

ELI base: https://portaljuridic.gencat.cat/eli/
Estratègia:
  1. Obtenir tots els PRIORITY_DOCS directament per documentId URL
  2. Obtenir pàgines de llistat ELI per rangs rellevants rang+any
  3. Parsejar HTML per extreure metadades dels documents
  4. Fusionar derogades a data/normativa_annexes.json

Sortida: catalogs/pjcat/catalogo_pjcat.json
"""

from __future__ import annotations

import io
import json
import os
import re
import sys
import time
from pathlib import Path

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
else:
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from config import PROJECT_ROOT, CATALOGS_DIR

# ─── Constants ────────────────────────────────────────────────────────────────

BASE_URL    = "https://portaljuridic.gencat.cat"
ELI_BASE    = f"{BASE_URL}/eli"
DELAY       = 2.0
MAX_RETRIES = 3

PJCAT_CATALOG_DIR = CATALOGS_DIR / "pjcat"
ANNEXES_PATH = PROJECT_ROOT / "data" / "normativa_annexes.json"

HEADERS = {
    "User-Agent":      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept":          "text/html,application/xhtml+xml",
    "Accept-Language": "ca,es;q=0.9",
    "Referer":         "https://portaljuridic.gencat.cat/",
}

# ─── Documents prioritaris (obtinguts directament per documentId) ────────────
# URL: https://portaljuridic.gencat.cat/ca/document-del-pjur/?documentId={id}

PRIORITY_DOCS = [
    # --- CARRETERES ---
    {
        "documentId": "480379",
        "codi": "DL-2/2009",
        "text": "Decret Legislatiu 2/2009, de 25 d'agost, Text refos de la Llei de carreteres",
        "categoria": "carreteres_cat",
        "estat": "VIGENT",
        "eli": "/eli/es-ct/dl/2009/08/25/2",
        "observacions": "Deroga la Llei 7/1993",
    },
    {
        "documentId": "312160",
        "codi": "D-293/2003",
        "text": "Decret 293/2003, de 18 de novembre, Reglament general de carreteres",
        "categoria": "carreteres_cat",
        "estat": "VIGENT",
        "eli": "/eli/es-ct/d/2003/11/18/293",
        "observacions": "Desenvolupa la Llei 7/1993 i el DL 2/2009",
    },
    {
        "documentId": "92770",
        "codi": "L-7/1993",
        "text": "Llei 7/1993, de 30 de setembre, de carreteres",
        "categoria": "carreteres_cat",
        "estat": "DEROGADA",
        "eli": "/eli/es-ct/l/1993/09/30/7",
        "derogada_per": "DL-2/2009",
        "observacions": "Derogada pel Decret Legislatiu 2/2009",
    },
    # --- SEGURETAT VIARIA ---
    {
        "documentId": "921467",
        "codi": "D-168/2025",
        "text": "Decret 168/2025, de 29 de juliol, de gestio de la seguretat viaria en les infraestructures viaries de la Generalitat de Catalunya",
        "categoria": "seguretat_viaria",
        "estat": "VIGENT",
        "eli": "/eli/es-ct/d/2025/07/29/168",
        "observacions": "Transposa Directiva (UE) 2019/1936. Deroga D-190/2016",
    },
    {
        "documentId": "673800",
        "codi": "D-190/2016",
        "text": "Decret 190/2016, de 16 de febrer, de gestio de la seguretat viaria en les infraestructures viaries de la Generalitat de Catalunya",
        "categoria": "seguretat_viaria",
        "estat": "DEROGADA",
        "eli": "/eli/es-ct/d/2016/02/16/190",
        "derogada_per": "D-168/2025",
        "observacions": "Derogat pel Decret 168/2025 des del 31.07.2025",
    },
    # --- CONTRACTACIO ---
    {
        "documentId": "824435",
        "codi": "L-9/2017",
        "text": "Llei 9/2017, de 8 de novembre, de Contractes del Sector Public",
        "categoria": "contractes",
        "estat": "VIGENT",
        "eli": "/eli/es/l/2017/11/08/9",
        "observacions": "LCSP vigent. Transposa Directives 2014/23/UE i 2014/24/UE",
    },
    {
        "documentId": "641266",
        "codi": "RD-1098/2001",
        "text": "Reial Decret 1098/2001, de 12 d'octubre, Reglament general de contractes de les administracions publiques",
        "categoria": "contractes",
        "estat": "VIGENT",
        "eli": "/eli/es/rd/2001/10/12/1098",
        "observacions": "Parcialment vigent. Alguns articles derogats per la LCSP",
    },
    # --- MEDI AMBIENT / RESIDUS ---
    {
        "documentId": "630565",
        "codi": "L-20/2009",
        "text": "Llei 20/2009, de 4 de desembre, de prevencio i control ambiental de les activitats",
        "categoria": "medi_ambient",
        "estat": "VIGENT",
        "eli": "/eli/es-ct/l/2009/12/04/20",
        "observacions": "",
    },
    # --- URBANISME ---
    {
        "documentId": "597312",
        "codi": "DL-1/2010",
        "text": "Decret Legislatiu 1/2010, de 3 d'agost, Text refos de la Llei d'urbanisme",
        "categoria": "urbanisme",
        "estat": "VIGENT",
        "eli": "/eli/es-ct/dl/2010/08/03/1",
        "observacions": "",
    },
    # --- ACCESSIBILITAT ---
    {
        "documentId": "672453",
        "codi": "D-209/2023",
        "text": "Decret 209/2023, de 28 de novembre, pel qual s'aprova el Codi d'accessibilitat de Catalunya",
        "categoria": "accessibilitat",
        "estat": "VIGENT",
        "eli": "/eli/es-ct/d/2023/11/28/209",
        "observacions": "Deroga el D-135/1995",
    },
    {
        "documentId": "167320",
        "codi": "D-135/1995",
        "text": "Decret 135/1995, de 24 de marc, de desplegament de la Llei 20/1991, de promocio de l'accessibilitat i de supressio de barreres arquitectoniques",
        "categoria": "accessibilitat",
        "estat": "DEROGADA",
        "eli": "/eli/es-ct/d/1995/03/24/135",
        "derogada_per": "D-209/2023",
        "observacions": "Derogat per D-209/2023 des de l'01.03.2024",
    },
    # --- SEGURETAT I SALUT ---
    {
        "documentId": "295505",
        "codi": "RD-1627/1997",
        "text": "Reial Decret 1627/1997, de 24 d'octubre, disposicions minimes de seguretat i salut en obres de construccio",
        "categoria": "seguretat_salut",
        "estat": "VIGENT",
        "eli": "/eli/es/rd/1997/10/24/1627",
        "observacions": "",
    },
]

# ─── Rangs ELI per descobrir documents addicionals ───────────────────────────
# Cada entrada: (rang_path, categoria, years_range)
# rang_path: part de la URL ELI després de /eli/es-ct/

ELI_RANGES = [
    # Decrets legislatius catalans (tots)
    ("dl", "carreteres_cat", range(1990, 2026)),
    # Decrets catalans recents rellevants
    ("d", "carreteres_cat", range(2000, 2026)),
    # Lleis catalanes
    ("l", "contractes", range(1985, 2026)),
]

# Paraules clau per classificar normes trobades per llistat ELI
KEYWORD_MAP = {
    "carreteres_cat":   ["carretera", "viari", "autopista", "transit", "infraestructura"],
    "contractes":       ["contracte", "licitacio", "concurs", "adjudicacio", "obra publica"],
    "seguretat_viaria": ["seguretat viar", "accident", "sinistral"],
    "medi_ambient":     ["medi ambient", "impacte ambiental", "residus", "RCD"],
    "urbanisme":        ["urbanisme", "sol", "edificacio", "planejament"],
    "accessibilitat":   ["accessibilitat", "barreres", "discapacitat"],
    "seguretat_salut":  ["seguretat i salut", "riscos laborals", "obra"],
}


# ─── Sessió ──────────────────────────────────────────────────────────────────

def make_session() -> requests.Session:
    session = requests.Session()
    retry = Retry(
        total=MAX_RETRIES,
        backoff_factor=2,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://",  adapter)
    return session


# ─── Helpers ──────────────────────────────────────────────────────────────────

def _get_html(session: requests.Session, url: str) -> BeautifulSoup | None:
    try:
        r = session.get(url, headers=HEADERS, timeout=30)
        if r.status_code in (404, 410):
            return None
        r.raise_for_status()
        return BeautifulSoup(r.text, "html.parser")
    except Exception as exc:
        print(f"  [WARNING] {url} -> {exc}")
        return None


def _save(catalog: list[dict], catalog_dir: Path) -> None:
    catalog_dir.mkdir(parents=True, exist_ok=True)
    catalog_path = catalog_dir / "catalogo_pjcat.json"
    with open(catalog_path, "w", encoding="utf-8") as f:
        json.dump(catalog, f, ensure_ascii=False, indent=2)


def _guess_categoria(text: str) -> str:
    text_lower = text.lower()
    for cat, keywords in KEYWORD_MAP.items():
        if any(kw in text_lower for kw in keywords):
            return cat
    return "altres"


def _detect_estat_from_html(soup: BeautifulSoup) -> str:
    """Intenta detectar l'estat de vigència des de l'HTML del document."""
    text = soup.get_text(" ", strip=True).lower()
    if "derogat" in text or "derogada" in text or "deixa de tenir vigencia" in text:
        return "DEROGADA"
    if "vigent" in text or "en vigor" in text:
        return "VIGENT"
    return "VIGENT"  # per defecte: assumim vigent si no es pot determinar


def _extract_dogc(soup: BeautifulSoup) -> str:
    """Extreu el número de DOGC de la pàgina."""
    text = soup.get_text(" ", strip=True)
    m = re.search(r"DOGC\s+n[uu]m\.?\s*(\d+)", text, re.IGNORECASE)
    return m.group(1) if m else ""


def _extract_date(soup: BeautifulSoup) -> str:
    """Extreu la data de publicació."""
    text = soup.get_text(" ", strip=True)
    m = re.search(r"(\d{1,2})\s+de\s+(\w+)\s+de\s+(\d{4})", text)
    if m:
        return f"{m.group(3)}-{m.group(1).zfill(2)}"
    return ""


# ─── Obtenir document individual per documentId ──────────────────────────────

def fetch_by_document_id(
    session: requests.Session,
    doc: dict,
) -> dict:
    """
    Obté una pàgina de document de PJCAT i enriqueix el dict de metadades.
    Retorna el dict enriquit.
    """
    url = f"{BASE_URL}/ca/document-del-pjur/?documentId={doc['documentId']}"
    entry = {
        "id":           f"pjcat-{doc['documentId']}",
        "documentId":   doc["documentId"],
        "codi":         doc.get("codi", ""),
        "text":         doc.get("text", ""),
        "categoria":    doc.get("categoria", "altres"),
        "estat":        doc.get("estat", "VIGENT"),
        "eli":          doc.get("eli", ""),
        "data":         "",
        "dogc_num":     "",
        "url_pjcat":    url,
        "derogada_per": doc.get("derogada_per", ""),
        "observacions": doc.get("observacions", ""),
        "font":         "Portal Juridic Catalunya",
        "fetch_ok":     False,
    }

    soup = _get_html(session, url)
    if soup:
        entry["fetch_ok"] = True
        # Intentar obtenir el títol real de la pàgina
        h1 = soup.find("h1")
        if h1 and h1.get_text(strip=True):
            real_title = h1.get_text(strip=True)
            if len(real_title) > 10:
                entry["text"] = real_title[:300]
        entry["dogc_num"] = _extract_dogc(soup)
        entry["data"]     = _extract_date(soup)
        # Només sobreescriure estat si no està fixat com a DEROGADA
        if entry["estat"] != "DEROGADA":
            entry["estat"] = _detect_estat_from_html(soup)

    return entry


# ─── Obtenció de llistat ELI ────────────────────────────────────────────────

def fetch_eli_listing(
    session:   requests.Session,
    rang:      str,
    year:      int,
    categoria: str,
    seen_ids:  set[str],
    catalog:   list[dict],
) -> int:
    """
    Obté el llistat ELI per un rang i any donats.
    URL: /eli/es-ct/{rang}/{year}/
    Retorna el nombre de noves entrades afegides.
    """
    url = f"{ELI_BASE}/es-ct/{rang}/{year}/"
    soup = _get_html(session, url)
    if not soup:
        return 0

    added = 0
    # Cercar links a pàgines de normes individuals
    for a in soup.find_all("a", href=True):
        href = a["href"]
        # Coincidir URLs ELI de normes: /eli/es-ct/{rang}/{year}/{month}/{day}/{num}
        m = re.match(
            r"/eli/es-ct/[a-z]+/\d{4}/\d{2}/\d{2}/[\w-]+",
            href
        )
        if not m:
            continue

        norm_id = href.strip("/").replace("/", "-")
        if norm_id in seen_ids:
            continue

        title = a.get_text(strip=True)
        if not title or len(title) < 5:
            continue

        cat = _guess_categoria(title)
        if cat != categoria and categoria not in ("carreteres_cat", "contractes"):
            continue

        seen_ids.add(norm_id)
        entry = {
            "id":           f"pjcat-eli-{norm_id}",
            "documentId":   "",
            "codi":         norm_id,
            "text":         title[:300],
            "categoria":    cat,
            "estat":        "VIGENT",
            "eli":          href,
            "data":         str(year),
            "dogc_num":     "",
            "url_pjcat":    f"{BASE_URL}{href}",
            "derogada_per": "",
            "observacions": "Descobert via llistat ELI",
            "font":         "Portal Juridic Catalunya (ELI)",
            "fetch_ok":     True,
        }
        catalog.append(entry)
        added += 1

    return added


# ─── Fusionar amb normativa_annexes.json ────────────────────────────────────

def merge_into_annexes(catalog: list[dict], annexes_path: Path) -> None:
    if not annexes_path.exists():
        print(f"  [INFO] {annexes_path} no trobat, omitint merge")
        return

    import shutil
    shutil.copy2(annexes_path, str(annexes_path) + ".bak")

    with open(annexes_path, encoding="utf-8") as f:
        annexes = json.load(f)

    existing = annexes.get("normativa_derogada", [])
    existing_codis = {e.get("codi", "") for e in existing}
    existing_texts = {e.get("text", "")[:50] for e in existing}

    added = 0
    for entry in catalog:
        if entry["estat"] != "DEROGADA":
            continue
        codi = entry.get("codi", "")
        text_key = entry.get("text", "")[:50]
        if codi in existing_codis or text_key in existing_texts:
            continue
        existing.append({
            "codi":        codi,
            "text":        entry.get("text", "")[:200],
            "derogada_per": entry.get("derogada_per", ""),
            "observacions": f"Font: PJCAT. {entry.get('observacions', '')}".strip(),
        })
        existing_codis.add(codi)
        existing_texts.add(text_key)
        added += 1
        print(f"    + DEROGADA: {codi} — {entry.get('text', '')[:55]}")

    annexes["normativa_derogada"] = existing
    with open(annexes_path, "w", encoding="utf-8") as f:
        json.dump(annexes, f, ensure_ascii=False, indent=2)

    print(f"  normativa_annexes.json actualitzat: +{added} entrades derogades")


# ─── Main ─────────────────────────────────────────────────────────────────────

def main(catalog_dir: Path = None) -> None:
    if catalog_dir is None:
        catalog_dir = PJCAT_CATALOG_DIR

    catalog_path = catalog_dir / "catalogo_pjcat.json"

    print("=== Portal Juridic Catalunya — Constructor de catàleg (ELI) ===")
    print(f"  Sortida: {catalog_path}\n")

    session  = make_session()
    catalog: list[dict] = []
    seen_ids: set[str]  = set()

    # 1) Documents prioritaris
    print(f"[1/3] Prioritat: {len(PRIORITY_DOCS)} documents directes")
    for doc in PRIORITY_DOCS:
        print(f"  Obtenint {doc['codi']} (id={doc['documentId']})...", end=" ", flush=True)
        entry = fetch_by_document_id(session, doc)
        doc_id = entry["id"]
        if doc_id not in seen_ids:
            seen_ids.add(doc_id)
            catalog.append(entry)
            status = "OK" if entry["fetch_ok"] else "WARN metadades hardcoded"
            print(f"{status} -> {entry['estat']}: {entry['text'][:60]}")
        else:
            print("SKIP (duplicat)")
        _save(catalog, catalog_dir)
        time.sleep(DELAY)

    # 2) Descoberta per llistat ELI (anys recents, prioritat carreteres i contractes)
    print(f"\n[2/3] Llistat ELI — descoberta automàtica")
    ELI_DISCOVERY = [
        ("dl", "carreteres_cat", range(2000, 2026)),  # Decrets legislatius
        ("d",  "carreteres_cat", range(2015, 2026)),  # Decrets recents
    ]
    for rang, categoria, years in ELI_DISCOVERY:
        count_rang = 0
        for year in years:
            n = fetch_eli_listing(session, rang, year, categoria, seen_ids, catalog)
            if n > 0:
                print(f"    /eli/es-ct/{rang}/{year}/ -> {n} noves")
                count_rang += n
                _save(catalog, catalog_dir)
            time.sleep(DELAY)
        print(f"  Rang '{rang}' ({categoria}): {count_rang} descobertes")

    # 3) Resum
    total   = len(catalog)
    vigents = sum(1 for e in catalog if e["estat"] == "VIGENT")
    derog   = sum(1 for e in catalog if e["estat"] == "DEROGADA")
    altre   = total - vigents - derog

    print(f"\n{'='*55}")
    print(f"  Total entrades PJCAT:  {total}")
    print(f"  Vigents:               {vigents}")
    print(f"  Derogades:             {derog}")
    print(f"  Pendent/altre:         {altre}")
    print(f"  Catàleg guardat:       {catalog_path}")
    print(f"{'='*55}")

    print("\n[Extra] Sincronitzant derogades amb normativa_annexes.json...")
    merge_into_annexes(catalog, ANNEXES_PATH)


if __name__ == "__main__":
    main()
