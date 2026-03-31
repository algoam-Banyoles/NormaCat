"""
norm_scraper.py
Descàrrega i sincronització incremental de normativa tècnica de carreteres.
https://www.transportes.gob.es/carreteras/normativa-tecnica

Estructura real (3 nivells):
  Nivell 1 — Pàgina de categoria  → links a 3 sub-pàgines
  Nivell 2 — Llista de documents  → links a PDFs al CDN
  Nivell 3 — CDN (obert, sense auth)

Sincronització incremental:
  - El catàleg JSON és la font de veritat entre execucions.
  - Clau única: url_original.
  - Detecta documents nous, canvis d'estat (tipus) i documents retirats.
  - Mai esborra fitxers del disc; els marca com RETIRAT al catàleg.
"""

import csv
import json
import os
import re
import shutil
import sys
import time
import unicodedata
from datetime import datetime
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup

# ─── Constants ────────────────────────────────────────────────────────────────
BASE_URL   = "https://www.transportes.gob.es/carreteras/normativa-tecnica"
CDN_BASE   = "https://cdn.transportes.gob.es"
OUTPUT_DIR = "normativa_dgc"
DELAY      = 1.0   # seconds between sub-page requests

CATEGORIES = [
    ("01_Cuestiones_generales",      "normativa-general-carreteras"),
    ("02_Impacto_ambiental",         "impacto-ambiental"),
    ("03_Seguridad_salud",           "seguridad-y-salud"),
    ("04_Seguridad_vial",            "04-seguridad-vial"),
    ("05_Proyectos",                 "05-proyecto"),
    ("06_Trazado",                   "06-trazado"),
    ("07_Drenaje",                   "drenaje"),
    ("08_Geologia_geotecnia",        "geologia-y-geotecnia"),
    ("09_Puentes_estructuras",       "puentes-estructuras"),
    ("10_Tuneles",                   "tuneles"),
    ("11_Firmes_pavimentos",         "firmes-pavimentos"),
    ("12_Equipamiento_vial",         "equipamiento-vial"),
    ("13_Iluminacion",               "iluminacion"),
    ("14_Ruido",                     "ruido"),
    ("15_Estaciones_servicio",       "estaciones-areas-servicio"),
    ("16_Pliegos",                   "pliegos-prescripciones-tecnicas-generales"),
    ("17_Calidad",                   "calidad"),
    ("18_Materiales_construccion",   "materiales-construccion"),
    ("19_Inventario_carreteras",     "inventario-carreteras"),
    ("20_Eurocodigos",               "eurocodigos"),
    ("21_Infraestructura_ciclista",  "infraestructura-ciclista"),
    ("22_OC_historicas",             "ordenes-circulares-historicas"),
]

SUB_PAGES = [
    ("normativa",  "normativa-tecnica"),
    ("referencia", "bibliografia-referencia"),
    ("historica",  "bibliografia-historica"),
]

# ─── Session ──────────────────────────────────────────────────────────────────
SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent":      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept":          "text/html,application/xhtml+xml,*/*;q=0.8",
    "Accept-Language": "es-ES,es;q=0.9,ca;q=0.8",
})


# ─── Function 1: get_subpage_links ───────────────────────────────────────────
def get_subpage_links(category_slug: str) -> dict:
    """
    Fetch the category page and return URLs for its 3 sub-pages.
    Selector: .aside_listado--titulo a
    Returns: {"normativa": url|None, "referencia": url|None, "historica": url|None}
    """
    category_url = f"{BASE_URL}/{category_slug}"
    result = {tipo: None for tipo, _ in SUB_PAGES}

    try:
        resp = SESSION.get(category_url, timeout=30, allow_redirects=True)
        if resp.status_code != 200:
            print(f"  ✗ HTTP {resp.status_code}: {category_url}")
        else:
            soup = BeautifulSoup(resp.content, "html.parser",
                                 from_encoding=resp.encoding or "utf-8")
            for a in soup.select(".aside_listado--titulo a"):
                href = a.get("href", "")
                text = a.get_text(" ", strip=True).lower()
                abs_url = href if href.startswith("http") else urljoin(
                    "https://www.transportes.gob.es", href
                )
                if "bibliograf" in text and ("hist" in text or "histor" in text):
                    result["historica"] = abs_url
                elif "bibliograf" in text or "referencia" in text:
                    result["referencia"] = abs_url
                elif "normativa" in text:
                    result["normativa"] = abs_url

    except Exception as exc:
        print(f"  ✗ Error carregant {category_url}: {exc}")

    # Fallback: construct expected URLs for any sub-page not found via selector
    for tipo, slug in SUB_PAGES:
        if result[tipo] is None:
            result[tipo] = f"{BASE_URL}/{category_slug}/{slug}"

    return result


# ─── Function 2: get_documents_from_subpage ──────────────────────────────────
def get_documents_from_subpage(url: str) -> list:
    """
    Fetch a document-list page and extract all PDF links.
    Returns list of {"title": str, "url": str, "subsection": str}.
    """
    docs = []
    try:
        resp = SESSION.get(url, timeout=30, allow_redirects=True)
        if resp.status_code == 404:
            return []
        if resp.status_code != 200:
            print(f"    ✗ HTTP {resp.status_code}: {url}")
            return []

        soup = BeautifulSoup(resp.content, "html.parser",
                             from_encoding=resp.encoding or "utf-8")

        current_subsection = ""
        seen_urls: set = set()

        for tag in soup.find_all(
            lambda t: t.name in ("h2", "h3", "h4", "strong", "a")
        ):
            if tag.name in ("h2", "h3", "h4", "strong"):
                text = tag.get_text(" ", strip=True)
                if re.match(r"^\d+\.\d+", text):
                    current_subsection = text

            if tag.name == "a":
                href = tag.get("href", "")
                if not href:
                    continue
                if ".pdf" not in href.lower() and "cdn.transportes" not in href:
                    continue

                abs_url = href if href.startswith("http") else urljoin(
                    "https://www.transportes.gob.es", href
                )
                if abs_url in seen_urls:
                    continue
                seen_urls.add(abs_url)

                title = tag.get_text(" ", strip=True)
                if len(title) < 5:
                    parent_text = tag.parent.get_text(" ", strip=True) if tag.parent else ""
                    title = parent_text[:120] if len(parent_text) >= 5 else href.split("/")[-1]

                docs.append({
                    "title":      title.strip(),
                    "url":        abs_url,
                    "subsection": current_subsection,
                })

    except Exception as exc:
        print(f"    ✗ Error carregant {url}: {exc}")

    return docs


# ─── Function 3: download_pdf ─────────────────────────────────────────────────
def download_pdf(url: str, dest_path: str) -> bool:
    """
    Download a PDF from the CDN.
    Returns True if downloaded now, False if already existed or failed.
    """
    if os.path.exists(dest_path):
        return False

    os.makedirs(os.path.dirname(dest_path), exist_ok=True)

    try:
        resp = SESSION.get(url, timeout=60, stream=True, allow_redirects=True)
        if resp.status_code != 200:
            print(f"    ✗ HTTP {resp.status_code}: {os.path.basename(dest_path)}")
            return False

        content = resp.content

        if not content.startswith(b"%PDF"):
            print(f"    ✗ No és PDF vàlid: {os.path.basename(dest_path)}")
            return False

        with open(dest_path, "wb") as f:
            f.write(content)

        size_kb = len(content) / 1024
        print(f"    ↓ {os.path.basename(dest_path)} ({size_kb:.0f} KB)")
        return True

    except Exception as exc:
        print(f"    ✗ Error descarregant {os.path.basename(dest_path)}: {exc}")
        return False


# ─── Function 4: sanitize_filename ───────────────────────────────────────────
def sanitize_filename(title: str, url: str) -> str:
    """Generate a safe ASCII filename from title, falling back to URL basename."""
    if title and len(title) >= 5:
        normalized = unicodedata.normalize("NFKD", title)
        ascii_text = normalized.encode("ascii", "ignore").decode("ascii")
        safe = re.sub(r"[^a-zA-Z0-9\-]+", "_", ascii_text)
        safe = re.sub(r"_+", "_", safe).strip("_")
        if len(safe) > 96:
            safe = safe[:96].rstrip("_")
    else:
        safe = urlparse(url).path.split("/")[-1]
        safe = re.sub(r"[^a-zA-Z0-9_.\-]", "_", safe)

    if not safe.lower().endswith(".pdf"):
        safe += ".pdf"
    return safe


# ─── Catalog persistence ──────────────────────────────────────────────────────
def load_existing_catalog(output_dir: str) -> dict:
    """
    Load existing catalog and return as dict keyed by url_original.
    Returns {} if catalog does not exist yet.
    """
    catalog_path = os.path.join(output_dir, "_catalogo", "catalogo_completo.json")
    if not os.path.exists(catalog_path):
        return {}
    try:
        with open(catalog_path, encoding="utf-8") as f:
            items = json.load(f)
        return {item["url_original"]: item for item in items if "url_original" in item}
    except Exception as exc:
        print(f"  [!] No s'ha pogut carregar el catàleg existent: {exc}")
        return {}


def _save_catalog(catalog: list, output_dir: str) -> None:
    """Persist catalog as JSON + CSV."""
    catalog_dir = os.path.join(output_dir, "_catalogo")
    os.makedirs(catalog_dir, exist_ok=True)

    json_path = os.path.join(catalog_dir, "catalogo_completo.json")
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(catalog, f, ensure_ascii=False, indent=2)

    if catalog:
        # Union of all keys found in any entry
        fieldnames = list(dict.fromkeys(k for entry in catalog for k in entry))
        csv_path = os.path.join(catalog_dir, "catalogo_completo.csv")
        with open(csv_path, "w", encoding="utf-8-sig", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
            writer.writeheader()
            writer.writerows(catalog)


def _save_sync_log(changes: dict, output_dir: str) -> str:
    """Save a dated sync log JSON and return its path."""
    catalog_dir = os.path.join(output_dir, "_catalogo")
    os.makedirs(catalog_dir, exist_ok=True)

    today = datetime.now().strftime("%Y%m%d")
    log_path = os.path.join(catalog_dir, f"sync_{today}.json")

    log = {
        "data":            datetime.now().isoformat()[:10],
        "nous":            len(changes["nous"]),
        "actualitzats":    len(changes["actualitzats"]),
        "eliminats":       len(changes["eliminats"]),
        "sense_canvis":    len(changes["sense_canvis"]),
        "detall_canvis": {
            "nous": [d["titol"] for d in changes["nous"]],
            "actualitzats": [
                {
                    "titol": d["titol"],
                    "antic": d.get("estat_legal_anterior", ""),
                    "nou":   d["estat_legal"],
                }
                for d in changes["actualitzats"]
            ],
            "eliminats": [d["titol"] for d in changes["eliminats"]],
        },
    }

    with open(log_path, "w", encoding="utf-8") as f:
        json.dump(log, f, ensure_ascii=False, indent=2)

    return log_path


# ─── Function: scrape_category ────────────────────────────────────────────────
def scrape_category(category_slug: str, folder_name: str) -> list:
    """
    Scrape one category and return metadata for all documents found.
    Does NOT download — only returns catalog metadata entries.
    Each entry has: url_original, titol, categoria, tipus, estat_legal, subseccio.
    """
    subpage_links = get_subpage_links(category_slug)
    time.sleep(DELAY)

    docs_out = []
    for tipus, sub_url in subpage_links.items():
        if not sub_url:
            continue

        raw_docs = get_documents_from_subpage(sub_url)
        for doc in raw_docs:
            docs_out.append({
                "categoria":    folder_name,
                "tipus":        tipus,
                "estat_legal":  tipus,   # for DGC, the sub-page type IS the legal status
                "subseccio":    doc.get("subsection", ""),
                "titol":        doc["title"],
                "url_original": doc["url"],
            })

        time.sleep(DELAY)

    return docs_out


# ─── Function 5: scrape_all ───────────────────────────────────────────────────
def scrape_all(output_dir: str) -> list:
    """
    Incremental sync orchestration.
    Compares current website state with stored catalog.
    Downloads only new or status-changed documents.
    Never deletes files; marks removed documents as RETIRAT.
    Returns the final merged catalog list.
    """
    today = datetime.now().isoformat()[:10]

    # ── Load existing catalog ─────────────────────────────────────────────────
    print("Carregant catàleg existent...")
    existing = load_existing_catalog(output_dir)
    print(f"  {len(existing)} documents al catàleg existent")

    # ── Scrape all categories (metadata only) ─────────────────────────────────
    print("\nEscaneig del lloc web...")
    current_web: dict[str, dict] = {}

    for i, (folder_name, category_slug) in enumerate(CATEGORIES, 1):
        print(f"  [{i:02d}/{len(CATEGORIES)}] {folder_name}", end=" … ")
        docs = scrape_category(category_slug, folder_name)
        for doc in docs:
            current_web[doc["url_original"]] = doc
        print(f"{len(docs)} docs")

    print(f"  {len(current_web)} documents trobats al lloc web")

    # ── Sync ──────────────────────────────────────────────────────────────────
    changes: dict[str, list] = {
        "nous": [], "actualitzats": [], "eliminats": [], "sense_canvis": []
    }
    final_catalog: list[dict] = []

    # Process documents found on the website
    for url, web_doc in current_web.items():
        if url in existing:
            old = existing[url]
            # Normalise: old entries without estat_legal inherit from tipus
            old_estat = old.get("estat_legal") or old.get("tipus", "")
            new_estat = web_doc["estat_legal"]

            if old_estat != new_estat:
                print(f"  ⚠ CANVI ESTAT: {web_doc['titol'][:60]}")
                print(f"    {old_estat} → {new_estat}")

                updated = {**old, **web_doc}
                updated["estat_legal_anterior"] = old_estat
                updated["data_canvi"]           = today

                # Move file to new folder if the type changed
                old_path = old.get("fitxer_local", "")
                if old_path and os.path.exists(old_path):
                    new_folder = os.path.join(output_dir, web_doc["categoria"], web_doc["tipus"])
                    os.makedirs(new_folder, exist_ok=True)
                    new_path = os.path.join(new_folder, os.path.basename(old_path))
                    if old_path != new_path:
                        try:
                            shutil.move(old_path, new_path)
                            updated["fitxer_local"] = new_path
                            print(f"    Fitxer mogut a: {web_doc['tipus']}/")
                        except Exception as exc:
                            print(f"    [!] No s'ha pogut moure el fitxer: {exc}")

                final_catalog.append(updated)
                changes["actualitzats"].append(updated)
            else:
                final_catalog.append(old)
                changes["sense_canvis"].append(old)
        else:
            web_doc["data_afegit"] = today
            final_catalog.append(web_doc)
            changes["nous"].append(web_doc)

    # Documents present in catalog but no longer on the website
    for url, old_doc in existing.items():
        if url not in current_web:
            removed = {**old_doc, "estat_legal": "RETIRAT", "data_retirada": today}
            final_catalog.append(removed)
            changes["eliminats"].append(removed)
            print(f"  ✗ RETIRAT: {old_doc['titol'][:60]}")

    # ── Download new and status-changed documents ─────────────────────────────
    to_download = changes["nous"] + changes["actualitzats"]
    print(f"\nDescàrrega de {len(to_download)} documents nous/actualitzats...")

    downloaded_count = 0
    for doc in to_download:
        existing_path = doc.get("fitxer_local", "")
        if existing_path and os.path.exists(existing_path):
            continue  # already moved or previously downloaded

        dest_folder = os.path.join(output_dir, doc["categoria"], doc["tipus"])
        filename    = sanitize_filename(doc["titol"], doc["url_original"])
        dest_path   = os.path.join(dest_folder, filename)
        doc["fitxer_local"] = dest_path

        ok = download_pdf(doc["url_original"], dest_path)
        doc["descarregat"] = ok
        if ok:
            downloaded_count += 1
        time.sleep(0.3)

    # ── Persist catalog and log ───────────────────────────────────────────────
    _save_catalog(final_catalog, output_dir)
    log_path = _save_sync_log(changes, output_dir)

    # ── Summary ───────────────────────────────────────────────────────────────
    print(f"\n{'─' * 55}")
    print(f"RESUM SINCRONITZACIÓ {today}:")
    print(f"  Documents nous:          {len(changes['nous']):4}")
    print(f"  Estat actualitzat:       {len(changes['actualitzats']):4}")
    for d in changes["actualitzats"]:
        print(f"    • {d['titol'][:50]} → {d['estat_legal']}")
    print(f"  Retirats del lloc web:   {len(changes['eliminats']):4}")
    print(f"  Sense canvis:            {len(changes['sense_canvis']):4}")
    print(f"  PDFs descarregats ara:   {downloaded_count:4}")
    print(f"  Log guardat: {log_path}")
    print(f"{'─' * 55}")

    return final_catalog


# ─── Entry point ──────────────────────────────────────────────────────────────
if __name__ == "__main__":
    out = sys.argv[1] if len(sys.argv) > 1 else OUTPUT_DIR
    print("=" * 55)
    print(" Scraper Normativa Tècnica DGC (sincronització incremental)")
    print(f" Destí: {out}/")
    print("=" * 55)
    scrape_all(out)
