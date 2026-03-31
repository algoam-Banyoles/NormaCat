"""
industria_scraper.py
Scraper del portal Ministerio de Industria – Calidad Industrial.

Descobreix totes les subpàgines de la secció de legislació, extreu
els links al BOE, resol la URL del PDF i construeix un catàleg JSON
incremental a normativa_industria/_catalogo/catalogo_industria.json.

Ús:
    python industria_scraper.py [output_dir]
"""

import json
import os
import re
import sys
import time
from datetime import date
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup

# ── Configuració ───────────────────────────────────────────────────────────────

BASE_INDUSTRIA = "https://industria.gob.es/Calidad-Industrial/"
BASE_BOE       = "https://www.boe.es"
OUTPUT_DIR     = "normativa_industria"
DELAY          = 1.5  # segons entre peticions

SECTION_INDEXES = [
    "https://industria.gob.es/Calidad-Industrial/legislaciongeneral/Paginas/index.aspx",
    "https://industria.gob.es/Calidad-Industrial/calidad/Paginas/index.aspx",
    "https://industria.gob.es/Calidad-Industrial/seguridadindustrial/Paginas/index.aspx",
    "https://industria.gob.es/Calidad-Industrial/seguridadindustrial/productosindustriales/Paginas/index.aspx",
    "https://industria.gob.es/Calidad-Industrial/seguridadindustrial/instalacionesindustriales/Paginas/index.aspx",
    "https://industria.gob.es/Calidad-Industrial/vehiculos/Paginas/index.aspx",
    "https://industria.gob.es/Calidad-Industrial/unidaddemercado/Paginas/index.aspx",
]

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept":          "text/html,application/xhtml+xml,*/*;q=0.8",
    "Accept-Language": "es-ES,es;q=0.9,ca;q=0.8",
}

_YEAR_RE = re.compile(r'\b(19|20)\d{2}\b')
_BOE_ID_RE = re.compile(
    r'www\.boe\.es/buscar/(act|doc)\.php\?id=(BOE-[A-Z]-\d{4}-\d+)',
    re.IGNORECASE,
)
_PDF_HREF_RE = re.compile(
    r'/boe/dias/(\d{4})/(\d{2})/(\d{2})/pdfs/(BOE-[A-Za-z0-9\-]+\.pdf)',
    re.IGNORECASE,
)

# ── Sessió HTTP amb reintentos ─────────────────────────────────────────────────

def _make_session() -> requests.Session:
    s = requests.Session()
    s.headers.update(HEADERS)
    adapter = requests.adapters.HTTPAdapter(max_retries=3)
    s.mount("https://", adapter)
    s.mount("http://",  adapter)
    return s


# ── Helpers ────────────────────────────────────────────────────────────────────

def _get(session: requests.Session, url: str, timeout: int = 20):
    """GET amb gestió d'errors. Retorna Response o None."""
    try:
        r = session.get(url, timeout=timeout, allow_redirects=True)
        r.raise_for_status()
        return r
    except requests.RequestException as exc:
        print(f"  [WARN] GET {url} → {exc}")
        return None


def _soup(response) -> BeautifulSoup:
    return BeautifulSoup(response.text, "html.parser")


def _extract_section(url: str) -> str:
    """Extreu el nom de secció de la URL: el segment just després de Calidad-Industrial/."""
    path = urlparse(url).path  # /Calidad-Industrial/legislaciongeneral/Paginas/...
    parts = [p for p in path.split("/") if p]
    try:
        idx = parts.index("Calidad-Industrial")
        return parts[idx + 1] if idx + 1 < len(parts) else "general"
    except ValueError:
        return "general"


def _extract_subsection(url: str) -> str:
    """Extreu el nom de la subpàgina (sense .aspx)."""
    basename = os.path.basename(urlparse(url).path)
    return basename.replace(".aspx", "")


# ── 1. Descoberta de subpàgines ────────────────────────────────────────────────

def discover_subpages(session: requests.Session, index_urls: list[str]) -> list[str]:
    """
    Per cada URL d'índex: descarrega la pàgina, extreu links de subpàgines
    que apunten a Calidad-Industrial/*.aspx (excloent els propis index.aspx).
    Retorna llista deduplicada.
    """
    seen:  set[str]  = set()
    pages: list[str] = []

    # Afegim les pròpies index pages per extreure les normes que hi puguin estar
    for url in index_urls:
        if url not in seen:
            seen.add(url)
            pages.append(url)

    for idx_url in index_urls:
        print(f"  [idx] {idx_url}")
        time.sleep(DELAY)
        resp = _get(session, idx_url)
        if not resp:
            continue

        soup = _soup(resp)
        for a in soup.find_all("a", href=True):
            href = a["href"].strip()
            # Normalitza a URL absoluta
            if href.startswith("/"):
                href = "https://industria.gob.es" + href
            elif not href.startswith("http"):
                href = urljoin(idx_url, href)

            # Filtre: ha d'apuntar al domini propi, dins Calidad-Industrial, acabar en .aspx
            if (
                "industria.gob.es/Calidad-Industrial/" in href
                and href.lower().endswith(".aspx")
                and href not in seen
            ):
                seen.add(href)
                pages.append(href)

    return pages


# ── 2. Extracció de normes per subpàgina ──────────────────────────────────────

def extract_norms(session: requests.Session, url: str) -> list[dict]:
    """
    Descarrega la subpàgina i extreu totes les normes referenciades (links BOE).
    Retorna llista de dicts.
    """
    time.sleep(DELAY)
    resp = _get(session, url)
    if not resp:
        return []

    soup      = _soup(resp)
    seccio    = _extract_section(url)
    subseccio = _extract_subsection(url)
    norms     = []

    # Cerca directament tots els <a> que apunten a boe.es/buscar/(act|doc).php
    for a in soup.find_all("a", href=_BOE_ID_RE):
        href = a["href"].strip()
        m    = _BOE_ID_RE.search(href)
        if not m:
            continue

        tipus_url = m.group(1).lower()   # "act" o "doc"
        boe_id    = m.group(2).upper()   # "BOE-A-1996-2468"

        # Títol: text del link + text del parent si és curt
        titol = a.get_text(separator=" ", strip=True)
        if len(titol) < 15:
            parent = a.find_parent(["li", "p", "td", "div"])
            if parent:
                titol = parent.get_text(separator=" ", strip=True)[:300]
        titol = re.sub(r'\s+', ' ', titol).strip()

        # URL BOE canònica
        url_boe = f"{BASE_BOE}/buscar/{tipus_url}.php?id={boe_id}"

        # Estat inicial
        estat = "VIGENT" if tipus_url == "act" else "PENDENT_VERIFICAR"

        norms.append({
            "boe_id":           boe_id,
            "titol":            titol,
            "url_boe":          url_boe,
            "tipus_url":        tipus_url,
            "estat":            estat,
            "url_pdf":          None,
            "path_local":       None,
            "seccio":           seccio,
            "subseccio":        subseccio,
            "url_pagina_origen": url,
            "data_indexat":     str(date.today()),
            "pdf_descarregat":  False,
        })

    # Dedup per boe_id dins la mateixa pàgina
    seen_ids:  set[str]  = set()
    unique:    list[dict] = []
    for n in norms:
        if n["boe_id"] not in seen_ids:
            seen_ids.add(n["boe_id"])
            unique.append(n)

    return unique


# ── 3. Resolució de la URL del PDF ────────────────────────────────────────────

def resolve_pdf_url(session: requests.Session, norm: dict) -> str | None:
    """
    Visita la pàgina BOE de la norma i intenta extreure la URL del PDF.
    Estratègia 1: link directe a /boe/dias/.../pdfs/BOE-*.pdf al HTML.
    Estratègia 2: camp "Fecha de publicación" → reconstrueix URL.
    Retorna la URL del PDF o None.
    """
    time.sleep(DELAY)
    resp = _get(session, norm["url_boe"])
    if not resp:
        return None

    soup   = _soup(resp)
    text   = resp.text

    # Estratègia 1: link directe al PDF al contingut de la pàgina
    for a in soup.find_all("a", href=_PDF_HREF_RE):
        href = a["href"]
        m    = _PDF_HREF_RE.search(href)
        if m:
            yyyy, mm, dd, fname = m.groups()
            return f"{BASE_BOE}/boe/dias/{yyyy}/{mm}/{dd}/pdfs/{fname}"

    # Estratègia 1b: cerca directa al text HTML (pot estar en un script o meta)
    m = _PDF_HREF_RE.search(text)
    if m:
        yyyy, mm, dd, fname = m.groups()
        return f"{BASE_BOE}/boe/dias/{yyyy}/{mm}/{dd}/pdfs/{fname}"

    # Estratègia 2: "Fecha de publicación: DD/MM/YYYY"
    fecha_re = re.compile(r'Fecha\s+de\s+publicaci[oó]n[:\s]+(\d{2})/(\d{2})/(\d{4})', re.IGNORECASE)
    m = fecha_re.search(resp.text)
    if m:
        dd, mm, yyyy = m.group(1), m.group(2), m.group(3)
        boe_id = norm["boe_id"]
        # El nom del fitxer pot ser BOE-A-YYYY-NNNNN.pdf
        candidate = f"{BASE_BOE}/boe/dias/{yyyy}/{mm}/{dd}/pdfs/{boe_id}.pdf"
        # Verifica existència amb HEAD
        time.sleep(0.5)
        try:
            hr = session.head(candidate, timeout=15, allow_redirects=True)
            if hr.status_code == 200:
                return candidate
        except requests.RequestException:
            pass

    # Estratègia 3: si és doc.php, prova act.php (text consolidat amb PDF directe)
    if norm["tipus_url"] == "doc":
        alt_url = norm["url_boe"].replace("/buscar/doc.php", "/buscar/act.php")
        if alt_url != norm["url_boe"]:
            norm_alt = dict(norm, url_boe=alt_url, tipus_url="act")
            found = resolve_pdf_url(session, norm_alt)
            # Evita recursió infinita: si norm_alt és igual no tornar
            if found:
                return found

    return None


# ── 4. Descàrrega de PDF ───────────────────────────────────────────────────────

def download_pdf(session: requests.Session, norm: dict, output_dir: str) -> bool:
    """
    Descarrega el PDF de la norma. Retorna True si ha reeixit.
    Mai sobreescriu fitxers existents.
    """
    if not norm.get("url_pdf"):
        return False

    dest_dir = os.path.join(output_dir, norm["seccio"])
    os.makedirs(dest_dir, exist_ok=True)
    filename  = norm["boe_id"] + ".pdf"
    dest_path = os.path.join(dest_dir, filename)

    if os.path.exists(dest_path):
        norm["path_local"]      = dest_path.replace("\\", "/")
        norm["pdf_descarregat"] = True
        return True

    time.sleep(DELAY)
    try:
        r = session.get(norm["url_pdf"], timeout=60, stream=True)
        r.raise_for_status()
        content = r.content
        if not content.startswith(b"%PDF"):
            print(f"  [WARN] {norm['boe_id']}: la resposta no és un PDF vàlid")
            return False
        with open(dest_path, "wb") as fh:
            fh.write(content)
        norm["path_local"]      = dest_path.replace("\\", "/")
        norm["pdf_descarregat"] = True
        return True
    except requests.RequestException as exc:
        print(f"  [WARN] PDF {norm['boe_id']}: {exc}")
        return False


# ── 5. Catàleg incremental ────────────────────────────────────────────────────

def _catalog_path(output_dir: str) -> str:
    return os.path.join(output_dir, "_catalogo", "catalogo_industria.json")


def load_catalog(output_dir: str) -> dict[str, dict]:
    """Carrega el catàleg existent. Retorna dict {boe_id: entry}."""
    path = _catalog_path(output_dir)
    if not os.path.exists(path):
        return {}
    with open(path, encoding="utf-8") as fh:
        entries = json.load(fh)
    return {e["boe_id"]: e for e in entries}


def save_catalog(catalog: dict[str, dict], output_dir: str) -> None:
    path = _catalog_path(output_dir)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    entries = sorted(catalog.values(), key=lambda e: e["boe_id"])
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(entries, fh, ensure_ascii=False, indent=2)


def _save_sync_log(log: dict, output_dir: str) -> str:
    log_name = f"sync_{date.today().strftime('%Y%m%d')}.json"
    log_path = os.path.join(output_dir, "_catalogo", log_name)
    os.makedirs(os.path.dirname(log_path), exist_ok=True)
    with open(log_path, "w", encoding="utf-8") as fh:
        json.dump(log, fh, ensure_ascii=False, indent=2)
    return log_path


# ── 6. Orquestrador principal ─────────────────────────────────────────────────

def scrape_all(output_dir: str = OUTPUT_DIR) -> None:
    session = _make_session()
    catalog = load_catalog(output_dir)

    log = {
        "data":        str(date.today()),
        "nous":        0,
        "actualitzats": 0,
        "sense_pdf":   0,
        "errors":      [],
        "total":       0,
    }

    # ── Fase 1: descoberta de subpàgines ──────────────────────────────────────
    print("Descobrint subpàgines...")
    subpages = discover_subpages(session, SECTION_INDEXES)
    print(f"  Subpàgines descobertes: {len(subpages)}")

    # ── Fase 2: extracció de normes ───────────────────────────────────────────
    print("Extraient normes de cada subpàgina...")
    all_norms: dict[str, dict] = {}  # boe_id → norm (dedup global)

    for i, page_url in enumerate(subpages, 1):
        print(f"  [{i}/{len(subpages)}] {page_url}")
        norms = extract_norms(session, page_url)
        for n in norms:
            if n["boe_id"] not in all_norms:
                all_norms[n["boe_id"]] = n
            else:
                # Conserva la subsecció més específica (no index)
                existing = all_norms[n["boe_id"]]
                if existing["subseccio"] in ("index", "") and n["subseccio"] not in ("index", ""):
                    all_norms[n["boe_id"]] = n

    print(f"  Normes detectades (úniques): {len(all_norms)}")

    # ── Fase 3 + 4: resolució PDF i descàrrega ────────────────────────────────
    print("Resolent PDFs i descarregant...")
    pdfs_ok      = 0
    pdfs_err     = 0

    for boe_id, norm in all_norms.items():
        existing = catalog.get(boe_id)

        if existing and existing.get("pdf_descarregat"):
            # Ja al catàleg i descarregat: skip
            catalog[boe_id] = existing  # manté entrada existent
            continue

        if existing:
            # Existia però sense PDF: actualitza metadades i reintenta
            existing.update({
                k: norm[k]
                for k in ("titol", "url_boe", "tipus_url", "estat",
                          "seccio", "subseccio", "url_pagina_origen")
                if norm.get(k)
            })
            norm = existing
            is_new = False
        else:
            is_new = True

        # Resol URL del PDF si no la tenim
        if not norm.get("url_pdf"):
            pdf_url = resolve_pdf_url(session, norm)
            if pdf_url:
                norm["url_pdf"] = pdf_url
            else:
                print(f"    [NO PDF] {boe_id}")
                log["errors"].append({"boe_id": boe_id, "motiu": "pdf_url no resolta"})
                pdfs_err += 1
                norm["url_pdf"] = None

        # Descarrega si tenim URL
        if norm.get("url_pdf"):
            ok = download_pdf(session, norm, output_dir)
            if ok:
                pdfs_ok  += 1
                if is_new:
                    log["nous"] += 1
                else:
                    log["actualitzats"] += 1
            else:
                pdfs_err += 1
                log["errors"].append({"boe_id": boe_id, "motiu": "descàrrega fallida"})
        else:
            log["sense_pdf"] += 1
            if is_new:
                log["nous"] += 1

        catalog[boe_id] = norm

    # ── Desa catàleg i log ────────────────────────────────────────────────────
    log["total"] = len(catalog)
    save_catalog(catalog, output_dir)
    log_path = _save_sync_log(log, output_dir)

    print()
    print(f"Subpàgines descobertes: {len(subpages)}")
    print(f"Normes detectades:      {len(all_norms)}")
    print(f"PDFs descarregats:      {pdfs_ok}")
    print(f"Sense PDF (error):      {pdfs_err}")
    print(f"Log guardat: {log_path}")


# ── Entry point ────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    out = sys.argv[1] if len(sys.argv) > 1 else OUTPUT_DIR
    scrape_all(out)
