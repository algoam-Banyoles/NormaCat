"""
aca_scraper.py — Catàleg de documents tècnics i normativa de l'ACA
Font: https://aca.gencat.cat

Estratègia:
  1. Scraping de tres seccions (documents tècnics, plans, normativa).
  2. Per cada secció, s'extreuen els <a> del contingut principal.
  3. Si l'enllaç és un PDF/DOC → document directe.
  4. Si l'enllaç és una pàgina interna → s'entra un nivell per buscar PDFs.
  5. S'aplica un delay d'1 segon entre peticions.

Ús:
    python normativa_aca/aca_scraper.py [output_dir]
"""

import json
import os
import re
import sys
import time
from datetime import datetime
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ─── Constants ────────────────────────────────────────────────────────────────
BASE_URL   = "https://aca.gencat.cat"
OUTPUT_DIR = "normativa_aca"
DELAY      = 1.0    # seconds between requests
TIMEOUT    = 30     # seconds per request

SECTIONS = [
    {
        "url":    BASE_URL + "/ca/laca/publicacions/estudis-i-informes-tecnics/",
        "label":  "Estudis i informes tècnics",
        "prefix": "ACA-DOC",
        "norm_refs": False,
    },
    {
        "url":    BASE_URL + "/ca/laca/perfil-del-contractant/normes-de-redaccio-de-projectes/",
        "label":  "Normes de redacció de projectes",
        "prefix": "ACA-DOC",
        "norm_refs": False,
    },
    {
        "url":    BASE_URL + "/ca/plans-i-programes/",
        "label":  "Plans i programes",
        "prefix": "ACA-PLA",
        "norm_refs": False,
    },
    {
        "url":    BASE_URL + "/ca/laca/normativa/normativa-substantiva-en-materia-daiguees/",
        "label":  "Normativa substantiva aigües",
        "prefix": "ACA-NOR",
        "norm_refs": True,
    },
    {
        "url":    BASE_URL + "/ca/laca/normativa/planificacio-hidrologica/",
        "label":  "Normativa planificació hidrològica",
        "prefix": "ACA-NOR",
        "norm_refs": True,
    },
    {
        "url":    BASE_URL + "/ca/laca/normativa/proteccio-de-les-aiguees/",
        "label":  "Normativa protecció aigües",
        "prefix": "ACA-NOR",
        "norm_refs": True,
    },
    {
        "url":    BASE_URL + "/ca/laca/normativa/normativa-incidental/",
        "label":  "Normativa incidental",
        "prefix": "ACA-NOR",
        "norm_refs": True,
    },
]

# Extensions that indicate a direct downloadable document
_DOC_EXTS = {".pdf", ".doc", ".docx", ".xls", ".xlsx", ".zip"}

# External normative sources accepted as document URLs
_NORM_HOSTS = {"dogc.gencat.cat", "boe.es", "www.boe.es", "portaljuridic.gencat.cat",
               "www.dogc.gencat.cat", "doue.gencat.cat"}

# Gencat content container selectors (try in order)
_CONTENT_SELECTORS = [
    "div.contingut",
    "div#contingut",
    "main",
    "article",
    "div.cos",
    "div.container",
]


# ─── Session ──────────────────────────────────────────────────────────────────

def _make_session() -> requests.Session:
    session = requests.Session()
    retry = Retry(
        total=2,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET", "HEAD"],
    )
    session.mount("https://", HTTPAdapter(max_retries=retry))
    session.mount("http://",  HTTPAdapter(max_retries=retry))
    session.headers.update({
        "User-Agent":      "Mozilla/5.0 (compatible; ProjectChecker/1.0)",
        "Accept":          "text/html,application/xhtml+xml,*/*;q=0.8",
        "Accept-Language": "ca,es;q=0.9",
    })
    return session


def _fetch(session: requests.Session, url: str) -> requests.Response | None:
    """GET with one retry on timeout. Returns None on failure."""
    for attempt in range(2):
        try:
            resp = session.get(url, timeout=TIMEOUT)
            resp.raise_for_status()
            return resp
        except requests.Timeout:
            if attempt == 0:
                print(f"  [WARN] Timeout {url} — reintentant...", flush=True)
                time.sleep(2)
            else:
                print(f"  [WARN] Timeout definitiu: {url}", flush=True)
                return None
        except requests.HTTPError as exc:
            print(f"  [WARN] HTTP {exc.response.status_code}: {url}", flush=True)
            return None
        except Exception as exc:
            print(f"  [WARN] Error {type(exc).__name__}: {url}", flush=True)
            return None
    return None


# ─── Text utilities ────────────────────────────────────────────────────────────

def _slugify(text: str, max_len: int = 60) -> str:
    """Lowercase slug with hyphens, max max_len chars."""
    import unicodedata
    text = unicodedata.normalize("NFKD", text or "")
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    text = text.lower()
    text = re.sub(r"[^\w\s-]", "", text)
    text = re.sub(r"[\s_]+", "-", text).strip("-")
    return text[:max_len]


def _classify_tipus(title: str) -> str:
    t = title.lower()
    if any(w in t for w in ("guia", "manual", "recomanació", "recomanacio")):
        return "guia"
    if any(w in t for w in ("criteri", "criteris")):
        return "criteri"
    if any(w in t for w in ("instrucció", "instruccio", "instruccions", "norma")):
        return "instruccio"
    if any(w in t for w in ("pla", "programa", "protocol")):
        return "pla"
    return "altre"


def _extract_year(text: str) -> int | None:
    m = re.search(r"\b(19[89]\d|20[012]\d)\b", text or "")
    return int(m.group(1)) if m else None


def _extract_temes(title: str) -> list[str]:
    t = title.lower()
    temes = []
    if any(w in t for w in ("hidrològ", "hidràulic", "hidraul", "hidrologi", "inundab")):
        temes.append("hidrologia")
    if any(w in t for w in ("drenatge", "abocament")):
        temes.append("drenatge")
    if any(w in t for w in ("qualitat", "contaminac")):
        temes.append("qualitat-aigues")
    if any(w in t for w in ("llera", "domini públic hidràulic", "domini public hidraulic")):
        temes.append("dph")
    if any(w in t for w in ("dma", "directiva marc")):
        temes.append("dma")
    if any(w in t for w in ("sequera", "escassetat")):
        temes.append("sequera")
    if any(w in t for w in ("depurador", "edar", "sanejament")):
        temes.append("sanejament")
    if any(w in t for w in ("abastament", "subministrament")):
        temes.append("abastament")
    if any(w in t for w in ("preses", "embassaments", "presa")):
        temes.append("preses")
    if any(w in t for w in ("zona inundable", "pgri", "risc inundació", "risc inundacio")):
        temes.append("risc-inundacio")
    return temes


def _is_doc_url(url: str) -> bool:
    ext = os.path.splitext(urlparse(url).path)[1].lower()
    return ext in _DOC_EXTS


def _is_norm_ref_url(url: str) -> bool:
    """Returns True for external normative sources (DOGC, BOE, Portal Jurídic)."""
    return urlparse(url).netloc in _NORM_HOSTS


def _is_internal_aca(url: str) -> bool:
    parsed = urlparse(url)
    return parsed.netloc in ("", "aca.gencat.cat") and bool(parsed.path)


def _find_content(soup: BeautifulSoup) -> BeautifulSoup:
    """Return the main content container, or full body as fallback."""
    for sel in _CONTENT_SELECTORS:
        el = soup.select_one(sel)
        if el:
            return el
    return soup.body or soup


def _clean_title(text: str) -> str:
    text = re.sub(r"\s+", " ", text or "").strip()
    # Remove "opens in new window" boilerplate added by Gencat
    text = re.sub(r"\s*[\(\[]obre en una nova finestra[\)\]]", "", text, flags=re.IGNORECASE)
    text = re.sub(r"\s*[\(\[]opens? in (a )?new (tab|window)[\)\]]", "", text, flags=re.IGNORECASE)
    return text.strip()


# ─── Document building ────────────────────────────────────────────────────────

def _make_doc(
    counter: int,
    prefix: str,
    title: str,
    url_fitxa: str,
    url_document: str | None,
) -> dict:
    """Build a single document record."""
    title = _clean_title(title)
    return {
        "id":              _slugify(title),
        "codi":            f"{prefix}-{counter:03d}",
        "titol":           title,
        "tipus":           _classify_tipus(title),
        "estat":           "DEROGADA" if "derogat" in title.lower() else "VIGENT",
        "url_fitxa":       url_fitxa,
        "url_document":    url_document,
        "any_publicacio":  _extract_year(title),
        "temes":           _extract_temes(title),
        "font":            "ACA",
        "observacions":    "",
    }


# ─── Sub-page extraction ──────────────────────────────────────────────────────

def _extract_docs_from_page(
    session: requests.Session,
    page_url: str,
    prefix: str,
    counter_start: int,
    visited: set,
    depth: int = 0,
    capture_norm_refs: bool = False,
) -> list[dict]:
    """
    Extract document records from a single ACA page.
    - capture_norm_refs=True: also capture links to DOGC/BOE/PortalJurídic.
    - If depth < 1 and a link leads to an internal ACA page (not a doc),
      follow it one level deep.
    """
    if page_url in visited:
        return []
    visited.add(page_url)

    resp = _fetch(session, page_url)
    if resp is None:
        return []

    soup = BeautifulSoup(resp.text, "html.parser")
    content = _find_content(soup)

    docs = []
    seen_titles: set[str] = set()
    counter = counter_start

    def _try_add(title: str, url_fitxa: str, url_document: str | None) -> bool:
        """Add doc if title is new. Returns True if added."""
        nonlocal counter
        title = _clean_title(title)
        if not title or len(title) < 4:
            return False
        key = title.lower()[:50]
        if key in seen_titles:
            return False
        seen_titles.add(key)
        docs.append(_make_doc(counter, prefix, title, url_fitxa, url_document))
        counter += 1
        return True

    # ── Step 1: Collect direct document and normative links ───────────────────
    for a in content.find_all("a", href=True):
        href = a["href"].strip()
        if not href or href.startswith("#") or href.startswith("mailto:"):
            continue

        abs_url = urljoin(page_url, href)
        title   = _clean_title(a.get_text())
        if not title or len(title) < 4:
            parent_text = _clean_title(a.find_parent().get_text()) if a.find_parent() else ""
            title = parent_text[:120] if parent_text else os.path.basename(urlparse(abs_url).path)

        if _is_doc_url(abs_url):
            _try_add(title, abs_url, abs_url)
        elif capture_norm_refs and _is_norm_ref_url(abs_url):
            # External normative reference (DOGC, BOE, Portal Jurídic)
            _try_add(title, abs_url, abs_url)

    # ── Step 2: Follow internal sub-pages one level deep ─────────────────────
    if depth == 0:
        sub_links = []
        for a in content.find_all("a", href=True):
            href = a["href"].strip()
            if not href or href.startswith("#") or href.startswith("mailto:"):
                continue
            abs_url = urljoin(page_url, href)
            if (
                _is_internal_aca(abs_url)
                and not _is_doc_url(abs_url)
                and abs_url not in visited
                and abs_url != page_url
                and "/ca/" in abs_url
                and len(urlparse(abs_url).path) > len(urlparse(page_url).path)
            ):
                sub_links.append((abs_url, _clean_title(a.get_text())))

        for sub_url, sub_hint in sub_links[:30]:
            time.sleep(DELAY)
            sub_resp = _fetch(session, sub_url)
            if sub_resp is None:
                continue

            sub_soup = BeautifulSoup(sub_resp.text, "html.parser")
            sub_content = _find_content(sub_soup)

            page_title = _clean_title(
                (sub_soup.find("h1") or sub_soup.find("h2") or sub_soup.find("title") or sub_soup).get_text()
            )

            found_doc = False
            for a in sub_content.find_all("a", href=True):
                href = a["href"].strip()
                if not href:
                    continue
                abs_url2 = urljoin(sub_url, href)
                raw_title = _clean_title(a.get_text())
                title = raw_title if len(raw_title) >= 4 else page_title

                if _is_doc_url(abs_url2):
                    if _try_add(title, sub_url, abs_url2):
                        found_doc = True
                elif capture_norm_refs and _is_norm_ref_url(abs_url2):
                    if _try_add(title, sub_url, abs_url2):
                        found_doc = True

            if not found_doc and page_title and len(page_title) >= 6:
                _try_add(page_title, sub_url, None)

    return docs


# ─── Debug helper ────────────────────────────────────────────────────────────

def debug_page(url: str) -> None:
    """Fetch a page and show all links found — useful for diagnosing 0-result pages."""
    headers = {"User-Agent": "Mozilla/5.0 (compatible; ProjectChecker/1.0)"}
    r = requests.get(url, headers=headers, timeout=30)
    print(f"Status: {r.status_code}")
    print(f"Content-Type: {r.headers.get('content-type', '')}")
    soup = BeautifulSoup(r.text, "html.parser")

    all_links = soup.find_all("a", href=True)
    print(f"\nTotal links found: {len(all_links)}")

    pdf_links = [a for a in all_links if ".pdf" in a["href"].lower()]
    print(f"PDF links found: {len(pdf_links)}")
    for a in pdf_links[:20]:
        print(f"  {a.get_text(strip=True)[:60]} → {a['href']}")

    doc_links = [
        a for a in all_links
        if any(x in a["href"].lower() for x in
               ("document", "arxiu", "fitxer", "download", "descarrega",
                "publicacio", "/doc/", "getFile"))
    ]
    print(f"\nDoc-pattern links: {len(doc_links)}")
    for a in doc_links[:20]:
        print(f"  {a.get_text(strip=True)[:60]} → {a['href']}")

    print("\n--- HTML snippet (first 3000 chars) ---")
    print(r.text[:3000])


# ─── Detail-page PDF extractor ────────────────────────────────────────────────

def get_pdf_from_detail_page(url: str, session: requests.Session) -> str | None:
    """Visit a document detail page and extract the direct PDF/document URL."""
    try:
        r = session.get(url, timeout=30)
        soup = BeautifulSoup(r.text, "html.parser")

        for a in soup.find_all("a", href=True):
            href = a["href"]
            text = a.get_text(strip=True).lower()
            if _is_doc_url(href):
                return href if href.startswith("http") else urljoin(BASE_URL, href)
            if any(w in text for w in ("descarregar", "descarrega", "baixar", "obrir", "document")):
                if _is_doc_url(href):
                    return href if href.startswith("http") else urljoin(BASE_URL, href)

        # Gencat data-url pattern
        for tag in soup.find_all(attrs={"data-url": True}):
            if _is_doc_url(tag["data-url"]):
                return tag["data-url"]

        return None
    except Exception as exc:
        print(f"  [WARN] Error fetching detail page {url}: {exc}", flush=True)
        return None


# ─── PDF downloader ────────────────────────────────────────────────────────────

def download_pdf(url: str, dest_dir: str, filename: str,
                 session: requests.Session) -> bool:
    """Download a PDF/document to dest_dir/filename. Returns True on success."""
    os.makedirs(dest_dir, exist_ok=True)
    filepath = os.path.join(dest_dir, filename)

    if os.path.exists(filepath):
        return True   # already present

    try:
        r = session.get(url, timeout=60, stream=True)
        ctype = r.headers.get("content-type", "").lower()
        if r.status_code == 200 and any(x in ctype for x in ("pdf", "octet", "document", "zip")):
            with open(filepath, "wb") as fh:
                for chunk in r.iter_content(chunk_size=8192):
                    fh.write(chunk)
            size_kb = os.path.getsize(filepath) // 1024
            print(f"  ✅ {filename} ({size_kb} KB)", flush=True)
            return True
        print(f"  [WARN] {r.status_code} / {ctype}: {url}", flush=True)
        return False
    except Exception as exc:
        print(f"  [WARN] Download failed {url}: {exc}", flush=True)
        return False


# ─── Main catalog builder ─────────────────────────────────────────────────────

def build_catalog(output_dir: str = OUTPUT_DIR, download: bool = False) -> tuple[list[dict], dict]:
    session    = _make_session()
    visited    = set()
    all_docs   = []
    prefix_ctr: dict[str, int] = {}   # sequential counter per prefix

    for section in SECTIONS:
        url    = section["url"]
        label  = section["label"]
        prefix = section["prefix"]

        start = prefix_ctr.get(prefix, 1)
        print(f"\n  Secció: {label}", flush=True)
        print(f"  URL: {url}", flush=True)

        docs = _extract_docs_from_page(
            session,
            url,
            prefix,
            counter_start=start,
            visited=visited,
            capture_norm_refs=section.get("norm_refs", False),
        )

        if not docs:
            print(f"  [INFO] Secció {label}: 0 docs found", flush=True)
        else:
            print(f"  → {len(docs)} documents", flush=True)
            prefix_ctr[prefix] = start + len(docs)

        all_docs.extend(docs)
        time.sleep(DELAY)

    # ── Deduplicate by url_fitxa + url_document ──────────────────────────────
    seen_urls: set[str] = set()
    unique_docs = []
    for doc in all_docs:
        key = doc.get("url_document") or doc.get("url_fitxa") or doc["id"]
        if key not in seen_urls:
            seen_urls.add(key)
            unique_docs.append(doc)

    # ── Optional PDF download ─────────────────────────────────────────────────
    pdf_dir        = os.path.join(output_dir, "pdfs")
    downloaded_cnt = 0
    if download:
        print("\n  Descarregant PDFs directes...", flush=True)
        for doc in unique_docs:
            url_d = doc.get("url_document", "")
            if not url_d or not _is_doc_url(url_d):
                continue
            slug     = (doc.get("id") or "doc")[:60]
            ext      = os.path.splitext(urlparse(url_d).path)[1] or ".pdf"
            filename = f"{slug}{ext}"
            ok = download_pdf(url_d, pdf_dir, filename, session)
            if ok:
                doc["url_local"] = os.path.join("normativa_aca", "pdfs", filename)
                downloaded_cnt  += 1
            time.sleep(0.5)

    # ── Build catalog ────────────────────────────────────────────────────────
    meta = {
        "font":             "Agència Catalana de l'Aigua",
        "url_base":         BASE_URL,
        "data_scraping":    datetime.now().isoformat()[:10],
        "total_documents":  len(unique_docs),
        "versio":           "1.0",
    }
    catalog = {"metadata": meta, "documents": unique_docs}

    # ── Save JSON ────────────────────────────────────────────────────────────
    catalog_dir = os.path.join(output_dir, "_catalogo")
    os.makedirs(catalog_dir, exist_ok=True)
    json_path = os.path.join(catalog_dir, "catalogo_aca.json")
    with open(json_path, "w", encoding="utf-8") as fh:
        json.dump(catalog, fh, ensure_ascii=False, indent=2)

    # ── Save resum ───────────────────────────────────────────────────────────
    _save_resum(unique_docs, catalog_dir)

    return unique_docs, meta


def _save_resum(docs: list[dict], catalog_dir: str) -> None:
    tipus_counts: dict[str, int] = {}
    temes_counts: dict[str, int] = {}
    pdfs_directes = 0

    for doc in docs:
        t = doc.get("tipus", "altre")
        tipus_counts[t] = tipus_counts.get(t, 0) + 1
        for tema in doc.get("temes", []):
            temes_counts[tema] = temes_counts.get(tema, 0) + 1
        url_d = doc.get("url_document", "")
        if url_d and _is_doc_url(url_d):
            pdfs_directes += 1

    lines = [
        f"Total documents: {len(docs)}",
        "Per tipus: " + ", ".join(
            f"{k}={v}" for k, v in sorted(tipus_counts.items())
        ),
        "Per tema: " + (", ".join(
            f"{k}={v}" for k, v in sorted(temes_counts.items())
        ) or "(cap tema detectat)"),
        f"PDFs directes trobats: {pdfs_directes}",
    ]

    txt_path = os.path.join(catalog_dir, "resum_aca.txt")
    with open(txt_path, "w", encoding="utf-8") as fh:
        fh.write("\n".join(lines) + "\n")


# ─── Entry point ──────────────────────────────────────────────────────────────

if __name__ == "__main__":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

    args    = sys.argv[1:]
    do_dl   = "--download" in args
    do_dbg  = "--debug"    in args
    out_dir = next((a for a in args if not a.startswith("--")), OUTPUT_DIR)

    if do_dbg:
        debug_page(SECTIONS[0]["url"])
        sys.exit(0)

    print("=" * 55)
    print(" ACA Scraper — Agència Catalana de l'Aigua")
    print(f" Destí: {os.path.join(out_dir, '_catalogo', 'catalogo_aca.json')}")
    if do_dl:
        print(" Mode: scraping + descàrrega de PDFs")
    print("=" * 55)

    docs, meta = build_catalog(out_dir, download=do_dl)

    direct_pdfs  = sum(1 for d in docs if d.get("url_document") and _is_doc_url(d["url_document"]))
    downloaded   = sum(1 for d in docs if d.get("url_local"))

    print()
    print("✅ ACA scraping complete")
    print(f"📄 Documents found: {meta['total_documents']}")
    print(f"🔗 Direct PDFs detected: {direct_pdfs}")
    if do_dl:
        print(f"💾 PDFs downloaded: {downloaded}  (to {os.path.join(out_dir, 'pdfs')})")
    print(f"💾 Catalog saved to {os.path.join(out_dir, '_catalogo', 'catalogo_aca.json')}")
