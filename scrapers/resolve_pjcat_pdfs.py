"""
resolve_pjcat_pdfs.py
Resol les URL dels PDFs per a les 12 entrades del catàleg PJCAT via ELI.

ELI /eli/es/...    → BOE (www.boe.es)
ELI /eli/es-ct/... → Portal Jurídic / DOGC (portaljuridic.gencat.cat)

Ús:
    python resolve_pjcat_pdfs.py
"""

import json
import os
import re
import sys
import time

import requests
from bs4 import BeautifulSoup

try:
    from curl_cffi import requests as cffi_requests
    _CFFI_AVAILABLE = True
except ImportError:
    _CFFI_AVAILABLE = False

# ── Constants ──────────────────────────────────────────────────────────────────

CATALOG_PATH = os.path.join("normativa_pjcat", "_catalogo", "catalogo_pjcat.json")
PDF_DIR      = os.path.join("normativa_pjcat", "pdfs")
BASE_BOE     = "https://www.boe.es"
BASE_PJCAT   = "https://portaljuridic.gencat.cat"
BASE_DOGC    = "https://portaldogc.gencat.cat"
DELAY        = 2.0

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept":     "text/html,application/pdf,*/*;q=0.8",
    "Accept-Language": "ca-ES,ca;q=0.9,es;q=0.7",
}

_BOE_PDF_RE  = re.compile(
    r"/boe/dias/(\d{4})/(\d{2})/(\d{2})/pdfs/(BOE-[A-Za-z0-9\-]+\.pdf)",
    re.IGNORECASE,
)
_BOE_ID_RE   = re.compile(r"BOE-[A-Z]-\d{4}-\d+", re.IGNORECASE)
_DOGC_PDF_RE = re.compile(
    r"portaldogc\.gencat\.cat/utilsEADOP/PDF/\d+/[\w\-]+\.pdf",
    re.IGNORECASE,
)
_FECHA_RE = re.compile(
    r"(\d{1,2})\s+de\s+(\w+)\s+de\s+(\d{4})",
    re.IGNORECASE,
)
_FECHA2_RE = re.compile(
    r"Fecha de publicaci[oó]n[:\s]+(\d{2})[/\-](\d{2})[/\-](\d{4})",
    re.IGNORECASE,
)
_MESOS_ES = {
    "enero": "01", "febrero": "02", "marzo": "03", "abril": "04",
    "mayo": "05", "junio": "06", "julio": "07", "agosto": "08",
    "septiembre": "09", "octubre": "10", "noviembre": "11", "diciembre": "12",
}


# ── Session ────────────────────────────────────────────────────────────────────

def _make_session() -> requests.Session:
    s = requests.Session()
    s.headers.update(HEADERS)
    s.mount("https://", requests.adapters.HTTPAdapter(max_retries=3))
    return s


# ── Helpers ────────────────────────────────────────────────────────────────────

def _get(session, url: str, timeout: int = 20):
    try:
        r = session.get(url, timeout=timeout, allow_redirects=True)
        r.raise_for_status()
        return r
    except requests.RequestException as exc:
        print(f"      [GET] {url} → {exc}")
        return None


def _cffi_get(url: str, timeout: int = 30):
    """GET via curl_cffi (Chrome TLS) per servidors amb SSL antic com portaldogc."""
    if not _CFFI_AVAILABLE:
        return None
    try:
        r = cffi_requests.get(url, timeout=timeout, impersonate="chrome120", allow_redirects=True)
        if r.status_code >= 400:
            return None
        return r
    except Exception as exc:
        print(f"      [CFFI] {url} → {exc}")
        return None


def _head_ok(session, url: str) -> bool:
    try:
        r = session.head(url, timeout=15, allow_redirects=True)
        if r.status_code != 200:
            return False
        ct = r.headers.get("Content-Type", "")
        return "pdf" in ct.lower() or url.lower().endswith(".pdf")
    except requests.RequestException:
        return False


def _download_pdf(url: str, dest: str, session) -> bool:
    def _save(content: bytes) -> bool:
        if content[:4] != b"%PDF":
            return False
        os.makedirs(os.path.dirname(dest), exist_ok=True)
        with open(dest, "wb") as fh:
            fh.write(content)
        return True

    # Per URLs de portaldogc / portaljuridic, usa curl_cffi directament (SSL antic)
    is_dogc = "portaldogc.gencat.cat" in url or "portaljuridic.gencat.cat" in url
    if is_dogc and _CFFI_AVAILABLE:
        try:
            r = cffi_requests.get(url, timeout=60, impersonate="chrome120", allow_redirects=True)
            if _save(r.content):
                return True
        except Exception as exc:
            print(f"      [DL cffi] {url} → {exc}")

    # Attempt estàndard (requests)
    try:
        r = session.get(url, timeout=60, stream=True)
        r.raise_for_status()
        if _save(r.content):
            return True
    except requests.RequestException as exc:
        print(f"      [DL] {url} → {exc}")

    # Fallback curl_cffi per qualsevol URL si requests falla
    if not is_dogc and _CFFI_AVAILABLE:
        try:
            r = cffi_requests.get(url, timeout=60, impersonate="chrome120", allow_redirects=True)
            if _save(r.content):
                return True
        except Exception as exc:
            print(f"      [DL cffi] {url} → {exc}")

    return False


def _pdf_links_from_html(html: str) -> list[str]:
    """Extreu tots els links a PDFs d'un HTML."""
    soup = BeautifulSoup(html, "html.parser")
    links = []
    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        if ".pdf" in href.lower():
            links.append(href)
    return links


# ── Resolució BOE (ELI /eli/es/...) ──────────────────────────────────────────

def _resolve_boe_eli(eli_path: str, session) -> str | None:
    """
    GET https://www.boe.es/eli/{eli_path} → segueix redirect → pàgina BOE.
    Cerca link al PDF al HTML resultant.
    """
    uri = BASE_BOE + eli_path
    time.sleep(DELAY)
    resp = _get(session, uri)
    if not resp:
        return None

    html = resp.text

    # Extreu BOE-ID de la URL final (redirect) per usar-lo en tots els intents
    final_id_m = re.search(r"id=(BOE-[A-Z]-\d{4}-\d+)", resp.url, re.IGNORECASE)
    boe_id_from_url = final_id_m.group(1) if final_id_m else None

    # Si no és a la URL, busca qualsevol BOE-ID al cos HTML
    if not boe_id_from_url:
        id_in_html = _BOE_ID_RE.search(html)
        if id_in_html:
            boe_id_from_url = id_in_html.group(0).upper()

    # Opció 1: patró directe /boe/dias/YYYY/MM/DD/pdfs/BOE-*.pdf al HTML
    m = _BOE_PDF_RE.search(html)
    if m:
        yyyy, mm, dd, fname = m.groups()
        return f"{BASE_BOE}/boe/dias/{yyyy}/{mm}/{dd}/pdfs/{fname}"

    # Opció 2: link <a href="..."> que contingui .pdf
    for href in _pdf_links_from_html(html):
        m = _BOE_PDF_RE.search(href)
        if m:
            yyyy, mm, dd, fname = m.groups()
            return f"{BASE_BOE}/boe/dias/{yyyy}/{mm}/{dd}/pdfs/{fname}"
        if href.startswith("http") and ".pdf" in href.lower():
            return href
        if href.startswith("/") and ".pdf" in href.lower():
            return BASE_BOE + href

    # Opció 3: text consolidat (suffix /con) → BOE busca act.php amb id
    if boe_id_from_url:
        year_m = re.search(r"BOE-[A-Z]-(\d{4})-", boe_id_from_url)
        if year_m:
            consol = f"{BASE_BOE}/buscar/pdf/{year_m.group(1)}/{boe_id_from_url}-consolidado.pdf"
            time.sleep(0.5)
            if _head_ok(session, consol):
                return consol

    # Opció 4: pàgina act.php directa → reparse buscant PDF
    if boe_id_from_url:
        act_url = f"{BASE_BOE}/buscar/act.php?id={boe_id_from_url}"
        if act_url != resp.url:
            time.sleep(DELAY)
            resp_act = _get(session, act_url)
            if resp_act:
                m2 = _BOE_PDF_RE.search(resp_act.text)
                if m2:
                    yyyy, mm, dd, fname = m2.groups()
                    return f"{BASE_BOE}/boe/dias/{yyyy}/{mm}/{dd}/pdfs/{fname}"
                for href in _pdf_links_from_html(resp_act.text):
                    if ".pdf" in href.lower():
                        return href if href.startswith("http") else BASE_BOE + href

    # Opció 5: dedueix data de publicació i construeix URL
    m = _FECHA_RE.search(html)
    if m:
        dd_s = m.group(1).zfill(2)
        mes_s = m.group(2).lower()
        yyyy  = m.group(3)
        mm_s  = _MESOS_ES.get(mes_s)
        if mm_s and boe_id_from_url:
            candidate = f"{BASE_BOE}/boe/dias/{yyyy}/{mm_s}/{dd_s}/pdfs/{boe_id_from_url}.pdf"
            time.sleep(0.5)
            if _head_ok(session, candidate):
                return candidate

    m = _FECHA2_RE.search(html)
    if m:
        dd_s, mm_s, yyyy = m.group(1), m.group(2), m.group(3)
        if boe_id_from_url:
            candidate = f"{BASE_BOE}/boe/dias/{yyyy}/{mm_s}/{dd_s}/pdfs/{boe_id_from_url}.pdf"
            time.sleep(0.5)
            if _head_ok(session, candidate):
                return candidate

    return None


# ── Resolució Portal Jurídic / DOGC (ELI /eli/es-ct/...) ─────────────────────

def _resolve_pjcat_eli(eli_path: str, session) -> str | None:
    """
    GET https://portaljuridic.gencat.cat/eli/es-ct/... → pàgina amb link .pdf
    El Portal Jurídic inclou normalment un link a portaldogc.gencat.cat/utilsEADOP/PDF/...
    usa curl_cffi per a portaldogc.gencat.cat (SSL antic incompatible amb Python requests)
    """
    uri = BASE_PJCAT + eli_path
    time.sleep(DELAY)
    resp = _get(session, uri)
    html = resp.text if resp else ""

    # Si requests falla o retorna HTML buit, intenta amb curl_cffi
    if not html.strip() and _CFFI_AVAILABLE:
        r2 = _cffi_get(uri)
        if r2:
            html = r2.text

    def _check_html(html_str):
        # Opció A: link directe a portaldogc.gencat.cat/utilsEADOP/PDF/...
        m = _DOGC_PDF_RE.search(html_str)
        if m:
            return "https://" + m.group(0)
        # Opció B: qualsevol link .pdf
        for href in _pdf_links_from_html(html_str):
            if "dogc" in href.lower() and ".pdf" in href.lower():
                if href.startswith("http"):
                    return href
                if href.startswith("/"):
                    return BASE_DOGC + href
        return None

    result = _check_html(html)
    if result:
        return result

    # Opció 3: segueix links a portaldogc.gencat.cat (no PDF) — usa curl_cffi
    soup = BeautifulSoup(html, "html.parser")
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if "portaldogc.gencat.cat" in href and "PDF" not in href:
            time.sleep(DELAY)
            r3 = _cffi_get(href) if _CFFI_AVAILABLE else None
            if not r3:
                r3 = _get(session, href)
            if r3:
                result = _check_html(r3.text)
                if result:
                    return result
                for href2 in _pdf_links_from_html(r3.text):
                    if ".pdf" in href2.lower():
                        if href2.startswith("http"):
                            return href2
                        if href2.startswith("/"):
                            from urllib.parse import urljoin
                            return urljoin(href, href2)
            break

    # Opció 4: ELI suffix /dof/{lang}/pdf — usa curl_cffi per SSL DOGC
    for suffix in ("/dof/cat/pdf", "/dof/spa/pdf"):
        pdf_url = uri.rstrip("/") + suffix
        time.sleep(0.5)
        r4 = _cffi_get(pdf_url) if _CFFI_AVAILABLE else None
        if r4 and (r4.status_code == 200 if hasattr(r4, "status_code") else True):
            ct = r4.headers.get("Content-Type", "") if hasattr(r4, "headers") else ""
            if "pdf" in ct.lower() or r4.content[:4] == b"%PDF":
                return pdf_url
        # Fallback requests
        try:
            r4b = session.get(pdf_url, timeout=30, allow_redirects=True)
            if r4b.status_code == 200:
                ct = r4b.headers.get("Content-Type", "")
                if "pdf" in ct.lower() or r4b.content[:4] == b"%PDF":
                    return pdf_url
        except Exception:
            pass

    # Opció 5: DOGC search API via curl_cffi (SSL antic)
    dogc_api = (
        "https://portaldogc.gencat.cat/utilsEADOP/AppJava/action/PortalEntity.do"
        f"?action=findDocumentByEli&eli={requests.utils.quote(eli_path)}"
    )
    time.sleep(DELAY)
    r5 = _cffi_get(dogc_api) if _CFFI_AVAILABLE else None
    if not r5:
        r5 = _get(session, dogc_api)
    if r5:
        result = _check_html(r5.text)
        if result:
            return result

    return None


# ── Fallback per codi al BOE ──────────────────────────────────────────────────

def _resolve_by_codi_boe(codi: str, session) -> str | None:
    """
    Cerca el codi normalitzat al buscador del BOE com a darrer recurs.
    Exemple: 'RD-1627/1997' → cerca 'RD 1627/1997' al BOE
    """
    query = codi.replace("-", " ").replace("/", " ")
    search_url = (
        f"{BASE_BOE}/buscar/boe.php?"
        f"campo[0]=TEXTO&dato[0]={requests.utils.quote(codi)}&page_hits=5"
    )
    time.sleep(DELAY)
    resp = _get(session, search_url)
    if not resp:
        return None

    soup = BeautifulSoup(resp.text, "html.parser")
    # Cerca el primer resultat amb link a buscar/act.php
    for a in soup.find_all("a", href=re.compile(r"buscar/act\.php\?id=", re.I)):
        act_url = BASE_BOE + a["href"] if a["href"].startswith("/") else a["href"]
        time.sleep(DELAY)
        resp2 = _get(session, act_url)
        if not resp2:
            continue
        m = _BOE_PDF_RE.search(resp2.text)
        if m:
            yyyy, mm, dd, fname = m.groups()
            return f"{BASE_BOE}/boe/dias/{yyyy}/{mm}/{dd}/pdfs/{fname}"

    return None


# ── Main ───────────────────────────────────────────────────────────────────────

def resolve_pjcat_pdfs() -> None:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")

    if not os.path.exists(CATALOG_PATH):
        print(f"ERROR: {CATALOG_PATH} not found")
        sys.exit(1)

    with open(CATALOG_PATH, encoding="utf-8") as fh:
        catalog = json.load(fh)

    session = _make_session()
    os.makedirs(PDF_DIR, exist_ok=True)

    pending = [e for e in catalog if not e.get("pdf_descarregat")]
    print(f"Entrades pendents: {len(pending)}")

    boe_ok   = 0
    dogc_ok  = 0
    no_resolt = 0

    for i, entry in enumerate(pending, 1):
        entry_id  = entry.get("id", f"pjcat-{i}")
        codi      = entry.get("codi", "")
        eli_path  = (entry.get("eli") or "").strip()
        print(f"  [{i}/{len(pending)}] {entry_id} ({codi})  eli={eli_path}")

        pdf_url   = None
        is_boe    = False

        # Determina el tipus d'ELI
        if eli_path.startswith("/eli/es/"):
            is_boe  = True
            pdf_url = _resolve_boe_eli(eli_path, session)
            if pdf_url:
                print(f"    → BOE ELI OK: {pdf_url[-60:]}")
            else:
                print(f"    → BOE ELI fallat, intent per codi...")
                pdf_url = _resolve_by_codi_boe(codi, session)
                if pdf_url:
                    print(f"    → Codi BOE OK: {pdf_url[-60:]}")

        elif eli_path.startswith("/eli/es-ct/"):
            pdf_url = _resolve_pjcat_eli(eli_path, session)
            if pdf_url:
                print(f"    → PJCAT/DOGC OK: {pdf_url[-60:]}")
            else:
                print(f"    → PJCAT fallat, intent per codi al BOE...")
                pdf_url = _resolve_by_codi_boe(codi, session)
                if pdf_url:
                    print(f"    → Codi BOE fallback OK: {pdf_url[-60:]}")

        elif eli_path:
            # ELI desconeguda, intenta directament
            time.sleep(DELAY)
            resp = _get(session, eli_path if eli_path.startswith("http") else "https:" + eli_path)
            if resp:
                for href in _pdf_links_from_html(resp.text):
                    if ".pdf" in href.lower():
                        pdf_url = href if href.startswith("http") else None
                        if pdf_url:
                            break

        if not pdf_url:
            # Darrer recurs: cerca per codi al BOE
            if codi and not pdf_url:
                pdf_url = _resolve_by_codi_boe(codi, session)
                if pdf_url:
                    print(f"    → Última oportunitat BOE OK: {pdf_url[-60:]}")

        if not pdf_url:
            print(f"    → NO PDF RESOLT")
            no_resolt += 1
            continue

        entry["url_pdf"] = pdf_url

        # Descarrega
        safe_id = re.sub(r"[^\w\-.]", "_", entry_id)
        dest = os.path.join(PDF_DIR, safe_id + ".pdf")
        if os.path.exists(dest) and os.path.getsize(dest) > 1000:
            entry["path_local"]      = dest.replace("\\", "/")
            entry["pdf_descarregat"] = True
            print(f"    → Ja existia localment")
            if is_boe:
                boe_ok += 1
            else:
                dogc_ok += 1
            continue

        time.sleep(DELAY)
        ok = _download_pdf(pdf_url, dest, session)
        if ok:
            entry["path_local"]      = dest.replace("\\", "/")
            entry["pdf_descarregat"] = True
            print(f"    → Descarregat OK")
            if is_boe:
                boe_ok += 1
            else:
                dogc_ok += 1
        else:
            entry["pdf_descarregat"] = False
            print(f"    → Error en descàrrega: {pdf_url}")
            no_resolt += 1

    # Desa catàleg actualitzat
    with open(CATALOG_PATH, "w", encoding="utf-8") as fh:
        json.dump(catalog, fh, ensure_ascii=False, indent=2)

    print()
    print(f"BOE resolts: {boe_ok} | DOGC resolts: {dogc_ok} | No resolts: {no_resolt}")


if __name__ == "__main__":
    resolve_pjcat_pdfs()
