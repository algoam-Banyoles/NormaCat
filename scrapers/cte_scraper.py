"""
cte_scraper.py — Catàleg estàtic del Codi Tècnic de l'Edificació (CTE)
Font: https://www.codigotecnico.org

Estratègia: catàleg estàtic de URLs conegudes + verificació HEAD.
El lloc web de codigotecnico.org renderitza amb JS, per tant no és escrapable
directament; les URLs dels PDFs segueixen un patró deduïble.

Ús:
    python cte_scraper.py [output_dir]   (default: normativa_cte)
"""

import json
import os
import sys
from datetime import datetime
from pathlib import Path

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from config import PROJECT_ROOT, CATALOGS_DIR

# ─── Constants ────────────────────────────────────────────────────────────────
CTE_CATALOG_DIR   = CATALOGS_DIR / "cte"
CTE_DOWNLOADS_DIR = PROJECT_ROOT / "downloads" / "cte"
BASE_URL          = "https://www.codigotecnico.org"
PDF_BASE     = BASE_URL + "/pdf/Documentos"

# ─── Document catalog (static, verified pattern) ──────────────────────────────
# RD 314/2006, modificat per RD 732/2019 (publicat al BOE 27/12/2019)
# Versions vigents: actualització 2019 per la majoria; HE actualitzat per
# Ordre TED/285/2022 (publicada BOE 25/03/2022).

DOCUMENTS = [
    {
        "codi":              "CTE-PARTE-I",
        "titol":             "CTE Parte I — Disposiciones generales",
        "familia":           "CTE",
        "grup":              "GENERAL",
        "estat":             "VIGENT",
        "versio":            "2019",
        "reial_decret":      "RD 314/2006 mod. RD 732/2019",
        "url_pdf":           PDF_BASE + "/CTE/CTE_2019.pdf",
        "url_pdf_alt":       PDF_BASE + "/CTE/CTEI.pdf",
        "data_actualitzacio": "2019-12-26",
        "observacions":      "Marc normatiu general. Exigències bàsiques, requisits i condicions d'ús",
    },
    {
        "codi":              "CTE-DB-SE",
        "titol":             "DB-SE Seguridad Estructural",
        "familia":           "CTE",
        "grup":              "SE",
        "estat":             "VIGENT",
        "versio":            "2019",
        "reial_decret":      "RD 314/2006 mod. RD 732/2019",
        "url_pdf":           PDF_BASE + "/SE/DBSE.pdf",
        "url_pdf_alt":       None,
        "data_actualitzacio": "2019-12-26",
        "observacions":      "Inclou DB-SE-AE, DB-SE-C, DB-SE-A, DB-SE-F, DB-SE-M com a documents complementaris",
    },
    {
        "codi":              "CTE-DB-SE-AE",
        "titol":             "DB-SE-AE Seguridad Estructural — Acciones en la Edificación",
        "familia":           "CTE",
        "grup":              "SE",
        "estat":             "VIGENT",
        "versio":            "2019",
        "reial_decret":      "RD 314/2006 mod. RD 732/2019",
        "url_pdf":           PDF_BASE + "/SE/DBSE-AE.pdf",
        "url_pdf_alt":       None,
        "data_actualitzacio": "2019-12-26",
        "observacions":      "Carregues gravitatòries, vent, neu, sísmiques",
    },
    {
        "codi":              "CTE-DB-SE-C",
        "titol":             "DB-SE-C Seguridad Estructural — Cimientos",
        "familia":           "CTE",
        "grup":              "SE",
        "estat":             "VIGENT",
        "versio":            "2019",
        "reial_decret":      "RD 314/2006 mod. RD 732/2019",
        "url_pdf":           PDF_BASE + "/SE/DBSE-C.pdf",
        "url_pdf_alt":       None,
        "data_actualitzacio": "2019-12-26",
        "observacions":      "",
    },
    {
        "codi":              "CTE-DB-SE-A",
        "titol":             "DB-SE-A Seguridad Estructural — Acero",
        "familia":           "CTE",
        "grup":              "SE",
        "estat":             "VIGENT",
        "versio":            "2019",
        "reial_decret":      "RD 314/2006 mod. RD 732/2019",
        "url_pdf":           PDF_BASE + "/SE/DBSE-A.pdf",
        "url_pdf_alt":       None,
        "data_actualitzacio": "2019-12-26",
        "observacions":      "",
    },
    {
        "codi":              "CTE-DB-SE-F",
        "titol":             "DB-SE-F Seguridad Estructural — Fábrica",
        "familia":           "CTE",
        "grup":              "SE",
        "estat":             "VIGENT",
        "versio":            "2019",
        "reial_decret":      "RD 314/2006 mod. RD 732/2019",
        "url_pdf":           PDF_BASE + "/SE/DBSE-F.pdf",
        "url_pdf_alt":       None,
        "data_actualitzacio": "2019-12-26",
        "observacions":      "",
    },
    {
        "codi":              "CTE-DB-SE-M",
        "titol":             "DB-SE-M Seguridad Estructural — Madera",
        "familia":           "CTE",
        "grup":              "SE",
        "estat":             "VIGENT",
        "versio":            "2019",
        "reial_decret":      "RD 314/2006 mod. RD 732/2019",
        "url_pdf":           PDF_BASE + "/SE/DBSE-M.pdf",
        "url_pdf_alt":       None,
        "data_actualitzacio": "2019-12-26",
        "observacions":      "",
    },
    {
        "codi":              "CTE-DB-SI",
        "titol":             "DB-SI Seguridad en caso de Incendio",
        "familia":           "CTE",
        "grup":              "SI",
        "estat":             "VIGENT",
        "versio":            "2019",
        "reial_decret":      "RD 314/2006 mod. RD 732/2019",
        "url_pdf":           PDF_BASE + "/SI/DBSI.pdf",
        "url_pdf_alt":       None,
        "data_actualitzacio": "2019-12-26",
        "observacions":      "Aplicable a edificis. Complementat per RIPCI (RD 513/2017) per instal·lacions",
    },
    {
        "codi":              "CTE-DB-SUA",
        "titol":             "DB-SUA Seguridad de Utilización y Accesibilidad",
        "familia":           "CTE",
        "grup":              "SUA",
        "estat":             "VIGENT",
        "versio":            "2019",
        "reial_decret":      "RD 314/2006 mod. RD 732/2019",
        "url_pdf":           PDF_BASE + "/SUA/DBSUA.pdf",
        "url_pdf_alt":       None,
        "data_actualitzacio": "2019-12-26",
        "observacions":      "Substitueix el DB-SU i DB-SUA anteriors",
    },
    {
        "codi":              "CTE-DB-HE",
        "titol":             "DB-HE Ahorro de Energía",
        "familia":           "CTE",
        "grup":              "HE",
        "estat":             "VIGENT",
        "versio":            "2022",
        "reial_decret":      "RD 314/2006 mod. Ordre TED/285/2022",
        "url_pdf":           PDF_BASE + "/HE/DBHE.pdf",
        "url_pdf_alt":       None,
        "data_actualitzacio": "2022-03-25",
        "observacions":      "Versió actualitzada 2022 (Ordre TED/285/2022). Requisits d'eficiencia energètica",
    },
    {
        "codi":              "CTE-DB-HS",
        "titol":             "DB-HS Salubridad",
        "familia":           "CTE",
        "grup":              "HS",
        "estat":             "VIGENT",
        "versio":            "2019",
        "reial_decret":      "RD 314/2006 mod. RD 732/2019",
        "url_pdf":           PDF_BASE + "/HS/DBHS.pdf",
        "url_pdf_alt":       None,
        "data_actualitzacio": "2019-12-26",
        "observacions":      "",
    },
    {
        "codi":              "CTE-DB-HR",
        "titol":             "DB-HR Protección frente al Ruido",
        "familia":           "CTE",
        "grup":              "HR",
        "estat":             "VIGENT",
        "versio":            "2019",
        "reial_decret":      "RD 314/2006 mod. RD 732/2019",
        "url_pdf":           PDF_BASE + "/HR/DBHR.pdf",
        "url_pdf_alt":       None,
        "data_actualitzacio": "2019-12-26",
        "observacions":      "",
    },
]

# ─── Session ──────────────────────────────────────────────────────────────────

def _make_session() -> requests.Session:
    session = requests.Session()
    retry = Retry(total=2, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
    session.mount("https://", HTTPAdapter(max_retries=retry))
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Accept": "*/*",
    })
    return session


# ─── URL verification ─────────────────────────────────────────────────────────

def _verify_url(session: requests.Session, url: str) -> bool:
    """HEAD request to check if URL exists. Returns True if 200 or 301/302."""
    try:
        resp = session.head(url, timeout=15, allow_redirects=True)
        return resp.status_code == 200
    except Exception:
        return False


def verify_documents(session: requests.Session, docs: list) -> list:
    """
    Verify each document's url_pdf with a HEAD request.
    If primary fails, tries url_pdf_alt.
    Sets 'url_pdf_verified' and 'url_ok' on each entry.
    """
    print("\nVerificant URLs...")
    ok = 0
    fail = 0
    for doc in docs:
        primary = doc["url_pdf"]
        alt     = doc.get("url_pdf_alt")

        if _verify_url(session, primary):
            doc["url_pdf_verified"] = primary
            doc["url_ok"] = True
            print(f"  [OK]  {doc['codi']}")
            ok += 1
        elif alt and _verify_url(session, alt):
            doc["url_pdf_verified"] = alt
            doc["url_ok"] = True
            print(f"  [ALT] {doc['codi']} — usant URL alternativa")
            ok += 1
        else:
            doc["url_pdf_verified"] = primary
            doc["url_ok"] = False
            print(f"  [FAIL]{doc['codi']} — {primary}")
            fail += 1

    print(f"  URLs OK: {ok}  |  Fallides: {fail}")
    return docs


# ─── Descàrrega de PDFs ──────────────────────────────────────────────────────

def _download_pdf(session: requests.Session, url: str, dest_path: Path) -> bool:
    """Descarrega un fitxer PDF. Retorna True si s'ha descarregat correctament."""
    if dest_path.exists():
        return True   # ja descarregat
    dest_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        resp = session.get(url, timeout=60)
        resp.raise_for_status()
        if len(resp.content) < 100:
            print(f"  [WARN] Fitxer massa petit: {dest_path.name}")
            return False
        with open(dest_path, "wb") as f:
            f.write(resp.content)
        size_kb = len(resp.content) // 1024
        print(f"  ↓ {dest_path.name} ({size_kb} KB)")
        return True
    except Exception as exc:
        print(f"  [ERROR] Descàrrega fallida {dest_path.name}: {exc}")
        return False


# ─── Catalog builder ──────────────────────────────────────────────────────────

def build_catalog(catalog_dir=None, downloads_dir=None) -> list:
    """Build and save the CTE catalog. Returns list of document entries."""
    if catalog_dir is None:
        catalog_dir = CTE_CATALOG_DIR
    if downloads_dir is None:
        downloads_dir = CTE_DOWNLOADS_DIR
    catalog_dir = Path(catalog_dir)
    downloads_dir = Path(downloads_dir)
    catalog_dir.mkdir(parents=True, exist_ok=True)

    session = _make_session()

    docs = [dict(d) for d in DOCUMENTS]   # shallow copy

    docs = verify_documents(session, docs)

    # Remove internal helper fields before saving
    catalog_docs = []
    for d in docs:
        entry = {
            "codi":              d["codi"],
            "titol":             d["titol"],
            "familia":           d["familia"],
            "grup":              d["grup"],
            "estat":             d["estat"],
            "versio":            d["versio"],
            "reial_decret":      d["reial_decret"],
            "url_pdf":           d.get("url_pdf_verified") or d["url_pdf"],
            "url_ok":            d.get("url_ok", False),
            "data_actualitzacio": d["data_actualitzacio"],
            "observacions":      d["observacions"],
        }
        catalog_docs.append(entry)

    # ── Descàrrega de PDFs ──────────────────────────────────────────────────
    downloaded = 0
    skipped = 0
    errors = 0
    print(f"\nDescarregant PDFs a {downloads_dir} ...")
    for doc in catalog_docs:
        url_pdf = doc.get("url_pdf", "")
        if not url_pdf or not doc.get("url_ok", False):
            continue

        filename = doc["codi"] + ".pdf"
        dest_path = downloads_dir / filename

        if dest_path.exists():
            doc["fitxer_local"] = str(dest_path)
            skipped += 1
            continue

        ok = _download_pdf(session, url_pdf, dest_path)
        if ok:
            doc["fitxer_local"] = str(dest_path)
            downloaded += 1
        else:
            errors += 1

    print(f"  PDFs descarregats: {downloaded}  |  Ja existents: {skipped}  |  Errors: {errors}")

    catalog = {
        "_meta": {
            "font":            "Código Técnico de la Edificación — codigotecnico.org",
            "data_extraccio":  datetime.now().isoformat()[:10],
            "total":           len(catalog_docs),
            "urls_ok":         sum(1 for d in catalog_docs if d["url_ok"]),
            "urls_fail":       sum(1 for d in catalog_docs if not d["url_ok"]),
            "pdfs_descarregats": downloaded + skipped,
            "versio_scraper":  "1.1",
        },
        "documents": catalog_docs,
    }

    out_path = catalog_dir / "catalogo_cte.json"
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(catalog, f, ensure_ascii=False, indent=2)

    return catalog_docs, catalog["_meta"], out_path


# ─── Entry point ──────────────────────────────────────────────────────────────

if __name__ == "__main__":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

    print("=" * 55)
    print(" CTE Scraper — Codi Tècnic de l'Edificació")
    print(f" Destí: {CTE_CATALOG_DIR / 'catalogo_cte.json'}")
    print("=" * 55)

    docs, meta, out_path = build_catalog()

    print(f"\n{'=' * 55}")
    print(f"  Total documents:   {meta['total']}")
    print(f"  URLs verificades:  {meta['urls_ok']}")
    print(f"  URLs fallides:     {meta['urls_fail']}")
    print(f"  Catàleg guardat:   {out_path}")
    print(f"{'=' * 55}")

    if meta["urls_fail"] > 0:
        print("\n  Documents amb URL no verificada:")
        for d in docs:
            if not d["url_ok"]:
                print(f"    - {d['codi']}: {d['url_pdf']}")
