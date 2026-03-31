"""
adif_scraper.py
Descàrrega i sincronització incremental de la Normativa Tècnica ADIF via API interna.
https://normativatecnica.adif.es/ntw

Flux:
  1. GET body-busqueda.jsp  → cookies de sessió + CSRF token
  2. POST getDocumentos      → metadades de tots els documents
  3. Sync incremental        → detecta nous i canvis d'estat (Vigente→Derogado)
  4. GET descargarDocumento  → URL real del PDF (sols per nous/canviats)
  5. GET <pdf_url>           → descarrega el fitxer

Sincronització incremental:
  - Clau única: object_id.
  - Detecta canvis de camp "estado" (ex. Vigente → Derogado).
  - Mai esborra fitxers del disc.
  - Guarda un log de sincronització datat a _catalogo/sync_YYYYMMDD.json.
"""

import csv
import json
import os
import re
import sys
import time
import unicodedata
from datetime import datetime

import requests
from bs4 import BeautifulSoup

# ─── Constants ────────────────────────────────────────────────────────────────
BASE_URL              = "https://normativatecnica.adif.es/ntw"
OUTPUT_DIR            = "normativa_adif"
DELAY                 = 1.5
SESSION_REFRESH_EVERY = 200  # refresh CSRF token every N documents


# ─── Helpers ─────────────────────────────────────────────────────────────────

def sanitize_folder(text: str) -> str:
    """Safe folder name: strip leading number, remove accents, replace spaces."""
    text = re.sub(r"^\d+\.\s*", "", text.strip())
    text = unicodedata.normalize("NFKD", text)
    text = "".join(c for c in text if not unicodedata.combining(c))
    text = re.sub(r"[^\w\s\-]", "", text)
    text = re.sub(r"\s+", "_", text.strip())
    return text[:60]


def sanitize_filename(codigo: str, titulo: str) -> str:
    """Generate a safe filename from code + title."""
    base = f"{codigo}_{titulo}" if codigo else titulo
    base = re.sub(r'[\\/:*?"<>|]', "_", base)
    base = re.sub(r"\s+", "_", base.strip())
    base = base[:100]
    if not base.lower().endswith(".pdf"):
        base += ".pdf"
    return base


def parse_ubicacion(ubicacion: str) -> tuple:
    """
    Parse the hierarchical location into (category, subcategory) folder names.
    Example: "/DN_NTE/01. NORMATIVA TÉCNICA Y BASE.../01. NORMATIVA VIGENTE/..."
    Skips the first part (DN_NTE), takes next two as cat / subcat.
    """
    parts = [p.strip() for p in ubicacion.strip("/").split("/") if p.strip()]
    cat    = sanitize_folder(parts[1]) if len(parts) > 1 else "GENERAL"
    subcat = sanitize_folder(parts[2]) if len(parts) > 2 else ""
    return cat, subcat


# ─── Catalog persistence ──────────────────────────────────────────────────────

def load_existing_adif_catalog(output_dir: str) -> dict:
    """
    Load existing ADIF catalog keyed by object_id.
    Returns {} if no catalog exists yet.
    Uses catalogo_adif_complet.json (the full run output) when available,
    falling back to catalogo_adif.json (raw metadata only).
    """
    catalog_dir = os.path.join(output_dir, "_catalogo")
    for filename in ("catalogo_adif_complet.json", "catalogo_adif.json"):
        path = os.path.join(catalog_dir, filename)
        if os.path.exists(path):
            try:
                with open(path, encoding="utf-8") as f:
                    items = json.load(f)
                keyed = {str(item["object_id"]): item for item in items if "object_id" in item}
                print(f"  Catàleg carregat: {len(keyed)} entrades ({filename})")
                return keyed
            except Exception as exc:
                print(f"  [!] No s'ha pogut carregar {filename}: {exc}")
    return {}


def _save_adif_sync_log(changes: dict, output_dir: str) -> str:
    """Save a dated sync log and return its path."""
    catalog_dir = os.path.join(output_dir, "_catalogo")
    os.makedirs(catalog_dir, exist_ok=True)

    today    = datetime.now().strftime("%Y%m%d")
    log_path = os.path.join(catalog_dir, f"sync_{today}.json")

    log = {
        "data":         datetime.now().isoformat()[:10],
        "nous":         len(changes["nous"]),
        "actualitzats": len(changes["actualitzats"]),
        "sense_canvis": len(changes["sense_canvis"]),
        "detall_canvis": {
            "nous": [
                f"{d.get('codigo','')} — {d.get('titulo','')}"
                for d in changes["nous"]
            ],
            "actualitzats": [
                {
                    "codigo": d.get("codigo", ""),
                    "titol":  d.get("titulo", ""),
                    "antic":  d.get("estado_anterior", ""),
                    "nou":    d.get("estado", ""),
                }
                for d in changes["actualitzats"]
            ],
        },
    }

    with open(log_path, "w", encoding="utf-8") as f:
        json.dump(log, f, ensure_ascii=False, indent=2)

    return log_path


# ─── Session / CSRF ───────────────────────────────────────────────────────────

def get_session_and_csrf() -> tuple:
    """
    Create a new session, load the search page, extract CSRF credentials.
    Returns: (session, csrf_token, csrf_header_name)
    """
    session = requests.Session()
    session.headers.update({
        "User-Agent":      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Accept":          "text/html,application/xhtml+xml,*/*;q=0.8",
        "Accept-Language": "es-ES,es;q=0.9",
    })

    csrf_token  = None
    csrf_header = "X-CSRF-TOKEN"

    try:
        resp = session.get(
            f"{BASE_URL}/views/busqueda/body-busqueda.jsp",
            timeout=30,
            allow_redirects=True,
        )
        resp.raise_for_status()
        html = resp.text
        soup = BeautifulSoup(html, "html.parser")

        # Pattern 1: <meta name="_csrf" content="...">
        meta = soup.find("meta", {"name": "_csrf"})
        if meta:
            csrf_token = meta.get("content")
        meta_h = soup.find("meta", {"name": "_csrf_header"})
        if meta_h:
            csrf_header = meta_h.get("content", csrf_header)

        # Pattern 2: JS variable  var tokenCsrf = "...";
        if not csrf_token:
            m = re.search(r'var\s+tokenCsrf\s*=\s*["\']([^"\']+)["\']', html)
            if m:
                csrf_token = m.group(1)
        m2 = re.search(r'var\s+headerCsrf\s*=\s*["\']([^"\']+)["\']', html)
        if m2:
            csrf_header = m2.group(1)

        # Pattern 3: cookie fallback
        if not csrf_token:
            csrf_token = (
                session.cookies.get("XSRF-TOKEN")
                or session.cookies.get("csrf")
                or session.cookies.get("CSRF-TOKEN")
            )

    except Exception as exc:
        print(f"  ✗ Error carregant pàgina de cerca: {exc}")

    token_preview = (csrf_token[:20] + "...") if csrf_token else "NO TROBAT"
    print(f"  CSRF header : {csrf_header}")
    print(f"  CSRF token  : {token_preview}")

    return session, csrf_token, csrf_header


# ─── API calls ────────────────────────────────────────────────────────────────

def get_all_documents(session, csrf_token: str, csrf_header: str) -> list:
    """
    POST to getDocumentos and return list of document dicts.
    Falls back to empty payload if the estado filter returns nothing.
    """
    headers = {
        "Content-Type":     "application/json",
        csrf_header:        csrf_token,
        "X-Requested-With": "XMLHttpRequest",
        "Referer":          f"{BASE_URL}/views/busqueda/body-busqueda.jsp",
    }

    for payload in (
        json.dumps([{"name": "estado", "value": "T"}]),
        json.dumps([]),
    ):
        try:
            resp = session.post(
                f"{BASE_URL}/action/BusquedaController/getDocumentos",
                data=payload,
                headers=headers,
                timeout=60,
            )
            resp.raise_for_status()
            data = resp.json()
        except Exception as exc:
            print(f"  ✗ Error cridant getDocumentos: {exc}")
            return []

        if data.get("error"):
            print(f"  ✗ Error API: {data['error']}")
            continue

        rows = data.get("data", [])
        if rows:
            documents = [
                {
                    "object_id": row[0],
                    "codigo":    row[1],
                    "titulo":    row[2],
                    "edicion":   row[3],
                    "fecha":     row[4],
                    "estado":    row[5],   # "Vigente" / "Histórico" / "Derogado"
                    "ubicacion": row[6],   # hierarchical path
                }
                for row in rows
            ]
            print(f"  Total documents API: {len(documents)}")
            return documents

        print("  [!] Sense resultats amb payload anterior, reintentant sense filtre…")

    return []


def get_annex_list(session, csrf_token: str, csrf_header: str, object_id) -> list:
    """
    GET getDocumentosAnexos?idDocPadre=<object_id>
    Returns list of raw row arrays: [annex_id, filename, content_type, ...]
    """
    headers = {
        csrf_header:        csrf_token,
        "X-Requested-With": "XMLHttpRequest",
        "Referer":          f"{BASE_URL}/views/busqueda/body-busqueda.jsp",
    }
    try:
        resp = session.get(
            f"{BASE_URL}/action/DocumentacionController/getDocumentosAnexos",
            params={"idDocPadre": object_id},
            headers=headers,
            timeout=30,
        )
        data = resp.json()
        return data.get("data", [])
    except Exception as exc:
        print(f"    [AX] error: {exc}")
        return []


def get_annex_download_url(session, csrf_token: str, csrf_header: str, annex_id) -> str | None:
    """GET descargarDocumento?identificador=<annex_id> -> PDF URL string."""
    try:
        resp = session.get(
            f"{BASE_URL}/action/DocumentacionController/descargarDocumento",
            params={"identificador": annex_id},
            headers={
                csrf_header:        csrf_token,
                "X-Requested-With": "XMLHttpRequest",
                "Referer":          f"{BASE_URL}/views/busqueda/body-busqueda.jsp",
            },
            timeout=30,
        )
        text = resp.text.strip()
        if not text:
            return None
        data = resp.json()
        return data.get("resultado")
    except Exception:
        return None


def download_pdf(session, url: str, dest_path: str) -> bool | None:
    """
    Download a PDF.
    Returns True  → downloaded now
            False → already existed (skipped)
            None  → error
    """
    if os.path.exists(dest_path):
        return False

    try:
        resp = session.get(url, timeout=60, stream=True, allow_redirects=True)
        resp.raise_for_status()
        content = resp.content

        if not content.startswith(b"%PDF"):
            return None

        os.makedirs(os.path.dirname(dest_path), exist_ok=True)
        with open(dest_path, "wb") as f:
            f.write(content)
        return True

    except Exception as exc:
        print(f"    ✗ Error descarregant: {exc}")
        return None


# ─── Main orchestration ───────────────────────────────────────────────────────

def scrape_all(output_dir: str) -> None:
    """
    Incremental sync for all ADIF NTE documents.
    Downloads only new documents and those whose estado has changed.
    """
    today = datetime.now().isoformat()[:10]

    print("Connectant amb ADIF Normativa Tècnica…")
    session, csrf_token, csrf_header = get_session_and_csrf()

    if not csrf_token:
        print("ERROR: No s'ha pogut obtenir el CSRF token. Atura.")
        return

    # ── Load existing catalog ─────────────────────────────────────────────────
    print("\nCarregant catàleg existent…")
    existing = load_existing_adif_catalog(output_dir)
    if not existing:
        print("  (primer ús — catàleg buit)")

    # ── Fetch current API state ───────────────────────────────────────────────
    print("\nObtenen llista de documents de l'API…")
    api_docs = get_all_documents(session, csrf_token, csrf_header)

    if not api_docs:
        print("ERROR: No s'han obtingut documents.")
        return

    # Save raw metadata immediately (for recovery)
    catalog_dir = os.path.join(output_dir, "_catalogo")
    os.makedirs(catalog_dir, exist_ok=True)
    raw_path = os.path.join(catalog_dir, "catalogo_adif.json")
    with open(raw_path, "w", encoding="utf-8") as f:
        json.dump(api_docs, f, ensure_ascii=False, indent=2)
    print(f"  Metadades raw guardades: {raw_path}")

    # ── Sync ──────────────────────────────────────────────────────────────────
    changes: dict[str, list] = {"nous": [], "actualitzats": [], "sense_canvis": []}

    for doc in api_docs:
        oid = str(doc["object_id"])
        if oid in existing:
            old_estat = existing[oid].get("estado", "")
            new_estat = doc["estado"]
            if old_estat != new_estat:
                merged = {**existing[oid], **doc}
                merged["estado_anterior"] = old_estat
                merged["data_canvi"]      = today
                changes["actualitzats"].append(merged)
                print(f"  ⚠ CANVI ESTAT: {doc['codigo']} — {old_estat} → {new_estat}")
            else:
                changes["sense_canvis"].append(existing[oid])
        else:
            doc["data_afegit"] = today
            changes["nous"].append(doc)

    print(f"\n  Nous: {len(changes['nous'])}  "
          f"Canviats: {len(changes['actualitzats'])}  "
          f"Sense canvis: {len(changes['sense_canvis'])}")

    # ── Download new and changed documents ────────────────────────────────────
    to_download = changes["nous"] + changes["actualitzats"]
    # Also queue unchanged entries that have no PDF yet
    missing_pdf = [
        d for d in changes["sense_canvis"]
        if not d.get("fitxers") and not d.get("te_pdf")
    ]
    if missing_pdf:
        print(f"  {len(missing_pdf)} entrades sense PDF: s'afegeixen a la cua")
        to_download = to_download + missing_pdf
    print(f"\nDescàrrega de {len(to_download)} documents nous/actualitzats…")

    downloaded_count = skipped_count = sense_pdf_count = errors_count = 0

    for i, doc in enumerate(to_download, 1):
        # Periodic CSRF refresh
        if i % SESSION_REFRESH_EVERY == 0:
            print(f"\n  [Refrescant sessió CSRF...]")
            session, csrf_token, csrf_header = get_session_and_csrf()
            time.sleep(2)

        code_str  = doc.get("codigo") or "???"
        title_str = (doc.get("titulo") or "(sense títol)")[:45]
        print(f"  [{i:4}/{len(to_download)}] {code_str} — {title_str}", end=" ")

        # Skip if all files already on disk
        existing_fitxers = doc.get("fitxers", [])
        if existing_fitxers and all(
            f.get("fitxer_local") and os.path.exists(f["fitxer_local"])
            for f in existing_fitxers
        ):
            print("ja existeix")
            skipped_count += 1
            doc["te_pdf"]         = True
            doc["estat_descarga"] = "omès"
            continue

        # Step 1: get annex list
        annexes = get_annex_list(session, csrf_token, csrf_header, doc["object_id"])

        if not annexes:
            doc["te_pdf"]         = False
            doc["fitxers"]        = []
            doc["estat_descarga"] = "sense_pdf"
            sense_pdf_count += 1
            print("sense annexos")
            time.sleep(0.5)
            continue


        doc["te_pdf"]  = True
        doc["fitxers"] = []

        # Build destination folder
        cat, subcat     = parse_ubicacion(doc.get("ubicacion", ""))
        estat_folder    = "vigent" if "vigent" in doc["estado"].lower() else "derogat"
        dest_folder     = os.path.join(output_dir, estat_folder, cat, subcat)
        os.makedirs(dest_folder, exist_ok=True)

        for annex in annexes:
            # annex row: [annex_id, filename, content_type, ...]
            annex_id   = annex[0] if annex else ""
            annex_name = annex[1] if len(annex) > 1 else f"{doc['codigo']}.pdf"

            if not annex_id:
                continue

            # Step 2: get real download URL
            pdf_url = get_annex_download_url(session, csrf_token, csrf_header, annex_id)

            if not pdf_url:
                print(f"annex {annex_name}: sense URL ", end="")
                continue

            # Step 3: download
            safe_name = re.sub(r'[\\/:*?"<>|]', '_', annex_name)
            if not safe_name.lower().endswith(".pdf"):
                safe_name += ".pdf"
            dest_path = os.path.join(dest_folder, safe_name)

            result = download_pdf(session, pdf_url, dest_path)
            doc["fitxers"].append({
                "annex_id":    annex_id,
                "nom":         annex_name,
                "fitxer_local": dest_path,
                "descarregat": result is True,
            })

            if result is True:
                size_kb = os.path.getsize(dest_path) / 1024
                print(f"↓ {safe_name} ({size_kb:.0f}KB) ", end="")
                downloaded_count += 1
            elif result is False:
                skipped_count += 1

            time.sleep(0.5)

        doc["estat_descarga"] = "descarregat" if downloaded_count else "omès"
        print()
        time.sleep(DELAY)

    # ── Build final catalog (merge all buckets) ───────────────────────────────
    # Start from existing entries not in to_download, then overlay with updated data
    to_download_ids = {str(d["object_id"]) for d in to_download}
    final_catalog: list[dict] = []

    # Add all entries; to_download overrides sense_canvis for same object_id
    for entry in changes["sense_canvis"]:
        if str(entry["object_id"]) not in to_download_ids:
            if "te_pdf" not in entry:
                entry["te_pdf"] = bool(entry.get("fitxer_local"))
            final_catalog.append(entry)

    # Add new, changed, and missing-pdf entries (now enriched with fitxers / estat_descarga)
    for entry in to_download:
        final_catalog.append(entry)

    # ── Save results catalog ──────────────────────────────────────────────────
    results_json = os.path.join(catalog_dir, "catalogo_adif_complet.json")
    with open(results_json, "w", encoding="utf-8") as f:
        json.dump(final_catalog, f, ensure_ascii=False, indent=2)

    if final_catalog:
        fieldnames = list(dict.fromkeys(k for e in final_catalog for k in e))
        results_csv = os.path.join(catalog_dir, "catalogo_adif_complet.csv")
        with open(results_csv, "w", encoding="utf-8-sig", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
            writer.writeheader()
            writer.writerows(final_catalog)

    # ── Save sync log ─────────────────────────────────────────────────────────
    log_path = _save_adif_sync_log(changes, output_dir)

    # ── Summary ───────────────────────────────────────────────────────────────
    print(f"\n{'-' * 55}")
    print(f"RESUM SINCRONITZACIO ADIF {today}:")
    print(f"  Documents nous:          {len(changes['nous']):4}")
    print(f"  Estat actualitzat:       {len(changes['actualitzats']):4}")
    for d in changes["actualitzats"]:
        print(f"    {d.get('codigo','')} {d.get('estado_anterior','')} -> {d.get('estado','')}")
    print(f"  Sense canvis:            {len(changes['sense_canvis']):4}")
    print(f"  Fitxers descarregats:    {downloaded_count:4}")
    print(f"  Sense annexos (antics):  {sense_pdf_count:4}")
    print(f"  Ja existien:             {skipped_count:4}")
    print(f"  Errors reals:            {errors_count:4}")
    print(f"  Log guardat: {log_path}")
    print(f"{'-' * 55}")


# ─── Entry point ──────────────────────────────────────────────────────────────
if __name__ == "__main__":
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    out = sys.argv[1] if len(sys.argv) > 1 else OUTPUT_DIR
    print("=" * 55)
    print(" Scraper Normativa Tecnica ADIF (sincronitzacio incremental)")
    print(f" Desti: {out}/")
    print("=" * 55)
    scrape_all(out)
