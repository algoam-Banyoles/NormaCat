"""
iso_catalog.py — Downloads and caches the ISO Open Data deliverables CSV,
then normalises it into normativa_iso/_catalogo/catalogo_iso.json.

Usage:
    python iso_catalog.py [output_dir]   (default: normativa_iso)

Dependencies: requests + stdlib only.
"""

from __future__ import annotations

import csv
import json
import os
import sys
from datetime import datetime

import requests

# ─── Constants ────────────────────────────────────────────────────────────────

OUTPUT_DIR   = "normativa_iso"
CATALOG_PATH = os.path.join(OUTPUT_DIR, "_catalogo", "catalogo_iso.json")
CSV_CACHE    = os.path.join(OUTPUT_DIR, "_catalogo", "iso_raw.csv")
CACHE_MAX_DAYS = 30

CSV_URL = (
    "https://isopublicstorageprod.blob.core.windows.net"
    "/opendata/_latest/iso_deliverables_metadata/csv"
    "/iso_deliverables_metadata.csv"
)


# ─── Helpers ──────────────────────────────────────────────────────────────────

def _get(row: dict, *keys: str) -> str:
    """Return first non-empty value from a dict for a list of candidate keys."""
    for k in keys:
        v = row.get(k) or row.get(k.lower()) or row.get(k.upper())
        if v and str(v).strip():
            return str(v).strip()
    return ""


# ─── Step 1: download CSV ─────────────────────────────────────────────────────

def download_csv(cache_path: str = CSV_CACHE) -> str:
    """Download the ISO open-data CSV file, using a local cache if fresh."""
    os.makedirs(os.path.dirname(cache_path), exist_ok=True)

    if os.path.exists(cache_path):
        age_days = (datetime.now().timestamp() - os.path.getmtime(cache_path)) / 86400
        if age_days < CACHE_MAX_DAYS:
            print(f"  Usant cache local ({age_days:.0f} dies d'antiguitat)")
            return cache_path
        print(f"  Cache expirada ({age_days:.0f} dies) — recarregant…")

    print("  Descarregant ISO Open Data CSV…")
    try:
        resp = requests.get(CSV_URL, timeout=120, stream=True)
        resp.raise_for_status()
        with open(cache_path, "wb") as f:
            for chunk in resp.iter_content(65_536):
                f.write(chunk)
        print(f"  Descarregat: {os.path.getsize(cache_path) / 1_048_576:.1f} MB")
    except Exception as exc:
        print(f"  ✗ Error descarregant CSV: {exc}")
        raise

    return cache_path


# ─── Step 2: parse CSV ────────────────────────────────────────────────────────

def parse_csv(csv_path: str) -> list[dict]:
    """
    Parse the ISO deliverables CSV and return a normalised list of dicts.

    The exact column names may vary between releases; we try multiple
    candidate names for each logical field.
    """
    catalog: list[dict] = []

    with open(csv_path, encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        cols = reader.fieldnames or []
        print(f"  Columnes detectades ({len(cols)}): {cols[:10]}{'…' if len(cols)>10 else ''}")

        for row in reader:
            ref = _get(row,
                       "reference", "deliverable_ref", "Reference",
                       "iso_reference", "DeliverableRef")
            if not ref:
                continue

            status_raw = _get(row,
                              "status", "Status", "deliverable_status",
                              "DeliverableStatus")
            sl = status_raw.lower()
            if "withdrawn" in sl or "retirad" in sl:
                estat = "RETIRADA"
            elif "published" in sl or "vigent" in sl:
                estat = "VIGENT"
            elif "development" in sl or "preparation" in sl:
                estat = "EN_ELABORACIO"
            else:
                estat = status_raw.upper() if status_raw else "DESCONEGUT"

            catalog.append({
                "referencia":       ref,
                "titol":            _get(row, "title_en", "title", "Title",
                                         "deliverable_title", "DeliverableTitle"),
                "estat":            estat,
                "estat_original":   status_raw,
                "data_publicacio":  _get(row, "publication_date", "PublicationDate",
                                         "pub_date", "publicationDate"),
                "edicio":           _get(row, "edition", "Edition"),
                "ics":              _get(row, "ics_codes", "ics", "ICS",
                                         "ics_code", "ICSCodes"),
                "tc":               _get(row, "tc_id", "tc", "committee",
                                         "TC", "TechnicalCommittee"),
                "substituida_per":  _get(row, "replaced_by", "replacedBy",
                                         "replaced_by_ref", "ReplacedBy"),
                "font":             "ISO Open Data",
            })

    return catalog


# ─── Step 3: save catalog ─────────────────────────────────────────────────────

def save_catalog(catalog: list[dict], path: str = CATALOG_PATH) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(catalog, f, ensure_ascii=False, indent=2)


# ─── Main ─────────────────────────────────────────────────────────────────────

def main(output_dir: str = OUTPUT_DIR) -> None:
    global CATALOG_PATH, CSV_CACHE
    if output_dir != OUTPUT_DIR:
        CATALOG_PATH = os.path.join(output_dir, "_catalogo", "catalogo_iso.json")
        CSV_CACHE    = os.path.join(output_dir, "_catalogo", "iso_raw.csv")

    print("═══ ISO Catalog Builder ═══")
    csv_path = download_csv(CSV_CACHE)
    catalog  = parse_csv(csv_path)

    vigents    = sum(1 for d in catalog if d["estat"] == "VIGENT")
    retirades  = sum(1 for d in catalog if d["estat"] == "RETIRADA")
    altres     = len(catalog) - vigents - retirades

    save_catalog(catalog, CATALOG_PATH)

    print(f"\n  Total normes ISO:       {len(catalog):,}")
    print(f"  Vigents:                {vigents:,}")
    print(f"  Retirades:              {retirades:,}")
    print(f"  Altres (elaboració…):   {altres:,}")
    print(f"  Catàleg guardat:        {CATALOG_PATH}")


if __name__ == "__main__":
    out = sys.argv[1] if len(sys.argv) > 1 else OUTPUT_DIR
    main(out)
