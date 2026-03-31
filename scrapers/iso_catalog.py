"""
iso_catalog.py — Descarrega i emmagatzema el CSV Open Data d'ISO,
i el normalitza a catalogs/iso/catalogo_iso.json.

Ús:
    python iso_catalog.py

Dependències: requests + stdlib.
"""

from __future__ import annotations

import csv
import json
import os
import sys
from datetime import datetime
from pathlib import Path

import requests

from config import PROJECT_ROOT, CATALOGS_DIR

# ─── Constants ────────────────────────────────────────────────────────────────

ISO_CATALOG_DIR  = CATALOGS_DIR / "iso"
ISO_CATALOG_PATH = ISO_CATALOG_DIR / "catalogo_iso.json"
ISO_CACHE_PATH   = ISO_CATALOG_DIR / "iso_raw.csv"
CACHE_MAX_DAYS   = 30

CSV_URL = (
    "https://isopublicstorageprod.blob.core.windows.net"
    "/opendata/_latest/iso_deliverables_metadata/csv"
    "/iso_deliverables_metadata.csv"
)


# ─── Helpers ──────────────────────────────────────────────────────────────────

def _get(row: dict, *keys: str) -> str:
    """Retorna el primer valor no buit d'un dict per una llista de claus candidates."""
    for k in keys:
        v = row.get(k) or row.get(k.lower()) or row.get(k.upper())
        if v and str(v).strip():
            return str(v).strip()
    return ""


# ─── Pas 1: descarregar CSV ─────────────────────────────────────────────────

def download_csv(cache_path: Path) -> Path:
    """Descarrega el fitxer CSV open-data d'ISO, usant cache local si és recent."""
    cache_path.parent.mkdir(parents=True, exist_ok=True)

    if cache_path.exists():
        age_days = (datetime.now().timestamp() - cache_path.stat().st_mtime) / 86400
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
        print(f"  Descarregat: {cache_path.stat().st_size / 1_048_576:.1f} MB")
    except Exception as exc:
        print(f"  Error descarregant CSV: {exc}")
        raise

    return cache_path


# ─── Pas 2: parsejar CSV ────────────────────────────────────────────────────

def parse_csv(csv_path: Path) -> list[dict]:
    """
    Parseja el CSV de deliverables ISO i retorna una llista normalitzada de dicts.

    Els noms exactes de columnes poden variar entre versions; provem múltiples
    noms candidats per cada camp lògic.
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


# ─── Pas 3: desar catàleg ─────────────────────────────────────────────────────

def save_catalog(catalog: list[dict], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(catalog, f, ensure_ascii=False, indent=2)


# ─── Main ─────────────────────────────────────────────────────────────────────

def main(catalog_dir: Path = None) -> None:
    if catalog_dir is None:
        catalog_dir = ISO_CATALOG_DIR

    catalog_path = catalog_dir / "catalogo_iso.json"
    cache_path   = catalog_dir / "iso_raw.csv"

    print("=== Constructor del catàleg ISO ===")
    csv_path = download_csv(cache_path)
    catalog  = parse_csv(csv_path)

    vigents    = sum(1 for d in catalog if d["estat"] == "VIGENT")
    retirades  = sum(1 for d in catalog if d["estat"] == "RETIRADA")
    altres     = len(catalog) - vigents - retirades

    save_catalog(catalog, catalog_path)

    print(f"\n  Total normes ISO:       {len(catalog):,}")
    print(f"  Vigents:                {vigents:,}")
    print(f"  Retirades:              {retirades:,}")
    print(f"  Altres (elaboració…):   {altres:,}")
    print(f"  Catàleg guardat:        {catalog_path}")


if __name__ == "__main__":
    main()
