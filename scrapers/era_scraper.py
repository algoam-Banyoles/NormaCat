"""
normativa_era/era_scraper.py
Catàleg d'ETIs (Especificaciones Técnicas de Interoperabilidad) de l'ERA
i normes CENELEC ferroviàries.

Estratègia:
  1. Intent de scraping de la pàgina ERA (sol requerir JS → normalment falla).
  2. Fallback: catàleg estàtic hardcoded.

Ús:
    python normativa_era/era_scraper.py
"""

import json
import os
import sys
from datetime import datetime

import requests
from bs4 import BeautifulSoup

BASE_URL   = "https://www.era.europa.eu"
TARGET_URL = (
    BASE_URL
    + "/domains/technical-standards-ims"
    + "/technical-specifications-interoperability_en"
)
OUTPUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "_catalogo")
HEADERS    = {"User-Agent": "Mozilla/5.0 (compatible; ProjectChecker/1.0)"}
TIMEOUT    = 30

# ─── Static fallback catalog ──────────────────────────────────────────────────
_STATIC_ETIS = [
    {"codi": "ETI-INF-HS",  "titol": "ETI Infraestructura - Alta Velocitat",
     "reglament": "EU 1299/2014", "any": 2014},
    {"codi": "ETI-INF-CC",  "titol": "ETI Infraestructura - Xarxa Convencional",
     "reglament": "EU 1299/2014", "any": 2014},
    {"codi": "ETI-ENE",     "titol": "ETI Energia",
     "reglament": "EU 1301/2014", "any": 2014},
    {"codi": "ETI-CCS",     "titol": "ETI Control-Comandament i Senyalització",
     "reglament": "EU 2016/919",  "any": 2016},
    {"codi": "ETI-WAG",     "titol": "ETI Material Rodant - Vagons",
     "reglament": "EU 321/2013",  "any": 2013},
    {"codi": "ETI-LOC-PAS", "titol": "ETI Material Rodant - Locomotores i Tren Passatgers",
     "reglament": "EU 1302/2014", "any": 2014},
    {"codi": "ETI-NOI",     "titol": "ETI Persones amb Mobilitat Reduïda",
     "reglament": "EU 1300/2014", "any": 2014},
    {"codi": "ETI-OPE",     "titol": "ETI Explotació i Gestió del Trànsit",
     "reglament": "EU 2015/995",  "any": 2015},
    {"codi": "ETI-TAF",     "titol": "ETI Aplicacions Telemàtiques Mercaderies",
     "reglament": "EU 1305/2014", "any": 2014},
    {"codi": "ETI-TAP",     "titol": "ETI Aplicacions Telemàtiques Passatgers",
     "reglament": "EU 454/2011",  "any": 2011},
    {"codi": "ETI-SRT",     "titol": "ETI Seguretat en Túnels Ferroviaris",
     "reglament": "EU 1303/2014", "any": 2014},
    {"codi": "ETI-RAM-STR", "titol": "EN 50126 RAMS - Especificació i Demostració",
     "reglament": "CENELEC EN 50126-1", "any": 2017},
    {"codi": "ETI-SW-SEC",  "titol": "EN 50128 Software per a sistemes ferroviaris",
     "reglament": "CENELEC EN 50128",   "any": 2011},
    {"codi": "ETI-SEC-SIS", "titol": "EN 50129 Aprovació seguretat sistemes electrònics",
     "reglament": "CENELEC EN 50129",   "any": 2018},
]


def _build_doc(raw: dict) -> dict:
    regl = raw.get("reglament", "")
    return {
        "codi":           raw["codi"],
        "titol":          raw["titol"],
        "reglament":      regl,
        "any_publicacio": raw.get("any"),
        "estat":          "VIGENT",
        "font":           "ERA/CENELEC",
        "url_fitxa":      TARGET_URL,
        "url_document":   None,
        "temes":          ["ferroviari"],
        "observacions":   f"Reglament: {regl}" if regl else "",
    }


def _try_scrape() -> list[dict]:
    """Attempt live scraping. Returns [] if page requires JS or fails."""
    try:
        resp = requests.get(TARGET_URL, headers=HEADERS, timeout=TIMEOUT)
        resp.raise_for_status()
    except Exception as exc:
        print(f"  [INFO] ERA live scrape failed: {exc}", flush=True)
        return []

    soup = BeautifulSoup(resp.text, "html.parser")
    docs = []

    # Look for ETI patterns in links or list items
    for a in soup.find_all("a", href=True):
        text = a.get_text(strip=True)
        href = a["href"]
        if "interoperabilit" in text.lower() or "TSI" in text or "ETI" in text:
            docs.append({
                "codi":   f"ETI-LIVE-{len(docs)+1:03d}",
                "titol":  text,
                "reglament": "",
                "any":    None,
            })

    if len(docs) < 3:
        print("  [INFO] Live scrape returned minimal results — using static catalog",
              flush=True)
        return []

    print(f"  [INFO] Live scrape OK: {len(docs)} links found", flush=True)
    return docs


def build_catalog() -> list[dict]:
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    print("  Attempting live scrape of ERA...", flush=True)
    live = _try_scrape()
    raw_list = live if live else _STATIC_ETIS
    source = "live" if live else "static fallback"
    print(f"  Source: {source} ({len(raw_list)} entries)", flush=True)

    docs = [_build_doc(r) for r in raw_list]

    catalog = {
        "metadata": {
            "font":            "European Union Agency for Railways (ERA) / CENELEC",
            "url_base":        BASE_URL,
            "data_scraping":   datetime.now().strftime("%Y-%m-%d"),
            "total_documents": len(docs),
            "versio":          "1.0",
            "source":          source,
        },
        "documents": docs,
    }

    out = os.path.join(OUTPUT_DIR, "catalogo_era.json")
    with open(out, "w", encoding="utf-8") as fh:
        json.dump(catalog, fh, ensure_ascii=False, indent=2)

    return docs


if __name__ == "__main__":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    print("=" * 50)
    print(" ERA Scraper — ETIs ferroviàries europees")
    print("=" * 50)

    docs = build_catalog()

    print()
    print("[OK] ERA catalog complete")
    print(f"[DOC] ETIs catalogades: {len(docs)}")
    print(f"[OK] Saved to {os.path.join(OUTPUT_DIR, 'catalogo_era.json')}")
