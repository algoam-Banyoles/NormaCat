"""
rebt_rite_scraper.py — Scraper especialitzat per a normativa electrica
i d'instal-lacions d'edificis (REBT, RITE, RAT, RIPCI, autoconsum).

Usa l'API OpenData del BOE per obtenir metadades i PDFs consolidats.
Actualitza normativa_annexes.json amb entrades noves i correccions.

Usage:
    python scrapers/rebt_rite_scraper.py
"""
from __future__ import annotations

import io
import json
import os
import shutil
import sys
import time
from datetime import datetime

# UTF-8 on Windows
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
else:
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

# Import from boe_scraper
_SCRAPERS_DIR = os.path.dirname(os.path.abspath(__file__))
if _SCRAPERS_DIR not in sys.path:
    sys.path.insert(0, _SCRAPERS_DIR)

try:
    from boe_scraper import (
        make_session, fetch_by_id,
    )
except ImportError:
    print("[ERROR] No s'ha pogut importar boe_scraper.py. Assegura't que existeix a scrapers/.")
    sys.exit(1)


# ─── Constants ────────────────────────────────────────────────────────────────

PROJECT_ROOT = os.path.dirname(_SCRAPERS_DIR)
OUTPUT_DIR = os.path.join(PROJECT_ROOT, "normativa_rebt_rite")
DELAY = 1.5

# ─── Registry: BOE documents ─────────────────────────────────────────────────

REGISTRY = {
    # REBT
    "BOE-A-2002-18099": {
        "codi": "REBT-RD-842/2002",
        "text": "Reial Decret 842/2002, de 2 d'agost, Reglament Electrotecnic "
                "de Baixa Tensio (REBT). Modificat per RD 560/2010, RD 1053/2014, "
                "RD 542/2020.",
        "domain": "REBT",
        "estat_esperat": "VIGENT",
        "derogada_per": "",
        "observacions": "Inclou 52 ITCs (BT-01 a BT-52). ITC-BT-52 afegida "
                        "per RD 542/2020 (vehicle electric).",
        "annex_ids": ["A09", "A08", "A12"],
        "itcs": [
            "ITC-BT-04", "ITC-BT-10", "ITC-BT-11", "ITC-BT-18",
            "ITC-BT-19", "ITC-BT-24", "ITC-BT-28", "ITC-BT-29",
            "ITC-BT-36", "ITC-BT-37", "ITC-BT-38", "ITC-BT-40",
            "ITC-BT-44", "ITC-BT-52",
        ],
    },
    "BOE-A-2020-17216": {
        "codi": "RD-542/2020",
        "text": "Reial Decret 542/2020, de 26 de juny, modifica el REBT "
                "(RD 842/2002). Afegeix ITC-BT-52 (vehicle electric), "
                "modifica ITC-BT-10, BT-25, BT-43, BT-44.",
        "domain": "REBT",
        "estat_esperat": "VIGENT",
        "derogada_per": "",
        "observacions": "Modificacio important del REBT. Vehicle electric.",
        "annex_ids": ["A09"],
    },
    "BOE-A-2005-13043": {
        "codi": "RD-1454/2005",
        "text": "Reial Decret 1454/2005, de 2 de desembre, modifica el REBT "
                "(RD 842/2002). Modifica ITCs BT-04, BT-16, BT-19.",
        "domain": "REBT",
        "estat_esperat": "VIGENT",
        "derogada_per": "",
        "observacions": "Modificacio puntual del REBT.",
        "annex_ids": ["A09"],
    },

    # RITE
    "BOE-A-2007-15820": {
        "codi": "RITE-RD-1027/2007",
        "text": "Reial Decret 1027/2007, de 20 de juliol, Reglament "
                "d'Instal-lacions Termiques als Edificis (RITE). "
                "Modificat per RD 238/2013 i RD 178/2021.",
        "domain": "RITE",
        "estat_esperat": "VIGENT",
        "derogada_per": "",
        "observacions": "El RITE base segueix vigent. La versio consolidada "
                        "inclou les modificacions del RD 178/2021 (en vigor "
                        "des del 01/07/2021).",
        "annex_ids": ["A09"],
    },
    "BOE-A-2021-4572": {
        "codi": "RD-178/2021",
        "text": "Reial Decret 178/2021, de 23 de marc, pel qual es modifica "
                "el RD 1027/2007 (RITE). Fase I d'actualitzacio.",
        "domain": "RITE",
        "estat_esperat": "VIGENT",
        "derogada_per": "",
        "observacions": "Modificacio important del RITE: autoconsum, "
                        "digitalitzacio, eficiencia energetica. "
                        "En vigor des del 01/07/2021.",
        "annex_ids": ["A09"],
    },

    # RAT
    "BOE-A-2014-5638": {
        "codi": "RAT-RD-337/2014",
        "text": "Reial Decret 337/2014, de 9 de maig, Reglament sobre "
                "condicions tecniques i garanties de seguretat en "
                "instal-lacions electriques d'alta tensio (RAT).",
        "domain": "RAT",
        "estat_esperat": "VIGENT",
        "derogada_per": "",
        "observacions": "Inclou ITCs LAT-01 a LAT-09, ITC-RAT-01 a RAT-23.",
        "annex_ids": ["A08", "A09"],
        "itcs": [
            "ITC-LAT-01", "ITC-LAT-02", "ITC-LAT-06", "ITC-LAT-07",
            "ITC-RAT-01", "ITC-RAT-02", "ITC-RAT-06", "ITC-RAT-09",
            "ITC-RAT-14", "ITC-RAT-15", "ITC-RAT-20",
        ],
    },

    # RIPCI
    "BOE-A-2017-6606": {
        "codi": "RIPCI-RD-513/2017",
        "text": "Reial Decret 513/2017, de 22 de maig, Reglament "
                "d'instal-lacions de proteccio contra incendis (RIPCI).",
        "domain": "RIPCI",
        "estat_esperat": "VIGENT",
        "derogada_per": "",
        "observacions": "Deroga RD 1942/1993.",
        "annex_ids": ["A10"],
    },
    "BOE-A-1993-26117": {
        "codi": "RD-1942/1993",
        "text": "Reial Decret 1942/1993, de 5 de novembre, Reglament "
                "d'instal-lacions de proteccio contra incendis (antic).",
        "domain": "RIPCI",
        "estat_esperat": "DEROGADA",
        "derogada_per": "RD 513/2017 (BOE-A-2017-6606)",
        "observacions": "DEROGAT per RD 513/2017.",
        "annex_ids": ["A10"],
    },

    # Autoconsum i distribucio
    "BOE-A-2019-5089": {
        "codi": "RD-244/2019",
        "text": "Reial Decret 244/2019, de 5 d'abril, condicions "
                "administratives, tecniques i economiques de l'autoconsum "
                "d'energia electrica.",
        "domain": "AUTOCONSUM",
        "estat_esperat": "VIGENT",
        "derogada_per": "",
        "observacions": "Regulacio de l'autoconsum.",
        "annex_ids": ["A09", "A12"],
    },
    "BOE-A-2000-24019": {
        "codi": "RD-1955/2000",
        "text": "Reial Decret 1955/2000, d'1 de desembre, activitats de "
                "transport, distribucio, comercialitzacio, subministrament.",
        "domain": "DISTRIBUCIO",
        "estat_esperat": "VIGENT",
        "derogada_per": "",
        "observacions": "Procediment d'escomesa i connexio a xarxa.",
        "annex_ids": ["A12"],
    },
    "BOE-A-2020-14725": {
        "codi": "RD-1183/2020",
        "text": "Reial Decret 1183/2020, de 29 de desembre, acces i "
                "connexio a les xarxes de transport i distribucio.",
        "domain": "DISTRIBUCIO",
        "estat_esperat": "VIGENT",
        "derogada_per": "",
        "observacions": "Permisos d'acces i connexio.",
        "annex_ids": ["A12"],
    },

    # Derogades importants
    "BOE-A-1973-1651": {
        "codi": "RD-2413/1973",
        "text": "Reial Decret 2413/1973, Reglament Electrotecnic de "
                "Baixa Tensio (antic REBT).",
        "domain": "REBT",
        "estat_esperat": "DEROGADA",
        "derogada_per": "RD 842/2002 (BOE-A-2002-18099)",
        "observacions": "Completament derogat. Si apareix -> NO OK greu.",
        "annex_ids": [],
    },
}

# ─── UNE norms (metadata only, no BOE) ───────────────────────────────────────

UNE_ELECTRIC = [
    {"codi": "UNE-20460", "text": "UNE 20460 Instal-lacions electriques en edificis",
     "domain": "REBT", "estat_esperat": "VIGENT", "observacions": "Serie basica."},
    {"codi": "UNE-EN-61439", "text": "UNE-EN 61439 Conjunts d'aparellatge de BT (quadres)",
     "domain": "REBT", "estat_esperat": "VIGENT", "observacions": "Substitueix UNE-EN 60439."},
    {"codi": "UNE-EN-60898", "text": "UNE-EN 60898 Interruptors automatics domestics (magnetotermics)",
     "domain": "REBT", "estat_esperat": "VIGENT", "observacions": "Ref: ITC-BT-22."},
    {"codi": "UNE-HD-60364", "text": "UNE-HD 60364 Instal-lacions electriques de BT (harmonitzada)",
     "domain": "REBT", "estat_esperat": "VIGENT", "observacions": "Serie harmonitzada europea."},
    {"codi": "UNE-EN-62305", "text": "UNE-EN 62305 Proteccio contra el llamp",
     "domain": "LLAMP", "estat_esperat": "VIGENT", "observacions": "4 parts. Complementa CTE DB-SUA-8."},
    {"codi": "UNE-EN-50160", "text": "UNE-EN 50160 Caracteristiques de la tensio subministrada",
     "domain": "DISTRIBUCIO", "estat_esperat": "VIGENT", "observacions": "Qualitat subministrament."},
    {"codi": "UNE-EN-12464-1", "text": "UNE-EN 12464-1 Il-luminacio llocs de treball interiors",
     "domain": "ENLLUMENAT", "estat_esperat": "VIGENT", "observacions": "Nivells lux per espai."},
    {"codi": "UNE-EN-1838", "text": "UNE-EN 1838 Enllumenat d'emergencia",
     "domain": "ENLLUMENAT", "estat_esperat": "VIGENT", "observacions": "Min 1 lux eix evacuacio."},
    {"codi": "UNE-EN-50575", "text": "UNE-EN 50575 Cables d'energia — comportament al foc",
     "domain": "CABLES", "estat_esperat": "VIGENT", "observacions": "Euroclasses per cables."},
    {"codi": "UNE-EN-60529", "text": "UNE-EN 60529 Graus de proteccio (codi IP)",
     "domain": "GENERAL", "estat_esperat": "VIGENT", "observacions": "IP d'envolupants."},
    {"codi": "UNE-100166", "text": "UNE 100166 Climatitzacio. Ventilacio d'aparcaments.",
     "domain": "RITE", "estat_esperat": "VIGENT", "observacions": "Ventilacio aparcaments soterrats."},
]


# ─── PDF download ─────────────────────────────────────────────────────────────

def _download_pdf(session, url, dest_path):
    """Download PDF if not already on disk."""
    if os.path.exists(dest_path):
        return True
    try:
        r = session.get(url, timeout=60, stream=True)
        if r.status_code != 200:
            return False
        ct = r.headers.get("Content-Type", "")
        if "pdf" not in ct.lower() and "octet" not in ct.lower():
            return False
        os.makedirs(os.path.dirname(dest_path), exist_ok=True)
        with open(dest_path, "wb") as f:
            for chunk in r.iter_content(8192):
                f.write(chunk)
        return True
    except Exception as e:
        print(f"    [WARN] Download failed: {e}")
        return False


# ─── Merge into normativa_annexes.json ────────────────────────────────────────

def merge_into_annexes(catalog, annexes_path):
    """Update normativa_annexes.json with electrical norms."""
    if not os.path.exists(annexes_path):
        print(f"  [INFO] {annexes_path} no trobat, omitint merge")
        return

    backup = annexes_path + ".bak"
    shutil.copy2(annexes_path, backup)

    with open(annexes_path, encoding="utf-8") as f:
        annexes = json.load(f)

    # Add to normativa_derogada
    existing_derogada = annexes.get("normativa_derogada", [])
    existing_codis = {e.get("codi", "") for e in existing_derogada}

    added_derogada = 0
    for entry in catalog:
        if entry.get("estat") != "DEROGADA":
            continue
        codi = entry.get("codi", "") or entry.get("id", "")
        if codi in existing_codis:
            continue
        existing_derogada.append({
            "codi": codi,
            "text": entry.get("text", "")[:200],
            "derogada_per": entry.get("derogada_per", ""),
            "observacions": f"Font: REBT/RITE scraper. {entry.get('observacions', '')}".strip(),
        })
        existing_codis.add(codi)
        added_derogada += 1
        print(f"    + DEROGADA: {codi}")

    annexes["normativa_derogada"] = existing_derogada

    # Fix RITE: update existing entries with modification info
    _fix_rite_rebt(annexes)

    with open(annexes_path, "w", encoding="utf-8") as f:
        json.dump(annexes, f, ensure_ascii=False, indent=2)

    print(f"  normativa_annexes.json actualitzat: +{added_derogada} derogades (backup: {backup})")


def _fix_rite_rebt(annexes):
    """Fix RITE and REBT entries in normativa_annexes.json."""
    # Search all lists in annexes for entries to fix
    for key, entries in annexes.items():
        if not isinstance(entries, list):
            continue
        for entry in entries:
            if not isinstance(entry, dict):
                continue
            text = entry.get("text", "")
            codi = entry.get("codi", "")
            obs = entry.get("observacions", "")

            # Fix RITE: add RD 178/2021 mention if missing
            if ("1027/2007" in text or "1027/2007" in codi) and "178/2021" not in text:
                if "178/2021" not in obs:
                    entry["observacions"] = (
                        (obs + " " if obs else "")
                        + "Versio consolidada vigent inclou modificacions "
                        "del RD 178/2021 (en vigor des del 01/07/2021)."
                    ).strip()
                    print(f"    ~ RITE fix: afegit RD 178/2021 a {codi or text[:40]}")

            # Fix REBT: add RD 542/2020 mention if missing
            if ("842/2002" in text or "842/2002" in codi) and "542/2020" not in text:
                if "542/2020" not in obs:
                    entry["observacions"] = (
                        (obs + " " if obs else "")
                        + "Modificat per RD 560/2010, RD 1053/2014, "
                        "RD 542/2020 (ITC-BT-52 vehicle electric)."
                    ).strip()
                    print(f"    ~ REBT fix: afegit modificacions a {codi or text[:40]}")


# ─── Save catalog ─────────────────────────────────────────────────────────────

def save_catalog(catalog, output_dir):
    """Save the catalog JSON."""
    cat_dir = os.path.join(output_dir, "_catalogo")
    os.makedirs(cat_dir, exist_ok=True)

    cat_path = os.path.join(cat_dir, "catalogo_rebt_rite.json")
    with open(cat_path, "w", encoding="utf-8") as f:
        json.dump(catalog, f, ensure_ascii=False, indent=2)
    print(f"  Cataleg desat: {cat_path} ({len(catalog)} entrades)")

    # Sync log
    sync_path = os.path.join(cat_dir, f"sync_{datetime.now().strftime('%Y%m%d')}.json")
    with open(sync_path, "w", encoding="utf-8") as f:
        json.dump({
            "data": datetime.now().isoformat(timespec="seconds"),
            "entrades_boe": sum(1 for e in catalog if e.get("font") == "BOE OpenData API"),
            "entrades_une": sum(1 for e in catalog if e.get("font") == "Registre intern"),
            "total": len(catalog),
        }, f, ensure_ascii=False, indent=2)


# ─── Main ─────────────────────────────────────────────────────────────────────

def main(output_dir=OUTPUT_DIR):
    print("=== REBT/RITE/RAT Catalog Builder ===")
    print(f"  Registre: {len(REGISTRY)} documents BOE + {len(UNE_ELECTRIC)} UNE")

    session = make_session()
    catalog = []
    n_pdfs = 0

    # Phase 1: Fetch BOE metadata
    print(f"\n--- Fase 1: Metadades BOE ({len(REGISTRY)} documents) ---")
    for idx, (boe_id, reg) in enumerate(REGISTRY.items(), 1):
        print(f"  [{idx}/{len(REGISTRY)}] {boe_id}", end="")
        try:
            entry = fetch_by_id(session, boe_id, "electric")
        except Exception as exc:
            print(f" -> ERROR: {exc}")
            entry = None

        if entry:
            # Enrich with registry data
            entry["domain"] = reg["domain"]
            entry["codi_local"] = reg["codi"]
            entry["estat_esperat"] = reg["estat_esperat"]
            entry["itcs"] = reg.get("itcs", [])
            entry["annex_ids"] = reg.get("annex_ids", [])
            if reg.get("observacions"):
                entry["observacions"] = reg["observacions"]
            if reg.get("derogada_per"):
                entry["derogada_per"] = reg["derogada_per"]

            # Verify estat
            api_estat = entry.get("estat", "PENDENT")
            if reg["estat_esperat"] == "DEROGADA" and api_estat != "DEROGADA":
                print(f" AVIS: esperat DEROGADA, API diu {api_estat}")
            elif reg["estat_esperat"] == "VIGENT" and api_estat == "DEROGADA":
                print(f" AVIS: esperat VIGENT, API diu DEROGADA!")

            # Force expected estat (registry is authoritative)
            entry["estat"] = reg["estat_esperat"]

            catalog.append(entry)

            # Download PDF
            pdf_url = entry.get("url_pdf", "")
            if pdf_url:
                domain_dir = os.path.join(output_dir, reg["domain"])
                pdf_name = f"{boe_id}.pdf"
                pdf_path = os.path.join(domain_dir, pdf_name)
                if _download_pdf(session, pdf_url, pdf_path):
                    n_pdfs += 1
                    entry["pdf_local"] = pdf_path

            title = entry.get("text", "")[:55]
            print(f" -> {entry['estat']}: {title}")
        else:
            print(f" -> NO TROBAT (API no ha retornat dades)")
            # Add from registry anyway
            catalog.append({
                "id": boe_id,
                "codi": reg["codi"],
                "text": reg["text"],
                "domain": reg["domain"],
                "estat": reg["estat_esperat"],
                "derogada_per": reg.get("derogada_per", ""),
                "observacions": reg.get("observacions", ""),
                "font": "Registre intern (API no disponible)",
                "annex_ids": reg.get("annex_ids", []),
                "itcs": reg.get("itcs", []),
            })

        time.sleep(DELAY)

    # Phase 2: Add UNE entries
    print(f"\n--- Fase 2: UNE ({len(UNE_ELECTRIC)} normes) ---")
    for une in UNE_ELECTRIC:
        catalog.append({
            "id": une["codi"],
            "codi": une["codi"],
            "text": une["text"],
            "domain": une["domain"],
            "estat": une["estat_esperat"],
            "observacions": une.get("observacions", ""),
            "font": "Registre intern",
        })
    print(f"  Afegides {len(UNE_ELECTRIC)} normes UNE al cataleg")

    # Phase 3: Save catalog
    print(f"\n--- Fase 3: Desar cataleg ---")
    save_catalog(catalog, output_dir)

    # Phase 4: Merge into normativa_annexes.json
    print(f"\n--- Fase 4: Actualitzar normativa_annexes.json ---")
    annexes_path = os.path.join(PROJECT_ROOT, "normativa_annexes.json")
    if not os.path.exists(annexes_path):
        annexes_path = os.path.join(PROJECT_ROOT, "data", "normativa_annexes.json")
    merge_into_annexes(catalog, annexes_path)

    # Summary
    domains = {}
    for e in catalog:
        d = e.get("domain", "?")
        if d not in domains:
            domains[d] = {"vigent": 0, "derogada": 0}
        if e.get("estat") == "DEROGADA":
            domains[d]["derogada"] += 1
        else:
            domains[d]["vigent"] += 1

    print(f"\n{'='*50}")
    print(f"  RESUM")
    print(f"{'='*50}")
    print(f"  {'Domini':<15} {'Vigent':>7} {'Derogat':>8}")
    print(f"  {'-'*15} {'-'*7} {'-'*8}")
    for d in sorted(domains):
        v = domains[d]["vigent"]
        der = domains[d]["derogada"]
        print(f"  {d:<15} {v:>7} {der:>8}")
    print(f"  {'-'*32}")
    print(f"  {'TOTAL':<15} {sum(v['vigent'] for v in domains.values()):>7} "
          f"{sum(v['derogada'] for v in domains.values()):>8}")
    print(f"  PDFs descarregats: {n_pdfs}")
    print(f"  Cataleg: {len(catalog)} entrades")
    print(f"{'='*50}")


if __name__ == "__main__":
    main()
