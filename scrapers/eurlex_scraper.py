"""
eurlex_scraper.py — Directives i Reglaments europeus des d'EUR-Lex.

Descarrega legislació UE aplicable a projectes d'infraestructures
de mobilitat a Catalunya.

Estratègia:
  1. Catàleg base de normativa UE essencial (llista curada).
  2. Cerca complementària via EUR-Lex search per ampliar cobertura.
  3. Descàrrega de PDFs (tots lliures a EUR-Lex).

URLs predictibles EUR-Lex:
  Fitxa:  https://eur-lex.europa.eu/legal-content/ES/ALL/?uri=CELEX:{celex}
  PDF ES: https://eur-lex.europa.eu/legal-content/ES/TXT/PDF/?uri=CELEX:{celex}
  PDF CA: no disponible (EUR-Lex no té versió catalana)

Ús:
    python scrapers/eurlex_scraper.py
"""

import json
import os
import re
import sys
import time
from datetime import datetime
from pathlib import Path

import requests
from bs4 import BeautifulSoup

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import PROJECT_ROOT, CATALOGS_DIR

# ── Constants ──────────────────────────────────────────────────────────────────

EURLEX_CATALOG_DIR = CATALOGS_DIR / "eurlex"
EURLEX_DOWNLOADS_DIR = PROJECT_ROOT / "downloads" / "eurlex"

BASE_URL = "https://eur-lex.europa.eu"
PDF_URL_TEMPLATE = BASE_URL + "/legal-content/ES/TXT/PDF/?uri=CELEX:{celex}"
FITXA_URL_TEMPLATE = BASE_URL + "/legal-content/ES/ALL/?uri=CELEX:{celex}"
SEARCH_URL = BASE_URL + "/search.html"

DELAY = 2.0  # segons entre peticions
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) NormaCat/1.0",
    "Accept-Language": "es-ES,es;q=0.9,ca;q=0.8",
}

# ── Catàleg base: legislació UE essencial per infraestructures ─────────────────
#
# Cada entrada: (celex, codi_curt, titol, temes[], observacions)
#
# Format CELEX:
#   Directives:  3YYYYLNNNN (L = legislació)
#   Reglaments:  3YYYYRNNNN (R = reglament)
#
# NOTA: Aquesta llista cobreix la normativa UE MÉS RELLEVANT per a
# projectes d'infraestructures de mobilitat a Catalunya (carreteres,
# ferrocarril, metro, tramvia, bus, bici, aparcaments).
# No és exhaustiva — la cerca complementària amplia la cobertura.

_CORE_LEGISLATION = [
    # ── PRODUCTES DE CONSTRUCCIÓ ──
    ("32011R0305", "Reg-305/2011",
     "Reglament (UE) 305/2011 — Condicions harmonitzades per a la comercialització de productes de construcció (CPR)",
     ["construcció", "productes", "marcatge_CE"],
     "Reglament de Productes de Construcció. Obligatori marcatge CE."),

    # ── CONTRACTACIÓ PÚBLICA ──
    ("32014L0024", "Dir-2014/24/UE",
     "Directiva 2014/24/UE — Contractació pública",
     ["contractes", "licitació"],
     "Transposada per Llei 9/2017 (LCSP)."),
    ("32014L0025", "Dir-2014/25/UE",
     "Directiva 2014/25/UE — Contractació sectors especials (aigua, energia, transports)",
     ["contractes", "sectors_especials", "transport"],
     "Sectors especials incloent transport."),
    ("32014L0023", "Dir-2014/23/UE",
     "Directiva 2014/23/UE — Adjudicació de contractes de concessió",
     ["contractes", "concessions"],
     "Concessions d'obres i serveis."),

    # ── MEDI AMBIENT ──
    ("32011L0092", "Dir-2011/92/UE",
     "Directiva 2011/92/UE — Avaluació d'impacte ambiental (AIA) de projectes",
     ["medi_ambient", "AIA"],
     "Codificació. Modificada per Dir 2014/52/UE."),
    ("32014L0052", "Dir-2014/52/UE",
     "Directiva 2014/52/UE — Modificació de la Directiva AIA 2011/92/UE",
     ["medi_ambient", "AIA"],
     "Modifica la Dir 2011/92/UE."),
    ("32001L0042", "Dir-2001/42/CE",
     "Directiva 2001/42/CE — Avaluació ambiental estratègica (AAE) de plans i programes",
     ["medi_ambient", "AAE"],
     "Avaluació ambiental estratègica."),
    ("32000L0060", "Dir-2000/60/CE",
     "Directiva 2000/60/CE — Marc comunitari d'actuació en política d'aigües (DMA)",
     ["medi_ambient", "aigües"],
     "Directiva Marc de l'Aigua."),
    ("32008L0098", "Dir-2008/98/CE",
     "Directiva 2008/98/CE — Marc de residus",
     ["medi_ambient", "residus"],
     "Jerarquia de residus. Transposada per Llei 7/2022."),
    ("32002L0049", "Dir-2002/49/CE",
     "Directiva 2002/49/CE — Avaluació i gestió del soroll ambiental",
     ["medi_ambient", "soroll"],
     "Mapes estratègics de soroll."),
    ("32009L0147", "Dir-2009/147/CE",
     "Directiva 2009/147/CE — Conservació d'aus silvestres",
     ["medi_ambient", "biodiversitat"],
     "Directiva d'Aus. Codificació de Dir 79/409/CEE."),
    ("31992L0043", "Dir-92/43/CEE",
     "Directiva 92/43/CEE — Conservació d'hàbitats naturals (Natura 2000)",
     ["medi_ambient", "biodiversitat"],
     "Directiva Hàbitats. Xarxa Natura 2000."),

    # ── FERROVIARI ──
    ("32016L0797", "Dir-2016/797/UE",
     "Directiva (UE) 2016/797 — Interoperabilitat del sistema ferroviari",
     ["ferroviari", "interoperabilitat"],
     "4t paquet ferroviari. Pilar tècnic."),
    ("32016L0798", "Dir-2016/798/UE",
     "Directiva (UE) 2016/798 — Seguretat ferroviària",
     ["ferroviari", "seguretat"],
     "4t paquet ferroviari."),
    ("32016R0796", "Reg-2016/796/UE",
     "Reglament (UE) 2016/796 — Agència Ferroviària Europea (ERA)",
     ["ferroviari", "ERA"],
     "Estableix l'ERA."),
    ("32014R1299", "Reg-1299/2014",
     "Reglament (UE) 1299/2014 — ETI Infraestructura",
     ["ferroviari", "ETI", "infraestructura"],
     "Especificació tècnica interoperabilitat infra."),
    ("32014R1301", "Reg-1301/2014",
     "Reglament (UE) 1301/2014 — ETI Energia",
     ["ferroviari", "ETI", "energia"],
     "ETI subsistema energia."),
    ("32016R0919", "Reg-2016/919",
     "Reglament (UE) 2016/919 — ETI Control-Comandament i Senyalització (CCS)",
     ["ferroviari", "ETI", "senyalització", "ERTMS"],
     "ETI CCS. ERTMS/ETCS."),
    ("32014R1300", "Reg-1300/2014",
     "Reglament (UE) 1300/2014 — ETI Accessibilitat PMR",
     ["ferroviari", "ETI", "accessibilitat"],
     "Persones amb mobilitat reduïda."),
    ("32014R1303", "Reg-1303/2014",
     "Reglament (UE) 1303/2014 — ETI Seguretat en túnels ferroviaris (SRT)",
     ["ferroviari", "ETI", "túnels", "seguretat"],
     "Seguretat túnels."),

    # ── SEGURETAT INDUSTRIAL ──
    ("32006L0042", "Dir-2006/42/CE",
     "Directiva 2006/42/CE — Maquinària",
     ["maquinària", "seguretat_industrial", "marcatge_CE"],
     "Directiva Maquinària. Marcatge CE."),
    ("32014L0035", "Dir-2014/35/UE",
     "Directiva 2014/35/UE — Baixa tensió (LVD)",
     ["elèctric", "baixa_tensió", "marcatge_CE"],
     "Material elèctric de baixa tensió."),
    ("32014L0030", "Dir-2014/30/UE",
     "Directiva 2014/30/UE — Compatibilitat electromagnètica (EMC)",
     ["elèctric", "EMC", "marcatge_CE"],
     "Compatibilitat electromagnètica."),
    ("32014L0034", "Dir-2014/34/UE",
     "Directiva 2014/34/UE — Equips ATEX (atmosferes explosives)",
     ["seguretat_industrial", "ATEX"],
     "Equips per a atmosferes explosives."),
    ("32014R0305", "Reg-305/2011",
     "DUPLICAT — veure Reg-305/2011 a dalt",
     [], "SKIP"),  # Evitar duplicat

    # ── ASCENSORS ──
    ("32014L0033", "Dir-2014/33/UE",
     "Directiva 2014/33/UE — Ascensors i components de seguretat",
     ["ascensors", "marcatge_CE"],
     "Transposada per RD 355/2024."),

    # ── ACCESSIBILITAT ──
    ("32019L0882", "Dir-2019/882/UE",
     "Directiva (UE) 2019/882 — Requisits d'accessibilitat de productes i serveis (EAA)",
     ["accessibilitat"],
     "European Accessibility Act. Transposició: RDL 11/2023."),

    # ── EQUIPS A PRESSIÓ ──
    ("32014L0068", "Dir-2014/68/UE",
     "Directiva 2014/68/UE — Equips a pressió (PED)",
     ["seguretat_industrial", "pressió"],
     "Equips a pressió. Transposada per RD 809/2021."),

    # ── PROTECCIÓ CONTRA INCENDIS ──
    # (No hi ha directiva específica d'incendis en edificació a nivell UE,
    #  es regula via CPR i normes EN nacionals)

    # ── EFICIÈNCIA ENERGÈTICA ──
    ("32010L0031", "Dir-2010/31/UE",
     "Directiva 2010/31/UE — Eficiència energètica dels edificis (EPBD)",
     ["energia", "edificació"],
     "Edificis de consum quasi nul (nZEB). Modificada per Dir 2024/1275."),
    ("32012L0027", "Dir-2012/27/UE",
     "Directiva 2012/27/UE — Eficiència energètica",
     ["energia"],
     "Marc general eficiència energètica."),

    # ── SEGURETAT EN TÚNELS VIARIS ──
    ("32004L0054", "Dir-2004/54/CE",
     "Directiva 2004/54/CE — Requisits mínims de seguretat per a túnels de la xarxa viària transeuropea",
     ["carreteres", "túnels", "seguretat"],
     "Túnels > 500m a TEN-T. Transposada per RD 635/2006."),

    # ── SEGURETAT VIÀRIA ──
    ("32008L0096", "Dir-2008/96/CE",
     "Directiva 2008/96/CE — Gestió de la seguretat de les infraestructures viàries",
     ["carreteres", "seguretat_viària"],
     "Auditories i inspeccions de seguretat viària. Modificada per Dir 2019/1936."),
    ("32019L1936", "Dir-2019/1936/UE",
     "Directiva (UE) 2019/1936 — Modificació Dir 2008/96/CE gestió seguretat viària",
     ["carreteres", "seguretat_viària"],
     "Amplia abast més enllà de TEN-T."),

    # ── EMISSIONS / SOSTENIBILITAT ──
    ("32024R1991", "Reg-2024/1991",
     "Reglament (UE) 2024/1991 — Restauració de la natura",
     ["medi_ambient", "biodiversitat"],
     "Nature Restoration Law. Vigor juny 2024."),
]

# Filtrar duplicats i entrades SKIP
CORE_LEGISLATION = [
    entry for entry in _CORE_LEGISLATION
    if entry[4] != "SKIP"
]


def _make_session():
    s = requests.Session()
    s.headers.update(HEADERS)
    adapter = requests.adapters.HTTPAdapter(max_retries=3)
    s.mount("https://", adapter)
    return s


def _download_pdf(session, celex, dest_path):
    """Descarrega el PDF d'un document EUR-Lex.

    Prova primer versió ES, si falla prova EN.
    """
    dest_path = Path(dest_path)
    if dest_path.exists() and dest_path.stat().st_size > 1000:
        return True  # ja existeix

    for lang in ("ES", "EN"):
        url = f"{BASE_URL}/legal-content/{lang}/TXT/PDF/?uri=CELEX:{celex}"
        time.sleep(DELAY)
        try:
            r = session.get(url, timeout=60)
            if r.status_code == 200 and r.content[:4] == b"%PDF":
                dest_path.parent.mkdir(parents=True, exist_ok=True)
                with open(dest_path, "wb") as f:
                    f.write(r.content)
                return True
        except Exception as e:
            print(f"    [WARN] PDF {lang} {celex}: {e}")

    return False


def _search_eurlex(session, query, max_pages=3):
    """Cerca complementària a EUR-Lex per ampliar cobertura.

    Fa scraping de la pàgina de resultats de cerca.
    Retorna llista de dicts amb celex, titol.
    """
    results = []

    for page in range(1, max_pages + 1):
        params = {
            "text": query,
            "scope": "EURLEX",
            "type": "quick",
            "lang": "es",
            "page": page,
        }
        time.sleep(DELAY)
        try:
            r = session.get(SEARCH_URL, params=params, timeout=30)
            if r.status_code != 200:
                break

            soup = BeautifulSoup(r.text, "html.parser")

            # Buscar resultats — EUR-Lex usa divs amb classe SearchResult
            for item in soup.select(".SearchResult, .EurlexContent"):
                # Extreure CELEX del link
                link = item.find("a", href=re.compile(r"CELEX"))
                if not link:
                    continue
                href = link.get("href", "")
                m = re.search(r"CELEX:(\d{5}[LR]\d{4})", href)
                if not m:
                    continue
                celex = m.group(1)
                titol = link.get_text(strip=True)[:300]

                if celex and titol:
                    results.append({
                        "celex": celex,
                        "titol": titol,
                    })

            # Si no hi ha més pàgines
            if not soup.select(".SearchResult, .EurlexContent"):
                break

        except Exception as e:
            print(f"  [WARN] Cerca EUR-Lex pàg {page}: {e}")
            break

    return results


def build_catalog(catalog_dir=None, downloads_dir=None):
    """Construeix el catàleg EUR-Lex i descarrega PDFs."""
    if catalog_dir is None:
        catalog_dir = EURLEX_CATALOG_DIR
    if downloads_dir is None:
        downloads_dir = EURLEX_DOWNLOADS_DIR

    catalog_dir = Path(catalog_dir)
    downloads_dir = Path(downloads_dir)
    catalog_dir.mkdir(parents=True, exist_ok=True)
    downloads_dir.mkdir(parents=True, exist_ok=True)

    session = _make_session()
    documents = []
    ok = fail = skip = 0

    # ── 1. Processar catàleg base (legislació curada) ──
    print(f"  Processant {len(CORE_LEGISLATION)} entrades del catàleg base...")

    seen_celex = set()
    for celex, codi, titol, temes, obs in CORE_LEGISLATION:
        if celex in seen_celex:
            continue
        seen_celex.add(celex)

        # Determinar tipus
        if "L" in celex[5:6]:
            tipus = "Directiva"
        elif "R" in celex[5:6]:
            tipus = "Reglament"
        else:
            tipus = "Altre"

        # Any de publicació
        any_pub = celex[1:5] if celex[1:5].isdigit() else None

        doc = {
            "celex": celex,
            "codi": codi,
            "titol": titol,
            "tipus": tipus,
            "any_publicacio": int(any_pub) if any_pub else None,
            "estat": "VIGENT",
            "font": "EUR-Lex",
            "temes": temes,
            "observacions": obs,
            "url_fitxa": FITXA_URL_TEMPLATE.format(celex=celex),
            "url_pdf": PDF_URL_TEMPLATE.format(celex=celex),
            "fitxer_local": None,
        }

        # Descarregar PDF
        safe_name = re.sub(r"[^\w\-.]", "_", codi) + ".pdf"
        dest = downloads_dir / safe_name

        print(f"  [{len(documents)+1:02d}] {codi} ", end="", flush=True)

        if _download_pdf(session, celex, dest):
            doc["fitxer_local"] = str(dest.relative_to(PROJECT_ROOT)).replace("\\", "/")
            print(f"✓ ({dest.stat().st_size // 1024} KB)")
            ok += 1
        else:
            print(f"✗ (PDF no disponible)")
            fail += 1

        documents.append(doc)

    # ── 2. Cerca complementària (opcional, pot ampliar cobertura) ──
    SEARCH_QUERIES = [
        "infraestructuras transporte directiva",
        "seguridad túneles carreteras reglamento",
        "interoperabilidad ferroviaria reglamento",
        "productos construcción reglamento",
    ]

    print(f"\n  Cerca complementària EUR-Lex ({len(SEARCH_QUERIES)} consultes)...")

    extra_count = 0
    for query in SEARCH_QUERIES:
        print(f"    Cercant: {query}...")
        found = _search_eurlex(session, query, max_pages=2)
        for item in found:
            celex = item["celex"]
            if celex in seen_celex:
                continue
            seen_celex.add(celex)

            titol = item["titol"]
            tipus = "Directiva" if "L" in celex[5:6] else "Reglament"
            any_pub = celex[1:5] if celex[1:5].isdigit() else None

            # Generar codi curt
            num_match = re.search(r"(\d+)[/\s]*(\d{4})", titol)
            if num_match:
                codi = f"{'Dir' if tipus == 'Directiva' else 'Reg'}-{num_match.group(1)}/{num_match.group(2)}"
            else:
                codi = f"CELEX-{celex}"

            doc = {
                "celex": celex,
                "codi": codi,
                "titol": titol,
                "tipus": tipus,
                "any_publicacio": int(any_pub) if any_pub else None,
                "estat": "VIGENT",
                "font": "EUR-Lex (cerca)",
                "temes": [],
                "observacions": f"Trobat via cerca: '{query}'",
                "url_fitxa": FITXA_URL_TEMPLATE.format(celex=celex),
                "url_pdf": PDF_URL_TEMPLATE.format(celex=celex),
                "fitxer_local": None,
            }

            safe_name = re.sub(r"[^\w\-.]", "_", codi) + ".pdf"
            dest = downloads_dir / safe_name

            if _download_pdf(session, celex, dest):
                doc["fitxer_local"] = str(dest.relative_to(PROJECT_ROOT)).replace("\\", "/")
                ok += 1
            else:
                fail += 1

            documents.append(doc)
            extra_count += 1

    print(f"  Cerca complementària: {extra_count} noves entrades")

    # ── 3. Desar catàleg ──
    catalog = {
        "metadata": {
            "font": "EUR-Lex (Diari Oficial de la UE)",
            "url_base": BASE_URL,
            "data_scraping": datetime.now().strftime("%Y-%m-%d"),
            "total_documents": len(documents),
            "versio": "1.0",
            "idioma_pdf": "ES (castellà); fallback EN",
            "nota": "Legislació UE aplicable a infraestructures de mobilitat",
        },
        "documents": documents,
    }

    out_path = catalog_dir / "catalogo_eurlex.json"
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(catalog, f, ensure_ascii=False, indent=2)

    print(f"\n  Catàleg desat: {out_path}")
    print(f"  Total: {len(documents)} documents ({ok} PDFs, {fail} sense PDF)")

    return documents


def main(catalog_dir=None, downloads_dir=None):
    """Punt d'entrada estàndard per cli.py."""
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")

    print("=" * 50)
    print(" EUR-Lex Scraper — Directives i Reglaments UE")
    print("=" * 50)

    docs = build_catalog(catalog_dir, downloads_dir)

    print()
    print(f"[OK] Catàleg EUR-Lex complet")
    print(f"[DOC] Documents catalogats: {len(docs)}")


if __name__ == "__main__":
    main()
