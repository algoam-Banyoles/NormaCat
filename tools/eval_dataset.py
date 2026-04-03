"""
eval_dataset.py — Genera dataset sintetic i valida amb LLM.

Pipeline:
  1. Genera queries dels 7 tipus de projecte x 15 annexes
  2. Executa cada query contra HybridSearcher
  3. Gemini Flash puntua rellevancia de cada resultat (0-3)
  4. Guarda a search_feedback

Us:
    python tools/eval_dataset.py generate   # Genera queries JSON
    python tools/eval_dataset.py evaluate   # Executa + valida amb LLM
    python tools/eval_dataset.py report     # Mostra estadistiques
"""

import argparse
import json
import os
import re
import sys
import time
from datetime import datetime
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import config
from env_utils import load_local_env
load_local_env()

DATASET_PATH = config.DATA_DIR / "eval_queries.json"
RESULTS_PATH = config.DATA_DIR / "eval_results.json"

# ══════════════════════════════════════════════════════════════
# QUERIES SINTETIQUES
# ══════════════════════════════════════════════════════════════

SYNTHETIC_QUERIES = [
    # A01 ANTECEDENTS
    {"text": "contingut minim projecte constructiu obra publica",
     "tipologia": "G", "annex": "A01",
     "expected_sources": ["pjcat", "boe"],
     "expected_terms": ["LCSP", "231", "projecte"]},
    {"text": "requisits ordre d'estudi i antecedents administratius",
     "tipologia": "G", "annex": "A01",
     "expected_sources": ["pjcat"],
     "expected_terms": ["LOP", "3/2007"]},

    # A02 QUALITAT I MEDI AMBIENT
    {"text": "pla de control de qualitat obra civil",
     "tipologia": "G", "annex": "A02",
     "expected_sources": ["territori", "pjcat"],
     "expected_terms": ["qualitat", "control", "assaig"]},
    {"text": "gestio de residus de construccio i demolicio",
     "tipologia": "G", "annex": "A02",
     "expected_sources": ["boe", "pjcat"],
     "expected_terms": ["residus", "residuos", "7/2022"]},
    {"text": "avaluacio impacte ambiental projectes infraestructures",
     "tipologia": "G", "annex": "A02",
     "expected_sources": ["pjcat", "eurlex", "boe"],
     "expected_terms": ["impacte", "impacto", "ambiental"]},

    # A03 ESTUDIS FUNCIONALS
    {"text": "parametres de tracat carretera C-80",
     "tipologia": "C", "annex": "A03",
     "expected_sources": ["dgc"],
     "expected_terms": ["tracat", "trazado", "3.1-IC"]},
    {"text": "radi minim en planta per a velocitat de projecte 80 km/h",
     "tipologia": "C", "annex": "A03",
     "expected_sources": ["dgc"],
     "expected_terms": ["radi", "radio", "planta"]},
    {"text": "galib ferroviari FGC ample metric",
     "tipologia": "F", "annex": "A03",
     "expected_sources": ["adif"],
     "expected_terms": ["galib", "galibo", "metric"]},
    {"text": "seccio transversal tipus carretera convencional",
     "tipologia": "C", "annex": "A03",
     "expected_sources": ["dgc"],
     "expected_terms": ["seccio", "seccion", "transversal"]},

    # A04 PLANEJAMENT I URBANISME
    {"text": "classificacio del sol urbanisme Catalunya",
     "tipologia": "G", "annex": "A04",
     "expected_sources": ["pjcat", "territori"],
     "expected_terms": ["urbanisme", "urbanismo", "sol"]},

    # A05 TRACAT / GEOMETRIA
    {"text": "norma 3.1-IC tracat instruccio de carreteres",
     "tipologia": "C", "annex": "A05",
     "expected_sources": ["dgc"],
     "expected_terms": ["3.1-IC", "trazado"]},
    {"text": "pendents longitudinals maximes en rampes",
     "tipologia": "C", "annex": "A05",
     "expected_sources": ["dgc"],
     "expected_terms": ["pendent", "pendiente", "rampa"]},
    {"text": "amplada minima plataforma tramvia",
     "tipologia": "T", "annex": "A05",
     "expected_sources": ["territori", "dgc"],
     "expected_terms": ["tramvia", "tranvia", "amplada"]},
    {"text": "amplada minima carril bici segregat",
     "tipologia": "B", "annex": "A05",
     "expected_sources": ["territori", "dgc"],
     "expected_terms": ["carril", "bici", "amplada"]},

    # A06 FERMS I PAVIMENTS
    {"text": "mescles bituminoses capa de rodadura",
     "tipologia": "C", "annex": "A06",
     "expected_sources": ["dgc"],
     "expected_terms": ["mezcla", "bituminosa", "rodadura", "542"]},
    {"text": "article 542 PG-3 mescla bituminosa tipus AC",
     "tipologia": "C", "annex": "A06",
     "expected_sources": ["dgc"],
     "expected_terms": ["542", "AC", "PG-3"]},
    {"text": "esplanada i ferm en carreteres categories de transit",
     "tipologia": "C", "annex": "A06",
     "expected_sources": ["dgc"],
     "expected_terms": ["esplanada", "explanada", "6.1-IC"]},
    {"text": "reg d'imprimacio i reg d'adherencia ferms",
     "tipologia": "C", "annex": "A06",
     "expected_sources": ["dgc"],
     "expected_terms": ["riego", "imprimacion", "adherencia"]},

    # A07 DRENATGE
    {"text": "drenatge transversal carreteres periode de retorn",
     "tipologia": "C", "annex": "A07",
     "expected_sources": ["dgc"],
     "expected_terms": ["drenaje", "transversal", "5.2-IC"]},
    {"text": "drenatge longitudinal cunetes i col.lectors",
     "tipologia": "C", "annex": "A07",
     "expected_sources": ["dgc", "aca"],
     "expected_terms": ["drenaje", "cuneta", "colector"]},
    {"text": "dimensionament hidraulic obres de fabrica",
     "tipologia": "C", "annex": "A07",
     "expected_sources": ["dgc", "aca"],
     "expected_terms": ["hidraulico", "obra", "fabrica"]},

    # A08 INSTAL.LACIONS FERROVIARIES
    {"text": "catenaria i electrificacio ferroviaria",
     "tipologia": "F", "annex": "A08",
     "expected_sources": ["adif"],
     "expected_terms": ["catenaria", "electrificacion"]},
    {"text": "senyalitzacio ferroviaria ERTMS i enclavaments",
     "tipologia": "F", "annex": "A08",
     "expected_sources": ["adif", "eurlex"],
     "expected_terms": ["ERTMS", "enclavamiento", "senalizacion"]},
    {"text": "superestructura de via balast i travesses",
     "tipologia": "F", "annex": "A08",
     "expected_sources": ["adif"],
     "expected_terms": ["via", "balasto", "traviesa"]},

    # A09 INSTAL.LACIONS NO FERROVIARIES
    {"text": "instal.lacio electrica baixa tensio REBT",
     "tipologia": "G", "annex": "A09",
     "expected_sources": ["industria", "boe"],
     "expected_terms": ["REBT", "842/2002", "baja tension"]},
    {"text": "climatitzacio i ventilacio RITE edificis",
     "tipologia": "G", "annex": "A09",
     "expected_sources": ["industria", "boe"],
     "expected_terms": ["RITE", "1027/2007", "ventilacion"]},
    {"text": "proteccio contra incendis en aparcaments soterrats",
     "tipologia": "P", "annex": "A09",
     "expected_sources": ["industria", "cte", "boe"],
     "expected_terms": ["incendios", "aparcamiento", "RIPCI"]},
    {"text": "enllumenat public carrers i vials eficiencia energetica",
     "tipologia": "C", "annex": "A09",
     "expected_sources": ["industria", "boe"],
     "expected_terms": ["alumbrado", "iluminacion", "eficiencia"]},

    # A10 SENYALITZACIO
    {"text": "senyalitzacio horitzontal viaria marcas viales",
     "tipologia": "C", "annex": "A10",
     "expected_sources": ["dgc"],
     "expected_terms": ["marcas viales", "8.2-IC", "horizontal"]},
    {"text": "senyalitzacio vertical carreteres norma 8.1-IC",
     "tipologia": "C", "annex": "A10",
     "expected_sources": ["dgc"],
     "expected_terms": ["8.1-IC", "senalizacion", "vertical"]},
    {"text": "senyalitzacio d'obres en carreteres 8.3-IC",
     "tipologia": "C", "annex": "A10",
     "expected_sources": ["dgc"],
     "expected_terms": ["8.3-IC", "obras", "senalizacion"]},
    {"text": "abalisament viari balises i captafars",
     "tipologia": "C", "annex": "A10",
     "expected_sources": ["territori", "dgc"],
     "expected_terms": ["balizamiento", "captafaro"]},

    # A11 ACCESSIBILITAT
    {"text": "accessibilitat persones mobilitat reduida estacions",
     "tipologia": "M", "annex": "A11",
     "expected_sources": ["pjcat", "boe", "eurlex"],
     "expected_terms": ["accessibilitat", "accesibilidad", "209/2023"]},
    {"text": "codi accessibilitat Catalunya Decret 209/2023",
     "tipologia": "G", "annex": "A11",
     "expected_sources": ["pjcat"],
     "expected_terms": ["209/2023", "accessibilitat"]},

    # A12 SUBMINISTRAMENTS
    {"text": "connexio xarxa electrica subministrament",
     "tipologia": "G", "annex": "A12",
     "expected_sources": ["industria"],
     "expected_terms": ["electrica", "suministro"]},

    # A13 INTEGRACIO URBANA
    {"text": "reposicio serveis afectats xarxes existents",
     "tipologia": "G", "annex": "A13",
     "expected_sources": ["pjcat", "boe"],
     "expected_terms": ["serveis", "servicios", "afectados"]},

    # A15 ESTRUCTURES
    {"text": "codi estructural formigo armat RD 470/2021",
     "tipologia": "G", "annex": "A15",
     "expected_sources": ["boe", "dgc"],
     "expected_terms": ["estructural", "470/2021", "hormigon"]},
    {"text": "accions en ponts de carretera IAP-11",
     "tipologia": "C", "annex": "A15",
     "expected_sources": ["dgc"],
     "expected_terms": ["IAP", "puente", "acciones"]},
    {"text": "fonamentacions profundes pilotatge",
     "tipologia": "G", "annex": "A15",
     "expected_sources": ["dgc"],
     "expected_terms": ["cimentacion", "pilote", "profunda"]},
    {"text": "murs de contencio empenta de terres",
     "tipologia": "G", "annex": "A15",
     "expected_sources": ["dgc"],
     "expected_terms": ["muro", "contencion", "empuje"]},

    # SEGURETAT I SALUT
    {"text": "estudi seguretat i salut en obres de construccio",
     "tipologia": "G", "annex": "",
     "expected_sources": ["boe", "pjcat"],
     "expected_terms": ["1627/1997", "seguridad", "salud"]},
    {"text": "coordinacio seguretat en fase de projecte i execucio",
     "tipologia": "G", "annex": "",
     "expected_sources": ["boe"],
     "expected_terms": ["coordinador", "seguridad"]},

    # CONTRACTACIO
    {"text": "plec prescripcions tecniques particulars LCSP",
     "tipologia": "G", "annex": "",
     "expected_sources": ["pjcat", "boe"],
     "expected_terms": ["pliego", "prescripciones", "9/2017"]},

    # NORMATIVA UE
    {"text": "directiva avaluacio impacte ambiental projectes UE",
     "tipologia": "G", "annex": "",
     "expected_sources": ["eurlex"],
     "expected_terms": ["2011/92", "impacto", "ambiental"]},
    {"text": "reglament productes construccio marcatge CE",
     "tipologia": "G", "annex": "",
     "expected_sources": ["eurlex"],
     "expected_terms": ["305/2011", "productos", "construccion"]},
    {"text": "directiva interoperabilitat sistema ferroviari",
     "tipologia": "F", "annex": "",
     "expected_sources": ["eurlex", "era"],
     "expected_terms": ["2016/797", "interoperabilidad"]},
    {"text": "seguretat en tunels viaris directiva europea",
     "tipologia": "C", "annex": "",
     "expected_sources": ["eurlex", "dgc"],
     "expected_terms": ["2004/54", "tunel", "seguridad"]},

    # TIPOLOGIES ESPECIFIQUES
    {"text": "estacio d'autobusos marquesina i zona d'espera",
     "tipologia": "A", "annex": "",
     "expected_sources": ["territori", "pjcat"],
     "expected_terms": ["estacio", "autobus", "marquesina"]},
    {"text": "parc de vehicles cotxeres ventilacio i incendis",
     "tipologia": "P", "annex": "",
     "expected_sources": ["cte", "industria"],
     "expected_terms": ["ventilacion", "incendios", "aparcamiento"]},
    {"text": "via verda carril bici senyalitzacio ciclista",
     "tipologia": "B", "annex": "",
     "expected_sources": ["territori", "dgc"],
     "expected_terms": ["ciclista", "bici", "senalizacion"]},
    {"text": "plataforma tramvia segregada galib vehicle",
     "tipologia": "T", "annex": "",
     "expected_sources": ["territori"],
     "expected_terms": ["tramvia", "plataforma", "galib"]},
    {"text": "ventilacio tunel metro sistema d'extraccio de fums",
     "tipologia": "M", "annex": "",
     "expected_sources": ["adif", "industria"],
     "expected_terms": ["ventilacion", "tunel", "humos"]},
]


# ══════════════════════════════════════════════════════════════
# GENERACIO
# ══════════════════════════════════════════════════════════════

def generate_dataset():
    """Genera el fitxer JSON de queries."""
    config.DATA_DIR.mkdir(parents=True, exist_ok=True)

    with open(DATASET_PATH, "w", encoding="utf-8") as f:
        json.dump(SYNTHETIC_QUERIES, f, ensure_ascii=False, indent=2)

    per_tipologia = {}
    per_annex = {}
    for q in SYNTHETIC_QUERIES:
        t = q["tipologia"]
        a = q["annex"] or "(sense annex)"
        per_tipologia[t] = per_tipologia.get(t, 0) + 1
        per_annex[a] = per_annex.get(a, 0) + 1

    print(f"  Dataset generat: {len(SYNTHETIC_QUERIES)} queries")
    print(f"  Fitxer: {DATASET_PATH}")
    print(f"\n  Per tipologia:")
    labels = {"C": "Carreteres", "F": "Ferrocarril", "M": "Metro",
              "T": "Tramvia", "A": "Bus", "B": "Bici", "P": "Aparcament",
              "G": "General"}
    for t, n in sorted(per_tipologia.items()):
        print(f"    {labels.get(t, t):15s} {n}")
    print(f"\n  Per annex:")
    for a, n in sorted(per_annex.items()):
        print(f"    {a:15s} {n}")


# ══════════════════════════════════════════════════════════════
# AVALUACIO AMB LLM
# ══════════════════════════════════════════════════════════════

LLM_EVAL_SYSTEM = """Ets un avaluador de rellevancia de cerca per a un sistema de
normativa tecnica d'infraestructures de mobilitat a Catalunya.

Se t'ha proporcionat una CONSULTA d'un enginyer de supervisio
i un FRAGMENT de text recuperat d'un document normatiu.

Puntua la RELLEVANCIA del fragment respecte a la consulta amb:
  3 = Molt rellevant: respon directament la consulta
  2 = Rellevant: conte informacio util relacionada
  1 = Marginalment rellevant: tracta el tema pero no respon
  0 = Irrellevant: no te res a veure amb la consulta

Respon NOMES amb un numero (0, 1, 2 o 3). Res mes."""

LLM_EVAL_USER = """CONSULTA: {query}

FRAGMENT (font: {source}, document: {doc_codi}):
{text}

Puntuacio (0-3):"""

DELAY_BETWEEN_CALLS = 1.5


def evaluate_with_llm(provider: str = "gemini"):
    """Executa queries, valida resultats amb LLM, guarda feedback."""

    if not DATASET_PATH.exists():
        print("  ERROR: Cal primer generar el dataset:")
        print("    python tools/eval_dataset.py generate")
        return

    with open(DATASET_PATH, encoding="utf-8") as f:
        queries = json.load(f)

    from search.hybrid_search import HybridSearcher
    from search.feedback import record_feedback
    from llm.llm_provider import LLMProvider

    print(f"  Inicialitzant HybridSearcher...")
    searcher = HybridSearcher(
        sqlite_path=config.SQLITE_PATH,
        chroma_path=config.CHROMA_PATH,
        embedding_model=config.EMBEDDING_MODEL,
    )

    print(f"  Inicialitzant LLM ({provider})...")
    llm = LLMProvider(backend=provider)

    all_results = []
    total_feedback = 0
    total_relevant = 0
    total_irrelevant = 0

    print(f"\n  Avaluant {len(queries)} queries x top 5 resultats...")
    print(f"  Estimacio: {len(queries) * 5 * DELAY_BETWEEN_CALLS / 60:.0f} minuts\n")

    for qi, q in enumerate(queries, 1):
        query_text = q["text"]
        tipologia = q["tipologia"]
        annex = q.get("annex", "")

        print(f"  [{qi:3d}/{len(queries)}] {query_text[:60]}...", flush=True)

        try:
            results = searcher.search(query_text, top_k=5)
        except Exception as exc:
            print(f"    ERROR cerca: {exc}")
            continue

        query_evals = []

        for ri, r in enumerate(results):
            meta = r.get("metadata", {})
            text = (r.get("document") or "")[:400]
            source = meta.get("source", "")
            doc_codi = meta.get("doc_codi", "")
            doc_titol = meta.get("doc_titol", "")
            page = meta.get("page", 0)
            score = r.get("score", 0)
            methods = r.get("methods", [])

            time.sleep(DELAY_BETWEEN_CALLS)
            try:
                user_msg = LLM_EVAL_USER.format(
                    query=query_text,
                    source=source,
                    doc_codi=doc_codi,
                    text=text,
                )
                result_llm = llm.call(
                    system=LLM_EVAL_SYSTEM,
                    user_message=user_msg,
                    max_tokens=10,
                    temperature=0,
                )

                raw_score = result_llm.get("text", "").strip()
                m = re.search(r"[0-3]", raw_score)
                llm_score = int(m.group()) if m else -1

            except Exception as exc:
                print(f"    LLM error resultat {ri+1}: {exc}")
                llm_score = -1
                continue

            relevant = llm_score >= 2

            record_feedback(
                query=query_text,
                doc_codi=doc_codi,
                doc_titol=doc_titol,
                source=source,
                page=page,
                rank_position=ri + 1,
                score=score,
                relevant=relevant,
                methods=",".join(methods) if methods else "",
                text_preview=text[:200],
            )
            total_feedback += 1
            if relevant:
                total_relevant += 1
            else:
                total_irrelevant += 1

            query_evals.append({
                "rank": ri + 1,
                "source": source,
                "doc_codi": doc_codi,
                "llm_score": llm_score,
                "search_score": score,
                "relevant": relevant,
            })

            marker = "+" if relevant else "-"
            print(f"    {marker} #{ri+1} [{source:6s}] {doc_codi[:30]:30s} LLM:{llm_score}",
                  flush=True)

        all_results.append({
            "query": query_text,
            "tipologia": tipologia,
            "annex": annex,
            "results": query_evals,
        })

    config.DATA_DIR.mkdir(parents=True, exist_ok=True)
    with open(RESULTS_PATH, "w", encoding="utf-8") as f:
        json.dump(all_results, f, ensure_ascii=False, indent=2)

    print(f"\n  {'='*50}")
    print(f"  AVALUACIO COMPLETADA")
    print(f"  {'='*50}")
    print(f"  Queries avaluades:  {len(queries)}")
    print(f"  Resultats avaluats: {total_feedback}")
    print(f"  Rellevants (>=2):   {total_relevant} "
          f"({total_relevant/max(total_feedback,1)*100:.0f}%)")
    print(f"  Irrellevants (<=1): {total_irrelevant} "
          f"({total_irrelevant/max(total_feedback,1)*100:.0f}%)")
    print(f"  Resultats desats a: {RESULTS_PATH}")


# ══════════════════════════════════════════════════════════════
# INFORME
# ══════════════════════════════════════════════════════════════

def generate_report():
    """Genera informe d'avaluacio a partir dels resultats."""

    if not RESULTS_PATH.exists():
        print("  ERROR: Cal primer executar l'avaluacio:")
        print("    python tools/eval_dataset.py evaluate")
        return

    with open(RESULTS_PATH, encoding="utf-8") as f:
        all_results = json.load(f)

    per_tipologia = {}
    per_source = {}
    per_rank = {1: [], 2: [], 3: [], 4: [], 5: []}

    for entry in all_results:
        tip = entry["tipologia"]
        if tip not in per_tipologia:
            per_tipologia[tip] = {"total": 0, "relevant": 0}

        for r in entry["results"]:
            llm_score = r.get("llm_score", -1)
            if llm_score < 0:
                continue

            relevant = llm_score >= 2

            per_tipologia[tip]["total"] += 1
            if relevant:
                per_tipologia[tip]["relevant"] += 1

            src = r.get("source", "?")
            if src not in per_source:
                per_source[src] = {"total": 0, "relevant": 0, "scores": []}
            per_source[src]["total"] += 1
            if relevant:
                per_source[src]["relevant"] += 1
            per_source[src]["scores"].append(llm_score)

            rank = r.get("rank", 0)
            if rank in per_rank:
                per_rank[rank].append(llm_score)

    labels = {"C": "Carreteres", "F": "Ferrocarril", "M": "Metro",
              "T": "Tramvia", "A": "Bus", "B": "Bici", "P": "Aparcament",
              "G": "General"}

    print(f"\n  {'='*60}")
    print(f"  INFORME D'AVALUACIO NormaCat")
    print(f"  {'='*60}")

    print(f"\n  Per tipologia:")
    for tip in sorted(per_tipologia.keys()):
        d = per_tipologia[tip]
        ratio = d["relevant"] / d["total"] * 100 if d["total"] > 0 else 0
        label = labels.get(tip, tip)
        bar = "#" * int(ratio / 5) + "." * (20 - int(ratio / 5))
        print(f"    {label:15s} {bar} {ratio:5.1f}%  ({d['relevant']}/{d['total']})")

    print(f"\n  Per font:")
    for src in sorted(per_source.keys(), key=lambda s: -per_source[s]["total"]):
        d = per_source[src]
        ratio = d["relevant"] / d["total"] * 100 if d["total"] > 0 else 0
        avg = sum(d["scores"]) / len(d["scores"]) if d["scores"] else 0
        src_label = config.SOURCES.get(src, {}).get("label", src)
        print(f"    {src_label:20s} {ratio:5.1f}% rellevant  "
              f"(avg LLM: {avg:.1f})  n={d['total']}")

    print(f"\n  Per posicio al ranking:")
    for rank in sorted(per_rank.keys()):
        scores = per_rank[rank]
        if not scores:
            continue
        avg = sum(scores) / len(scores)
        rel = sum(1 for s in scores if s >= 2) / len(scores) * 100
        print(f"    #{rank}: avg={avg:.1f}  {rel:.0f}% rellevant  (n={len(scores)})")

    problematic = []
    for entry in all_results:
        relevant_count = sum(1 for r in entry["results"]
                           if r.get("llm_score", 0) >= 2)
        if relevant_count == 0 and entry["results"]:
            problematic.append(entry["query"])

    if problematic:
        print(f"\n  Queries SENSE cap resultat rellevant ({len(problematic)}):")
        for q in problematic:
            print(f"    - {q}")


# ══════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════

def main():
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")

    parser = argparse.ArgumentParser(
        description="Dataset sintetic i avaluacio automatica NormaCat"
    )
    parser.add_argument("command", choices=["generate", "evaluate", "report"],
                        help="generate=crea queries, evaluate=valida amb LLM, report=estadistiques")
    parser.add_argument("--provider", default="gemini",
                        help="LLM per avaluar (default: gemini)")
    args = parser.parse_args()

    print("=" * 50)
    print("  NormaCat -- Avaluacio Automatica")
    print("=" * 50)
    print()

    if args.command == "generate":
        generate_dataset()
    elif args.command == "evaluate":
        evaluate_with_llm(provider=args.provider)
    elif args.command == "report":
        generate_report()


if __name__ == "__main__":
    main()
