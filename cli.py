#!/usr/bin/env python3
"""
NormaCat CLI — Interficie de linia de comandes.

Subcomandes:
    scrape  — Descarrega normativa de les fonts configurades
    index   — Indexa els cataleg a ChromaDB + SQLite
    search  — Cerca semantica en llenguatge natural

Us:
    python cli.py scrape --source all
    python cli.py index --source all --reset
    python cli.py search "drenatge transversal carreteres" --top 5
"""
import argparse
import json
import os
import sys
import time

# Assegurar que NormaCat root esta al path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import config


# ── Helpers ───────────────────────────────────────────────────────────────────

def _log(msg: str):
    print(f"  {msg}", file=sys.stderr, flush=True)


# ── SCRAPE ────────────────────────────────────────────────────────────────────

# Mapping font -> modul i funcio d'entrada
_SCRAPER_MAP = {
    "aca":              ("scrapers.aca_scraper",         "main"),
    "adif":             ("scrapers.adif_scraper",        "main"),
    "boe":              ("scrapers.boe_scraper",         "main"),
    "cte":              ("scrapers.cte_scraper",         "main"),
    "dgc":              ("scrapers.norm_scraper",        "main"),
    "era":              ("scrapers.era_scraper",         "main"),
    "industria":        ("scrapers.industria_scraper",   "main"),
    "iso":              ("scrapers.iso_catalog",         "main"),
    "pjcat":            ("scrapers.pjcat_scraper",       "main"),
    "rebt_rite":        ("scrapers.rebt_rite_scraper",   "main"),
    "une":              ("scrapers.une_catalog",         "main"),
}


def cmd_scrape(args):
    """Executa els scrapers de les fonts indicades."""
    sources = list(_SCRAPER_MAP.keys()) if args.source == "all" else [args.source]

    for source in sources:
        if source not in _SCRAPER_MAP:
            _log(f"Font desconeguda: {source}")
            continue

        mod_name, fn_name = _SCRAPER_MAP[source]
        label = config.SOURCES.get(source, {}).get("label", source)
        print(f"\n{'='*60}")
        print(f"  SCRAPING: {label} ({source})")
        print(f"{'='*60}")

        try:
            mod = __import__(mod_name, fromlist=[fn_name])
            fn = getattr(mod, fn_name, None)
            if fn and callable(fn):
                fn()
            else:
                _log(f"No s'ha trobat la funcio {fn_name}() a {mod_name}")
        except Exception as exc:
            _log(f"Error executant {source}: {exc}")

    print(f"\nScraping completat.")


# ── INDEX ─────────────────────────────────────────────────────────────────────

def cmd_index(args):
    """Indexa els cataleg descarregats a ChromaDB + SQLite."""
    try:
        from indexer.norm_indexer import NormIndexer
    except ImportError:
        _log("Error: no s'ha pogut importar NormIndexer.")
        _log("Verifica que indexer/norm_indexer.py existeix.")
        sys.exit(1)

    if args.reset:
        _log("Esborrant bases de dades existents...")
        import shutil
        if os.path.exists(config.CHROMA_PATH):
            shutil.rmtree(config.CHROMA_PATH)
        if os.path.exists(config.SQLITE_PATH):
            os.remove(config.SQLITE_PATH)

    sources = list(config.SOURCES.keys()) if args.source == "all" else [args.source]

    print(f"\n{'='*60}")
    print(f"  INDEXACIO: {len(sources)} fonts")
    print(f"{'='*60}")

    t0 = time.time()

    try:
        indexer = NormIndexer(
            chroma_path=config.CHROMA_PATH,
            sqlite_path=config.SQLITE_PATH,
            embedding_model=config.EMBEDDING_MODEL,
            chunk_size=config.CHUNK_SIZE,
            chunk_overlap=config.CHUNK_OVERLAP,
        )

        for source in sources:
            src_info = config.SOURCES.get(source)
            if not src_info:
                _log(f"Font desconeguda: {source}")
                continue

            catalog_path = str(config.PROJECT_ROOT / src_info["catalog"])
            if not os.path.exists(catalog_path):
                _log(f"Cataleg no trobat: {catalog_path} (executa scrape primer)")
                continue

            label = src_info["label"]
            _log(f"Indexant {label}...")
            indexer.index_catalog(catalog_path, source=source)

        elapsed = time.time() - t0
        _log(f"Indexacio completada en {elapsed:.1f}s")

    except Exception as exc:
        _log(f"Error d'indexacio: {exc}")
        sys.exit(1)


# ── SEARCH ────────────────────────────────────────────────────────────────────

def cmd_search(args):
    """Cerca semantica en les bases de dades indexades."""
    query = args.query
    top_k = args.top

    if not query:
        print("Error: cal especificar una consulta.")
        sys.exit(1)

    # Intentar cerca via ChromaDB
    try:
        from indexer.norm_indexer import NormIndexer

        indexer = NormIndexer(
            chroma_path=config.CHROMA_PATH,
            sqlite_path=config.SQLITE_PATH,
            embedding_model=config.EMBEDDING_MODEL,
        )

        results = indexer.search(query, n_results=top_k)

    except Exception as exc:
        _log(f"Error cerca ChromaDB: {exc}")
        _log("Provant cerca en cataleg en memoria...")

        # Fallback: cerca en NormIndex (cataleg en memoria)
        try:
            from search.norm_index import NormIndex
            idx = NormIndex(str(config.CATALOGS_DIR))
            result = idx.lookup(query)
            if result:
                results = [{"document": json.dumps(result, ensure_ascii=False),
                            "metadata": {"source": result.get("source", "?")},
                            "distance": 0.0}]
            else:
                results = []
        except Exception as exc2:
            _log(f"Error cerca fallback: {exc2}")
            results = []

    # Mostrar resultats
    sep = "\u2550" * 56
    print(f"\n  {sep}")
    print(f'  Resultats per a: "{query}"')
    print(f"  {sep}\n")

    if not results:
        print("  Cap resultat trobat.")
        return

    for i, r in enumerate(results, 1):
        dist = r.get("distance", 0.0)
        meta = r.get("metadata", {})
        source = meta.get("source", "?")
        title = meta.get("title", meta.get("reference", ""))
        page = meta.get("page", "")
        text = r.get("document", "")[:200].strip()

        label = config.SOURCES.get(source, {}).get("label", source)
        page_str = f"Pag. {page}" if page else ""

        print(f"  {i}. [{dist:.2f}] {label} — {title}")
        if page_str:
            print(f"     {page_str} | {text}")
        else:
            print(f"     {text}")
        print()


# ── MAIN ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="NormaCat — Cercador de normativa tecnica",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    subparsers = parser.add_subparsers(dest="command", help="Subcomanda")

    # scrape
    p_scrape = subparsers.add_parser("scrape", help="Descarregar normativa")
    p_scrape.add_argument("--source", default="all",
                          help="Font a descarregar (all per totes)")

    # index
    p_index = subparsers.add_parser("index", help="Indexar cataleg")
    p_index.add_argument("--source", default="all",
                         help="Font a indexar (all per totes)")
    p_index.add_argument("--reset", action="store_true",
                         help="Esborrar BD abans de reindexar")

    # search
    p_search = subparsers.add_parser("search", help="Cerca semantica")
    p_search.add_argument("query", help="Consulta en llenguatge natural")
    p_search.add_argument("--top", type=int, default=5,
                          help="Nombre de resultats (default: 5)")
    p_search.add_argument("--provider", default=None,
                          help="Proveidur LLM per reformular consulta")

    args = parser.parse_args()

    if args.command == "scrape":
        cmd_scrape(args)
    elif args.command == "index":
        cmd_index(args)
    elif args.command == "search":
        cmd_search(args)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
