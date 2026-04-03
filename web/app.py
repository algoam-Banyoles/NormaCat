"""
NormaCat Web — Interficie web per a cerca de normativa tecnica.

Us:
    cd NormaCat
    python web/app.py
    -> Obre http://127.0.0.1:5000 al navegador

Requereix: Flask, i que la indexacio (cli.py index) s'hagi executat.
"""

import json
import os
import re
import sys
import time
from datetime import datetime

# Assegurar que NormaCat root esta al path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# UTF-8 a Windows
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

from flask import Flask, render_template, request, jsonify, send_file

import config
from search.norm_index import NormIndex
from search.norm_resolver import resolve as resolve_ref

# ── Inicialitzacio lazy dels components pesants ────────────────────────────────

_norm_index = None      # NormIndex (lookup en memoria, ~100K normes)
_norm_indexer = None     # NormIndexer (ChromaDB, cerca semantica)
_hybrid_searcher = None  # HybridSearcher (FTS5 + ChromaDB + RRF)


def _get_norm_index():
    """Lazy init del NormIndex (catalegs en memoria)."""
    global _norm_index
    if _norm_index is None:
        print("  Carregant NormIndex (catalegs en memoria)...")
        _norm_index = NormIndex(str(config.PROJECT_ROOT))
    return _norm_index


def _get_hybrid_searcher():
    """Lazy init del HybridSearcher (FTS5 + ChromaDB + RRF)."""
    global _hybrid_searcher
    if _hybrid_searcher is None:
        print("  Carregant HybridSearcher (FTS5 + ChromaDB)...")
        from search.hybrid_search import HybridSearcher
        from indexer.norm_indexer import _get_embedding_model
        _hybrid_searcher = HybridSearcher(
            sqlite_path=config.SQLITE_PATH,
            chroma_path=config.CHROMA_PATH,
            embedding_model=config.EMBEDDING_MODEL,
            model_instance=_get_embedding_model(),
        )
    return _hybrid_searcher


def _get_indexer():
    """Lazy init del NormIndexer (ChromaDB + embeddings)."""
    global _norm_indexer
    if _norm_indexer is None:
        print("  Carregant NormIndexer (ChromaDB + model embeddings)...")
        from indexer.norm_indexer import NormIndexer
        _norm_indexer = NormIndexer(
            chroma_path=config.CHROMA_PATH,
            sqlite_path=config.SQLITE_PATH,
            embedding_model=config.EMBEDDING_MODEL,
        )
    return _norm_indexer


# ── Flask App ──────────────────────────────────────────────────────────────────

app = Flask(__name__,
            template_folder=os.path.join(os.path.dirname(__file__), "templates"),
            static_folder=os.path.join(os.path.dirname(__file__), "static"))


@app.route("/")
def index():
    """Pagina principal."""
    idx = _get_norm_index()
    stats = idx.stats()
    source_labels = {k: v["label"] for k, v in config.SOURCES.items()}
    return render_template("index.html", stats=stats, source_labels=source_labels)


@app.route("/api/search", methods=["POST"])
def api_search():
    """Cerca hibrida: BM25 + Semantica + RRF."""
    data = request.get_json(force=True)
    query = (data.get("query") or "").strip()
    top = min(int(data.get("top", 10)), 50)
    source_filter = (data.get("source") or "").strip()

    if not query:
        return jsonify({"error": "Cal especificar una consulta.", "results": []})

    t0 = time.time()
    try:
        searcher = _get_hybrid_searcher()
        raw = searcher.search(query, top_k=top, source_filter=source_filter)
    except Exception as exc:
        return jsonify({"error": f"Error de cerca: {exc}", "results": []})

    results = []
    for r in raw:
        meta = r.get("metadata", {})
        src = meta.get("source", "")
        source_label = config.SOURCES.get(src, {}).get("label", src)
        methods = r.get("methods", [])

        results.append({
            "text": (r.get("document") or "")[:500],
            "codi": meta.get("doc_codi", ""),
            "titol": meta.get("doc_titol", ""),
            "source": src,
            "source_label": source_label,
            "page": meta.get("page", ""),
            "score": r.get("score", 0),
            "distance": r.get("distance", 0),
            "methods": methods,
        })

    elapsed = round(time.time() - t0, 2)
    return jsonify({"results": results, "elapsed": elapsed, "query": query})


@app.route("/api/lookup", methods=["POST"])
def api_lookup():
    """Lookup d'un codi normatiu concret."""
    data = request.get_json(force=True)
    code = (data.get("code") or "").strip()

    if not code:
        return jsonify({"error": "Cal especificar un codi normatiu."})

    idx = _get_norm_index()

    resolved = resolve_ref(code)
    result = idx.lookup(code)

    return jsonify({
        "code": code,
        "resolved": resolved,
        "result": result,
    })


@app.route("/api/analyze", methods=["POST"])
def api_analyze():
    """Analitza un bloc de text i extreu totes les referencies normatives."""
    data = request.get_json(force=True)
    text = (data.get("text") or "").strip()

    if not text:
        return jsonify({"error": "Cal enganxar text a analitzar."})

    idx = _get_norm_index()

    patterns = [
        r"\b(?:Reial\s+Decret|Real\s+Decreto|R\.?D\.?)\s*(?:n[uu]m\.?\s*)?\d+\s*/\s*\d{2,4}\b",
        r"\b(?:Llei|Ley)\s+(?:n[uu]m\.?\s*)?\d+\s*/\s*\d{2,4}\b",
        r"(?<!\bReial\s)(?<!\bReal\s)\b(?:Decret|Decreto)\s+(?:n[uu]m\.?\s*)?\d+\s*/\s*\d{2,4}\b",
        r"\b(?:Ordre|Orden)\s+[A-Z]+(?:/[A-Z]+)?/\d+/\d{4}\b",
        r"\bUNE(?:-EN)?(?:-ISO)?(?:/IEC)?\s+\d+(?:[-:/]\d+)*\b",
        r"\bISO[/ ]?\d+(?:[-:/]\d+)*\b",
        r"(?<!UNE[-\s])\bEN\s+\d+(?:[-:/]\d+)*\b",
        r"\bDirecti(?:va|ve)\s+\d+/\d+/(?:CE|UE|EU|CEE)\b",
        r"\bReglament(?:o)?\s+\(?UE\)?\s+(?:n[uu]m\.?\s*)?\d+\s*/\s*\d{4}\b",
        r"\b(?:EHE|IAP|NCSE|PG|CTE|REBT|RITE|RIPCI|EAE)[-\s]?\d{0,2}\b",
        r"\bNTE[-\s]?[A-Z]{2,4}[-\s]?\d{2,3}\b",
    ]

    found_refs = set()
    for pattern in patterns:
        for match in re.finditer(pattern, text, re.IGNORECASE):
            ref_text = match.group(0).strip()
            if len(ref_text) > 3:
                found_refs.add(ref_text)

    references = []
    for ref in sorted(found_refs):
        resolved = resolve_ref(ref)
        result = idx.lookup(ref)

        references.append({
            "raw": ref,
            "resolved": resolved,
            "status": result.get("status", "PENDENT") if result else "NO_PARSEJAT",
            "source": result.get("source") if result else None,
            "title": result.get("title") if result else None,
            "substituted_by": result.get("substituted_by") if result else None,
            "fuzzy": result.get("fuzzy", False) if result else False,
        })

    status_order = {"DEROGADA": 0, "PENDENT": 1, "NO_PARSEJAT": 2,
                    "REFERENCIA": 3, "VIGENT": 4}
    references.sort(key=lambda r: status_order.get(r["status"], 5))

    return jsonify({
        "references": references,
        "total": len(references),
        "derogades": sum(1 for r in references if r["status"] == "DEROGADA"),
        "vigents": sum(1 for r in references if r["status"] == "VIGENT"),
        "pendents": sum(1 for r in references if r["status"] in ("PENDENT", "NO_PARSEJAT")),
    })


@app.route("/api/stats")
def api_stats():
    """Estadistiques de l'index."""
    idx = _get_norm_index()
    stats = idx.stats()

    source_info = {}
    for key, count in stats.get("per_source", {}).items():
        label = config.SOURCES.get(key.lower(), {}).get("label", key)
        source_info[key] = {"count": count, "label": label}

    return jsonify({
        "total": stats.get("total_indexed", 0),
        "per_source": source_info,
        "per_status": stats.get("per_status", {}),
    })


# ── Report (informe PDF → DOCX) ──────────────────────────────────────────────

@app.route("/api/report", methods=["POST"])
def api_report():
    """Puja un PDF, analitza les referencies, retorna JSON amb resultats."""
    if "file" not in request.files:
        return jsonify({"error": "Cal pujar un fitxer PDF."}), 400

    file = request.files["file"]
    if not file.filename.lower().endswith(".pdf"):
        return jsonify({"error": "El fitxer ha de ser un PDF."}), 400

    project_name = request.form.get("project_name", "").strip()
    pdf_bytes = file.read()

    try:
        from web.report_generator import analyze_pdf
        result = analyze_pdf(
            pdf_bytes,
            project_name=project_name,
            pdf_filename=file.filename,
        )

        report_dir = os.path.join(os.path.dirname(__file__), "_reports")
        os.makedirs(report_dir, exist_ok=True)

        safe_name = re.sub(r"[^\w\-.]", "_", file.filename.rsplit(".", 1)[0])
        report_name = f"informe_{safe_name}_{datetime.now().strftime('%Y%m%d_%H%M')}.docx"
        report_path = os.path.join(report_dir, report_name)

        with open(report_path, "wb") as f:
            f.write(result["report_bytes"])

        return jsonify({
            "references": result["references"],
            "stats": result["stats"],
            "text_length": result["text_length"],
            "report_filename": report_name,
        })

    except Exception as exc:
        return jsonify({"error": f"Error analitzant PDF: {exc}"}), 500


@app.route("/api/report/download/<filename>")
def download_report(filename):
    """Descarrega un informe DOCX generat."""
    safe = re.sub(r"[^\w\-.]", "_", filename)
    report_dir = os.path.join(os.path.dirname(__file__), "_reports")
    path = os.path.join(report_dir, safe)

    if not os.path.exists(path):
        return jsonify({"error": "Informe no trobat."}), 404

    return send_file(
        path,
        as_attachment=True,
        download_name=safe,
        mimetype="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
    )


# ── RAG (consulta IA) ─────────────────────────────────────────────────────────

@app.route("/api/rag", methods=["POST"])
def api_rag():
    """Consulta RAG: pregunta + cerca + LLM -> resposta sintetitzada."""
    data = request.get_json(force=True)
    query = (data.get("query") or "").strip()
    provider = (data.get("provider") or "").strip() or None
    source_filter = (data.get("source") or "").strip()
    top_k = min(int(data.get("top_k", 12)), 20)

    if not query:
        return jsonify({"error": "Cal especificar una pregunta."})

    try:
        indexer = _get_indexer()
        from web.rag_engine import rag_query
        result = rag_query(
            query=query,
            indexer=indexer,
            provider=provider,
            top_k=top_k,
            source_filter=source_filter,
        )
        return jsonify(result)
    except Exception as exc:
        return jsonify({"error": f"Error RAG: {exc}"})


# ── Feedback ──────────────────────────────────────────────────────────────────

@app.route("/api/feedback", methods=["POST"])
def api_feedback():
    """Enregistra feedback d'un resultat."""
    data = request.get_json(force=True)
    try:
        from search.feedback import record_feedback
        fid = record_feedback(
            query=data.get("query", ""),
            doc_codi=data.get("doc_codi", ""),
            doc_titol=data.get("doc_titol", ""),
            source=data.get("source", ""),
            page=data.get("page", 0),
            rank_position=data.get("rank_position", 0),
            score=data.get("score", 0),
            relevant=data.get("relevant", True),
            methods=data.get("methods", ""),
            text_preview=data.get("text_preview", ""),
        )
        return jsonify({"ok": True, "id": fid})
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500


@app.route("/api/feedback/stats")
def api_feedback_stats():
    """Estadistiques del feedback recollit."""
    from search.feedback import get_feedback_stats
    return jsonify(get_feedback_stats())


# ── Main ──────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("=" * 50)
    print("  NormaCat Web -- Cercador de Normativa Tecnica")
    print("=" * 50)
    print()
    print("  Iniciant servidor...")
    print()

    _get_norm_index()

    # Pre-carregar model embeddings i ChromaDB
    print("  Carregant model embeddings i ChromaDB...")
    searcher = _get_hybrid_searcher()
    searcher._get_model()

    # Pre-carregar cross-encoder (reranker)
    print("  Carregant cross-encoder (reranker)...")
    searcher._get_reranker()._get_model()

    # Pre-escalfar ChromaDB (query dummy per forcar HNSW a RAM)
    print("  Pre-escalfant ChromaDB...")
    _ = searcher.search("test", top_k=1)
    print("  Tot carregat!")

    print()
    print("  Servidor llest!")
    print("  -> Obre http://127.0.0.1:5000 al navegador")
    print("  -> Ctrl+C per aturar")
    print()

    app.run(host="127.0.0.1", port=5000, debug=False)
