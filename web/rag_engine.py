"""
rag_engine.py — Motor RAG (Retrieval-Augmented Generation) per NormaCat.

Pipeline:
  1. Query -> ChromaDB (top-K chunks rellevants)
  2. Construeix prompt amb context normatiu
  3. Envia a LLM (Gemini/Claude/Groq)
  4. Retorna resposta sintetitzada amb cites
"""

import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import config
from llm.llm_provider import LLMProvider
from env_utils import load_local_env

load_local_env()

# ── System prompt per al LLM ──────────────────────────────────────────────────

SYSTEM_PROMPT = """Ets un expert en normativa tecnica d'infraestructures de mobilitat \
a Catalunya (carreteres, ferrocarril, metro, tramvia, bus, bici, aparcaments).

Treballes per al Servei de Supervisio de Projectes de la DGIM \
(Direccio General d'Infraestructures de Mobilitat de Catalunya).

INSTRUCCIONS:
1. Respon SEMPRE en CATALA.
2. Basa la teva resposta EXCLUSIVAMENT en els fragments normatius \
proporcionats com a context. NO inventis normativa ni articles.
3. Cita les fonts amb aquest format: [Font: CODI, pag. X]
4. Si el context no conte informacio suficient, digues-ho clarament: \
"Els fragments recuperats no contenen informacio especifica sobre..."
5. Organitza la resposta de forma clara i practica per a un enginyer \
de supervisio.
6. Si detectes normes derogades al context, avisa'n.
7. Quan sigui rellevant, menciona articles concrets.
8. Sigues concis pero complet. Objectiu: 200-400 paraules."""

USER_PROMPT_TEMPLATE = """PREGUNTA DE L'ENGINYER:
{query}

CONTEXT NORMATIU RECUPERAT (fragments de les bases de dades NormaCat):
{context}

Respon a la pregunta basant-te en els fragments anteriors. Cita les fonts."""


def _format_chunk(i: int, result: dict) -> str:
    """Formata un chunk per incloure al prompt."""
    meta = result.get("metadata", {})
    text = (result.get("document") or "")[:600]
    codi = meta.get("reference", meta.get("doc_codi", "?"))
    titol = meta.get("title", meta.get("doc_titol", ""))
    page = meta.get("page", "")
    source = meta.get("source", "")
    source_label = config.SOURCES.get(source, {}).get("label", source)

    header = f"[Fragment {i}] {codi}"
    if titol:
        header += f" -- {titol[:80]}"
    if page:
        header += f" (pag. {page})"
    header += f" [{source_label}]"

    return f"{header}\n{text}\n"


def rag_query(
    query: str,
    indexer,
    provider: str = None,
    top_k: int = 12,
    source_filter: str = "",
) -> dict:
    """Executa una consulta RAG completa.

    Args:
        query: pregunta de l'usuari
        indexer: NormIndexer instance (amb search())
        provider: "gemini" | "claude" | "groq" (default: config)
        top_k: nombre de chunks a recuperar
        source_filter: filtra per font (opcional)

    Returns:
        dict amb keys: answer, sources, elapsed, model, tokens
    """
    t0 = time.time()

    # 1. Retrieval (cerca hibrida si disponible, fallback semantica)
    fetch_n = top_k * 2 if source_filter else top_k
    try:
        from search.hybrid_search import HybridSearcher
        searcher = HybridSearcher(
            sqlite_path=config.SQLITE_PATH,
            chroma_path=config.CHROMA_PATH,
            embedding_model=config.EMBEDDING_MODEL,
        )
        raw_results = searcher.search(query, top_k=fetch_n,
                                       source_filter=source_filter)
    except Exception:
        raw_results = indexer.search(query, n_results=fetch_n)
        if source_filter:
            raw_results = [
                r for r in raw_results
                if (r.get("metadata", {}).get("source", "").lower() == source_filter.lower())
            ][:top_k]
        else:
            raw_results = raw_results[:top_k]

    if not raw_results:
        return {
            "answer": "No s'han trobat fragments normatius rellevants per a aquesta consulta.",
            "sources": [],
            "elapsed": round(time.time() - t0, 2),
            "model": "",
            "provider": "",
            "tokens_in": 0,
            "tokens_out": 0,
            "chunks_used": 0,
        }

    # 2. Construir context
    context_parts = []
    sources = []
    for i, r in enumerate(raw_results, 1):
        context_parts.append(_format_chunk(i, r))
        meta = r.get("metadata", {})
        dist = r.get("distance", 1.0)
        score = max(0, round((1 - dist / 20) * 100, 1))

        sources.append({
            "codi": meta.get("reference", meta.get("doc_codi", "")),
            "titol": meta.get("title", meta.get("doc_titol", "")),
            "page": meta.get("page", ""),
            "source": meta.get("source", ""),
            "source_label": config.SOURCES.get(
                meta.get("source", ""), {}
            ).get("label", ""),
            "score": score,
            "text_preview": (r.get("document") or "")[:150],
        })

    context = "\n---\n".join(context_parts)
    user_prompt = USER_PROMPT_TEMPLATE.format(query=query, context=context)

    # 3. LLM call
    provider_name = provider or config.DEFAULT_LLM_PROVIDER
    try:
        llm = LLMProvider(backend=provider_name)
        result = llm.call(
            system=SYSTEM_PROMPT,
            user_message=user_prompt,
            max_tokens=2000,
            temperature=0.3,
        )
        answer = result.get("text", "Error: resposta buida del LLM.")
        model = result.get("model", "")
        tokens_in = result.get("tokens_in", 0)
        tokens_out = result.get("tokens_out", 0)
    except Exception as exc:
        answer = f"Error en la consulta al LLM ({provider_name}): {exc}"
        model = ""
        tokens_in = 0
        tokens_out = 0

    elapsed = round(time.time() - t0, 2)

    return {
        "answer": answer,
        "sources": sources,
        "elapsed": elapsed,
        "model": model,
        "provider": provider_name,
        "tokens_in": tokens_in,
        "tokens_out": tokens_out,
        "chunks_used": len(raw_results),
    }
