"""
hybrid_search.py — Cerca hibrida: BM25 (FTS5) + Semantica (ChromaDB) + RRF.

Resol el problema de desbalanceig de fonts i millora la precisio
combinant keyword matching amb cerca semantica.

Components:
  1. SQLite FTS5 -> BM25 keyword ranking (paraules exactes)
  2. ChromaDB -> Semantic similarity (significat)
  3. RRF (Reciprocal Rank Fusion) -> fusio dels dos rankings
  4. Diversitat de fonts -> evita que una sola font domini

Us:
    from search.hybrid_search import HybridSearcher
    searcher = HybridSearcher(sqlite_path, chroma_path, embedding_model)
    results = searcher.search("senyalitzacio horitzontal viaria", top_k=10)
"""

import hashlib
import os
import re
import sqlite3
import sys
import time as _time
import unicodedata
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import chromadb
from sentence_transformers import SentenceTransformer

from search.query_expansion import expand_for_bm25
from search.tipologia import get_source_multiplier


# ── Helpers ────────────────────────────────────────────────────────────────────

def _normalize_text(text: str) -> str:
    """Normalitza text per a FTS: treu accents, lowercase."""
    nfkd = unicodedata.normalize("NFKD", text)
    ascii_text = "".join(c for c in nfkd if not unicodedata.combining(c))
    return ascii_text.lower()


def _strip_accents(text: str) -> str:
    """Treu accents mantenint la resta."""
    nfkd = unicodedata.normalize("NFKD", text)
    return "".join(c for c in nfkd if not unicodedata.combining(c))


# ── FTS5 Setup ─────────────────────────────────────────────────────────────────

FTS_SCHEMA = """
CREATE VIRTUAL TABLE IF NOT EXISTS chunks_fts USING fts5(
    text,
    doc_codi,
    doc_titol,
    source,
    chunk_rowid UNINDEXED,
    doc_id UNINDEXED,
    page_num UNINDEXED,
    chroma_id UNINDEXED,
    tokenize='unicode61 remove_diacritics 2'
);
"""


def build_fts_index(sqlite_path: str) -> int:
    """Construeix (o reconstrueix) la taula FTS5 a partir dels chunks existents.

    Retorna el nombre de chunks indexats.
    """
    conn = sqlite3.connect(sqlite_path)
    conn.row_factory = sqlite3.Row

    conn.execute("DROP TABLE IF EXISTS chunks_fts")
    conn.execute(FTS_SCHEMA)

    # Afegir columna 'source' a documents si no existeix
    try:
        conn.execute("ALTER TABLE documents ADD COLUMN source TEXT DEFAULT ''")
        conn.commit()
    except sqlite3.OperationalError:
        pass

    # Detectar source a partir del filename
    conn.execute("UPDATE documents SET source = '' WHERE source IS NULL")
    for row in conn.execute("SELECT id, filename FROM documents"):
        doc_id = row["id"]
        filename = row["filename"] or ""
        m = re.search(r"downloads[/\\](\w+)[/\\]", filename)
        if m:
            source = m.group(1)
        else:
            m2 = re.search(r"normativa_(\w+)[/\\]", filename)
            source = m2.group(1) if m2 else ""
        conn.execute("UPDATE documents SET source = ? WHERE id = ?",
                     (source, doc_id))
    conn.commit()

    # Poblar FTS
    count = 0
    cursor = conn.execute("""
        SELECT c.id, c.doc_id, c.text, c.page_num, c.chroma_id,
               d.codi, d.titol, d.source
        FROM chunks c
        JOIN documents d ON c.doc_id = d.id
        WHERE c.text IS NOT NULL AND length(c.text) > 10
    """)

    batch = []
    for row in cursor:
        batch.append((
            _strip_accents(row["text"]),
            row["codi"] or "",
            _strip_accents(row["titol"] or ""),
            row["source"] or "",
            row["id"],
            row["doc_id"],
            row["page_num"] or 0,
            row["chroma_id"] or "",
        ))
        count += 1

        if len(batch) >= 5000:
            conn.executemany(
                "INSERT INTO chunks_fts VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                batch
            )
            conn.commit()
            batch = []
            print(f"  {count:>9,} chunks...", flush=True)

    if batch:
        conn.executemany(
            "INSERT INTO chunks_fts VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            batch
        )
        conn.commit()

    conn.close()
    return count


# ── Hybrid Searcher ────────────────────────────────────────────────────────────

class HybridSearcher:
    """Cerca hibrida BM25 + Semantica amb fusio RRF."""

    WEIGHT_BM25 = 0.6
    WEIGHT_SEMANTIC = 0.4
    RRF_K = 60
    MAX_PER_SOURCE = 4

    # Limits especifics per font (override de MAX_PER_SOURCE)
    SOURCE_LIMITS = {
        "pjcat": 1,       # 31.7% rellevant — lleis generiques, molt soroll
        "iso": 1,         # Nomes metadades
        "une": 1,         # Nomes metadades
        "aca": 1,         # 28.1% rellevant — jornades i proves generiques
        "adif": 2,        # 40.7% rellevant pero 39% del corpus -> domina
        "rebt_rite": 2,   # 54.2% pero poc volum, limitar per diversitat
    }

    def __init__(self, sqlite_path: str, chroma_path: str,
                 embedding_model: str = "paraphrase-multilingual-MiniLM-L12-v2",
                 model_instance=None):
        self.sqlite_path = sqlite_path
        self.chroma_path = chroma_path
        self._model_name = embedding_model
        self._model = model_instance

        # Connexio ChromaDB persistent
        self._chroma_client = chromadb.PersistentClient(path=chroma_path)
        self._collection = self._chroma_client.get_or_create_collection("normativa")

        # Connexio SQLite persistent amb optimitzacions
        self._sqlite_conn = sqlite3.connect(
            sqlite_path, check_same_thread=False
        )
        self._sqlite_conn.row_factory = sqlite3.Row
        self._sqlite_conn.execute("PRAGMA journal_mode=WAL")
        self._sqlite_conn.execute("PRAGMA cache_size=-64000")
        self._sqlite_conn.execute("PRAGMA mmap_size=268435456")
        self._sqlite_conn.execute("PRAGMA synchronous=NORMAL")

        # Verificar FTS disponible
        try:
            row = self._sqlite_conn.execute(
                "SELECT COUNT(*) FROM chunks_fts"
            ).fetchone()
            self._fts_ok = row[0] > 0
        except Exception:
            self._fts_ok = False

        # Reranker (cross-encoder)
        self._reranker = None

        # Cache de resultats (TTL 5 min)
        self._cache = {}
        self._cache_ttl = 300

    def _get_model(self):
        if self._model is None:
            self._model = SentenceTransformer(self._model_name)
        return self._model

    def _get_reranker(self):
        if self._reranker is None:
            from search.reranker import Reranker
            self._reranker = Reranker()
        return self._reranker

    def _cache_key(self, query, top_k, source_filter, tipologia=""):
        raw = f"{query}|{top_k}|{source_filter}|{tipologia}"
        return hashlib.md5(raw.encode()).hexdigest()

    def search_bm25(self, query: str, top_k: int = 30) -> list[dict]:
        """Cerca BM25 via SQLite FTS5."""
        fts_expression = expand_for_bm25(query)
        if not fts_expression:
            return []

        conn = self._sqlite_conn
        try:
            rows = conn.execute("""
                SELECT chunk_rowid, doc_id, text, doc_codi, doc_titol,
                       source, page_num, chroma_id, rank
                FROM chunks_fts
                WHERE chunks_fts MATCH ?
                ORDER BY rank
                LIMIT ?
            """, (fts_expression, top_k)).fetchall()
        except Exception:
            # Fallback: buscar paraules individuals
            words = [w for w in re.sub(r'[^\w\s]', ' ', fts_expression).split()
                     if len(w) > 2]
            rows = []
            for word in words[:3]:
                try:
                    partial = conn.execute("""
                        SELECT chunk_rowid, doc_id, text, doc_codi,
                               doc_titol, source, page_num, chroma_id, rank
                        FROM chunks_fts
                        WHERE chunks_fts MATCH ?
                        ORDER BY rank
                        LIMIT ?
                    """, (word, top_k // max(len(words), 1))).fetchall()
                    rows.extend(partial)
                except Exception:
                    pass

        results = []
        seen = set()
        for row in rows:
            rid = row["chunk_rowid"]
            if rid in seen:
                continue
            seen.add(rid)
            results.append({
                "chunk_id": rid,
                "doc_id": row["doc_id"],
                "document": row["text"],
                "metadata": {
                    "doc_codi": row["doc_codi"],
                    "doc_titol": row["doc_titol"],
                    "source": row["source"],
                    "page": row["page_num"],
                },
                "chroma_id": row["chroma_id"],
                "bm25_rank": abs(float(row["rank"])),
            })

        return results

    def search_semantic(self, query: str, top_k: int = 30) -> list[dict]:
        """Cerca semantica via ChromaDB."""
        model = self._get_model()

        embedding = model.encode(
            [query], show_progress_bar=False, convert_to_numpy=True
        )[0].tolist()

        result = self._collection.query(
            query_embeddings=[embedding],
            n_results=top_k,
            include=["documents", "metadatas", "distances"],
        )

        docs = result.get("documents", [[]])[0]
        metas = result.get("metadatas", [[]])[0]
        dists = result.get("distances", [[]])[0]

        results = []
        for text, meta, dist in zip(docs, metas, dists):
            meta = meta or {}
            # Assegurar que source existeix (pot faltar en metadades)
            if not meta.get("source") and meta.get("doc_codi"):
                codi = meta["doc_codi"].lower()
                if "meta_" in codi:
                    parts = codi.split("_")
                    if len(parts) >= 2:
                        meta["source"] = parts[1]
            results.append({
                "document": text,
                "metadata": meta,
                "distance": float(dist),
            })

        return results

    def search(self, query: str, top_k: int = 10,
               source_filter: str = "", tipologia: str = "") -> list[dict]:
        """Cerca hibrida: BM25 + Semantica -> fusio RRF."""

        # Cache check
        ck = self._cache_key(query, top_k, source_filter, tipologia)
        if ck in self._cache:
            ts, cached = self._cache[ck]
            if _time.time() - ts < self._cache_ttl:
                return cached

        fetch_k = max(top_k * 3, 30)

        # 1+2. BM25 + Semantica en paral.lel
        bm25_results = []
        semantic_results = []

        with ThreadPoolExecutor(max_workers=2) as executor:
            futures = {}
            if self._fts_ok:
                futures[executor.submit(
                    self.search_bm25, query, fetch_k
                )] = "bm25"
            futures[executor.submit(
                self.search_semantic, query, fetch_k
            )] = "semantic"

            for future in as_completed(futures):
                name = futures[future]
                try:
                    if name == "bm25":
                        bm25_results = future.result()
                    else:
                        semantic_results = future.result()
                except Exception as exc:
                    print(f"  [WARN] {name} search failed: {exc}")

        # 3. RRF Fusion
        def _result_key(r):
            meta = r.get("metadata", {})
            cid = r.get("chroma_id") or meta.get("chroma_id", "")
            if cid:
                return cid
            text = (r.get("document") or "")[:200]
            return hash(text)

        scores = {}

        for rank, r in enumerate(bm25_results):
            key = _result_key(r)
            rrf = self.WEIGHT_BM25 / (self.RRF_K + rank + 1)
            if key not in scores:
                scores[key] = {"score": 0, "result": r, "methods": set()}
            scores[key]["score"] += rrf
            scores[key]["methods"].add("bm25")

        for rank, r in enumerate(semantic_results):
            key = _result_key(r)
            rrf = self.WEIGHT_SEMANTIC / (self.RRF_K + rank + 1)
            if key not in scores:
                scores[key] = {"score": 0, "result": r, "methods": set()}
            scores[key]["score"] += rrf
            scores[key]["methods"].add("semantic")
            if "bm25" in scores[key]["methods"]:
                scores[key]["score"] *= 1.3

        # ── Boost per jerarquia normativa ──
        for key, item in scores.items():
            r = item["result"]
            doc_titol = (r.get("metadata", {}).get("doc_titol") or "").lower()
            doc_codi = (r.get("metadata", {}).get("doc_codi") or "").lower()

            # Boost alt: Instruccions de Carreteres (IC), CTE DB, ETIs
            if any(x in doc_titol or x in doc_codi for x in [
                "-ic", "instruccion de carreteras", "instruccio de carreteres",
                "cte db", "ehe-08", "iap-11", "pg-3", "pg3",
                "pliego de prescripciones", "reglamento", "reglament",
                "real decreto", "reial decret",
            ]):
                item["score"] *= 1.5

            # Boost moderat: normes tecniques, ordres ministerials
            elif any(x in doc_titol or x in doc_codi for x in [
                "norma", "orden", "ordre", "especificacion tecnica",
                "une-en", "iso ", "directiva", "decret",
            ]):
                item["score"] *= 1.2

            # Penalitzar: llistats de preus, jornades, inventaris
            elif any(x in doc_titol for x in [
                "precio", "preu", "jornades", "jornadas",
                "inventario", "inventari", "demostrativa",
            ]):
                item["score"] *= 0.6

        # 4. Boost per tipologia de projecte
        if tipologia:
            for key, item in scores.items():
                r = item["result"]
                source = (r.get("metadata", {}).get("source") or "").lower()
                multiplier = get_source_multiplier(tipologia, source, query)
                item["score"] *= multiplier

        # 5. Ordenar
        ranked = sorted(scores.values(), key=lambda x: -x["score"])

        # 6. Diversitat de fonts
        source_counts = {}
        final = []

        for item in ranked:
            r = item["result"]
            meta = r.get("metadata", {})
            source = meta.get("source", "").lower()

            if source_filter and source != source_filter.lower():
                continue

            source_counts[source] = source_counts.get(source, 0) + 1
            max_for_source = self.SOURCE_LIMITS.get(source, self.MAX_PER_SOURCE)
            if source_counts[source] > max_for_source:
                continue

            max_score = ranked[0]["score"] if ranked else 1
            norm_score = round((item["score"] / max_score) * 100, 1)

            methods = list(item["methods"])
            final.append({
                "document": r.get("document", ""),
                "metadata": meta,
                "score": norm_score,
                "distance": r.get("distance", 0),
                "methods": methods,
            })

            if len(final) >= top_k:
                break

        # 6. Reranking amb cross-encoder
        if final and len(final) > 1:
            try:
                reranker = self._get_reranker()
                final = reranker.rerank(query, final, top_k=top_k)
            except Exception as exc:
                print(f"  [WARN] Reranker failed: {exc}")

        # Cache store
        self._cache[ck] = (_time.time(), final)
        if len(self._cache) > 50:
            oldest = sorted(self._cache.items(), key=lambda x: x[1][0])[:25]
            for k, _ in oldest:
                del self._cache[k]

        return final


# ── CLI ────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    import config

    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")

    print("=" * 50)
    print("  NormaCat -- Construccio index FTS5 (BM25)")
    print("=" * 50)
    print()

    count = build_fts_index(config.SQLITE_PATH)
    print(f"\n  FTS5 construit: {count:,} chunks indexats")
    print(f"  Base de dades: {config.SQLITE_PATH}")
