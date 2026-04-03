from __future__ import annotations

import hashlib
import json
import os
import re
import signal
import sqlite3
import subprocess
import sys
import threading
import unicodedata
from datetime import datetime
from pathlib import Path

import chromadb
import fitz
from docx import Document
from sentence_transformers import SentenceTransformer


BASE_DIR = Path(__file__).resolve().parent.parent

# Carpetes de descàrregues a indexar (PDFs/DOCX)
DOWNLOADS_DIR = BASE_DIR / "downloads"
NORMATIVA_FOLDERS = [
    str(DOWNLOADS_DIR / name)
    for name in [
        "adif", "dgc", "industria", "rebt_rite",
        "aca", "pjcat", "boe", "cte", "territori",
        "mitma_ferroviari", "eurlex",
    ]
]
DB_PATH = str(BASE_DIR / "db" / "normativa.db")
CHROMA_PATH = str(BASE_DIR / "db" / "chroma_db")
CHUNK_SIZE = 800
CHUNK_OVERLAP = 150
EMBEDDING_MODEL = "paraphrase-multilingual-MiniLM-L12-v2"
CHROMA_BATCH_SIZE = 500  # conservative to avoid ChromaDB max batch limit (~5461)

SCHEMA = [
    """
    CREATE TABLE IF NOT EXISTS documents (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        filename TEXT NOT NULL,
        codi TEXT,
        titol TEXT,
        tipus TEXT,
        any_aprovacio INTEGER,
        vigent INTEGER DEFAULT 1,
        data_indexat TEXT,
        num_chunks INTEGER,
        file_hash TEXT
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS chunks (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        doc_id INTEGER REFERENCES documents(id),
        chunk_index INTEGER,
        text TEXT NOT NULL,
        page_num INTEGER,
        chroma_id TEXT
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS articles (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        doc_id INTEGER REFERENCES documents(id),
        article_num TEXT,
        article_title TEXT,
        chunk_id INTEGER REFERENCES chunks(id)
    )
    """,
]

ARTICLE_HEADER_RE = re.compile(r"^(?:Article|Articulo|Articulo|Art\.)\s+\d+[a-z]?(?:\.\d+)?\b", re.IGNORECASE)
ARTICLE_SCAN_RE = re.compile(
    r"(?:Article|Articulo|Art\.)\s+(\d+[a-z]?(?:\.\d+)?)\s*[.\-–]?\s*([^\n]{0,80})",
    re.IGNORECASE,
)

_MODEL: SentenceTransformer | None = None
_LAST_INDEX_RESULT: dict = {}


PDF_TIMEOUT = 60  # segons màxim per extreure text d'un PDF
MAX_PDF_SIZE_MB = 30  # saltar PDFs més grans (pengen MuPDF)


def extract_text_from_file(filepath: str) -> list[dict]:
    path = Path(filepath)
    suffix = path.suffix.lower()
    pages = []

    if suffix == ".pdf":
        doc = fitz.open(str(path))
        fitz.TOOLS.mupdf_warnings()  # discard warnings from open (e.g. ICC profile)
        try:
            for index, page in enumerate(doc, 1):
                pages.append({"text": page.get_text() or "", "page": index})
        finally:
            fitz.TOOLS.mupdf_warnings()  # discard any remaining warnings
            doc.close()
        return pages

    if suffix == ".docx":
        document = Document(str(path))
        for para in document.paragraphs:
            text = para.text.strip()
            if text:
                pages.append({"text": text, "page": 0})
        return pages

    raise ValueError(f"Format no suportat: {path.suffix}")


def detect_document_metadata(filename: str, text: str) -> dict:
    sample = f"{filename}\n{text[:2000]}"
    sample_norm = _norm(sample)

    patterns = [
        ("RD", re.compile(r"\b(?:rd|reial\s+decret|real\s+decreto)\s+(\d+/\d{4})\b", re.IGNORECASE)),
        ("Llei", re.compile(r"\b(?:llei|ley)\s+(\d+/\d{4})\b", re.IGNORECASE)),
        ("Decret", re.compile(r"\b(?:decret|decreto)\s+(\d+/\d{4})\b", re.IGNORECASE)),
        ("UNE", re.compile(r"\b(?:une(?:-en)?(?:\s+en)?)\s*([0-9][0-9A-Z./-]*)\b", re.IGNORECASE)),
        ("Ordre", re.compile(r"\b(?:ordre|orden|instruccio|instruccion)\s+([A-Z0-9./-]+(?:/\d{4})?)\b", re.IGNORECASE)),
    ]

    tipus = None
    codi = None
    for doc_type, pattern in patterns:
        match = pattern.search(sample)
        if match:
            tipus = doc_type
            codi = _clean_code(match.group(1) if match.groups() else match.group(0), doc_type)
            break

    if codi is None:
        generic = re.search(r"\b(\d+/\d{4})\b", sample)
        if generic:
            codi = generic.group(1)

    any_aprovacio = None
    year_match = re.search(r"\d+/(\d{4})\b", sample)
    if year_match:
        any_aprovacio = int(year_match.group(1))

    title = _detect_title(text)
    vigent = 1
    catalog = _load_norm_catalog()
    if codi:
        code_norm = _norm(codi)
        if code_norm in catalog["derogada_aliases"]:
            vigent = 0
    if vigent and re.search(r"\bderogad[ao]\b", sample_norm):
        vigent = 0

    return {
        "codi": codi,
        "titol": title,
        "tipus": tipus,
        "any_aprovacio": any_aprovacio,
        "vigent": vigent,
    }


def chunk_text(pages: list[dict], chunk_size: int, overlap: int) -> list[dict]:
    paragraphs = _paragraphs_from_pages(pages)
    chunks = []
    current_parts: list[dict] = []
    current_len = 0

    def flush_chunk() -> None:
        nonlocal current_parts, current_len
        if not current_parts:
            return

        text = "\n\n".join(part["text"] for part in current_parts if part.get("text"))
        page = current_parts[0].get("page", 0)
        chunks.append(
            {
                "text": text,
                "page": page,
                "chunk_index": len(chunks),
            }
        )

        tail = text[-overlap:].strip() if overlap > 0 else ""
        current_parts = [{"text": tail, "page": page}] if tail else []
        current_len = len(tail)

    for para in paragraphs:
        para_text = para["text"].strip()
        if not para_text:
            continue

        if len(para_text) > chunk_size:
            if current_parts:
                flush_chunk()
            for subtext in _split_long_paragraph(para_text, chunk_size, overlap):
                chunks.append(
                    {
                        "text": subtext,
                        "page": para["page"],
                        "chunk_index": len(chunks),
                    }
                )
            continue

        proposed = current_len + len(para_text) + (2 if current_parts else 0)
        is_header = bool(ARTICLE_HEADER_RE.match(para_text))

        if proposed > chunk_size and current_parts:
            flush_chunk()

        if is_header and current_parts and current_len > chunk_size * 0.6:
            flush_chunk()

        current_parts.append({"text": para_text, "page": para["page"]})
        current_len = current_len + len(para_text) + (2 if current_len else 0)

    flush_chunk()
    return chunks


def detect_articles(chunks: list[dict], doc_id: int) -> list[dict]:
    articles = []
    for chunk in chunks:
        text = chunk.get("text", "")
        chunk_id = chunk.get("chunk_id", chunk.get("db_chunk_id", chunk.get("chunk_index")))
        for match in ARTICLE_SCAN_RE.finditer(text):
            article_num = (match.group(1) or "").strip()
            article_title = (match.group(2) or "").strip(" .-–")
            if not article_num:
                continue
            articles.append(
                {
                    "doc_id": doc_id,
                    "article_num": article_num,
                    "article_title": article_title,
                    "chunk_id": chunk_id,
                }
            )
    return articles


def _upsert_in_batches(
    collection,
    ids: list[str],
    embeddings: list,
    documents: list[str],
    metadatas: list[dict],
    batch_size: int = CHROMA_BATCH_SIZE,
) -> int:
    """Upsert to ChromaDB in batches to avoid the max batch size limit."""
    total = len(ids)
    upserted = 0
    for i in range(0, total, batch_size):
        end = min(i + batch_size, total)
        try:
            collection.upsert(
                ids=ids[i:end],
                embeddings=embeddings[i:end],
                documents=documents[i:end],
                metadatas=metadatas[i:end],
            )
            upserted += end - i
        except Exception as exc:
            print(f"  ERROR upsert batch {i}-{end}: {type(exc).__name__}: {exc}")
    return upserted


def _extract_with_timeout(filepath: str, timeout: int = PDF_TIMEOUT) -> list[dict]:
    """Extreu text amb timeout real via subprocess (MuPDF bloqueja el GIL)."""
    import subprocess, tempfile
    script = (
        "import json, sys, io, fitz\n"
        "sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')\n"
        "doc = fitz.open(sys.argv[1])\n"
        "pages = []\n"
        "for i, page in enumerate(doc, 1):\n"
        "    pages.append({'text': page.get_text() or '', 'page': i})\n"
        "doc.close()\n"
        "print(json.dumps(pages, ensure_ascii=False))\n"
    )
    try:
        result = subprocess.run(
            [sys.executable, "-c", script, str(filepath)],
            capture_output=True, text=True, timeout=timeout,
            encoding="utf-8", errors="replace",
        )
        if result.returncode != 0:
            raise RuntimeError(f"subprocess error: {result.stderr[:200]}")
        pages = json.loads(result.stdout)
        return pages
    except subprocess.TimeoutExpired:
        raise TimeoutError(f"Extraccio de text supera {timeout}s: {Path(filepath).name}")
    except json.JSONDecodeError:
        raise RuntimeError(f"JSON invalid de subprocess per {Path(filepath).name}")


def index_document(filepath: str, collection, conn) -> bool:
    global _LAST_INDEX_RESULT

    path = Path(filepath)
    filename = str(path)
    file_hash = _md5_file(path)
    cur = conn.cursor()
    row = cur.execute(
        "SELECT id, file_hash FROM documents WHERE filename = ?",
        (filename,),
    ).fetchone()

    # Saltar PDFs massa grans que pengen MuPDF
    file_size_mb = path.stat().st_size / (1024 * 1024)
    if file_size_mb > MAX_PDF_SIZE_MB:
        _LAST_INDEX_RESULT = {"status": "skipped_large", "filename": filename, "chunks": 0, "articles": 0}
        print(f"  [SKIP] {path.name} ({file_size_mb:.0f} MB > {MAX_PDF_SIZE_MB} MB)")
        return False

    if row and row[1] == file_hash:
        _LAST_INDEX_RESULT = {
            "status": "skipped",
            "filename": filename,
            "chunks": 0,
            "articles": 0,
        }
        return False

    if row:
        _delete_existing_document(conn, collection, row[0])

    pages = _extract_with_timeout(filepath)
    full_text = "\n\n".join(page.get("text", "") for page in pages)
    metadata = detect_document_metadata(path.name, full_text)
    chunks = chunk_text(pages, CHUNK_SIZE, CHUNK_OVERLAP)

    # Guard: skip documents with no extractable text
    chunk_texts = [c["text"] for c in chunks]
    if not chunk_texts or not any(t.strip() for t in chunk_texts):
        _LAST_INDEX_RESULT = {
            "status": "empty",
            "filename": filename,
            "chunks": 0,
            "articles": 0,
        }
        return False

    cur.execute(
        """
        INSERT INTO documents (
            filename, codi, titol, tipus, any_aprovacio, vigent,
            data_indexat, num_chunks, file_hash
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            filename,
            metadata.get("codi"),
            metadata.get("titol"),
            metadata.get("tipus"),
            metadata.get("any_aprovacio"),
            metadata.get("vigent", 1),
            datetime.utcnow().isoformat(timespec="seconds"),
            len(chunks),
            file_hash,
        ),
    )
    doc_id = cur.lastrowid

    embeddings = _get_embedding_model().encode(
        [chunk["text"] for chunk in chunks],
        show_progress_bar=False,
        convert_to_numpy=True,
    )

    # Filter out chunks with empty text or empty embeddings
    valid_indices = [
        i for i, (c, e) in enumerate(zip(chunks, embeddings))
        if c["text"].strip() and len(e) > 0
    ]
    if not valid_indices:
        _LAST_INDEX_RESULT = {
            "status": "empty",
            "filename": filename,
            "chunks": 0,
            "articles": 0,
        }
        return False

    chunks = [chunks[i] for i in valid_indices]
    embeddings = embeddings[valid_indices]

    chroma_ids = [f"doc_{doc_id}_chunk_{chunk['chunk_index']}" for chunk in chunks]
    metadatas = [
        {
            "doc_id": doc_id,
            "doc_codi": metadata.get("codi") or "",
            "doc_titol": metadata.get("titol") or path.name,
            "page": int(chunk.get("page", 0) or 0),
            "chunk_index": int(chunk["chunk_index"]),
            "vigent": int(metadata.get("vigent", 1)),
        }
        for chunk in chunks
    ]

    _upsert_in_batches(
        collection,
        ids=chroma_ids,
        embeddings=embeddings.tolist(),
        documents=[chunk["text"] for chunk in chunks],
        metadatas=metadatas,
    )

    for chunk, chroma_id in zip(chunks, chroma_ids):
        cur.execute(
            "INSERT INTO chunks (doc_id, chunk_index, text, page_num, chroma_id) VALUES (?, ?, ?, ?, ?)",
            (
                doc_id,
                chunk["chunk_index"],
                chunk["text"],
                chunk.get("page", 0),
                chroma_id,
            ),
        )
        chunk["chunk_id"] = cur.lastrowid

    articles = detect_articles(chunks, doc_id)
    for article in articles:
        cur.execute(
            "INSERT INTO articles (doc_id, article_num, article_title, chunk_id) VALUES (?, ?, ?, ?)",
            (
                article["doc_id"],
                article["article_num"],
                article["article_title"],
                article["chunk_id"],
            ),
        )

    conn.commit()
    label = metadata.get("codi") or path.name
    print(f"  [OK] {label} -> {len(chunks)} chunks, {len(articles)} articles")

    _LAST_INDEX_RESULT = {
        "status": "indexed",
        "filename": filename,
        "doc_id": doc_id,
        "chunks": len(chunks),
        "articles": len(articles),
    }
    return True


def index_folder(folder: str) -> dict:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        _init_db(conn)
        client = chromadb.PersistentClient(path=CHROMA_PATH)
        collection = client.get_or_create_collection(name="normativa")

        indexed_docs = 0
        skipped_docs = 0
        empty_docs = 0
        errors = 0
        chunk_total = 0
        article_total = 0

        for path in sorted(Path(folder).rglob("*")):
            if not path.is_file() or path.suffix.lower() not in {".pdf", ".docx"}:
                continue

            try:
                indexed = index_document(str(path), collection, conn)
                if indexed:
                    indexed_docs += 1
                    chunk_total += int(_LAST_INDEX_RESULT.get("chunks", 0))
                    article_total += int(_LAST_INDEX_RESULT.get("articles", 0))
                elif _LAST_INDEX_RESULT.get("status") == "empty":
                    empty_docs += 1
                    print(f"  [WARN] {path.name}: sense text extraible, omes")
                else:
                    skipped_docs += 1
            except Exception as exc:
                errors += 1
                conn.rollback()
                print(f"ERROR indexant {path.name}: {type(exc).__name__}: {exc}")

        print(f"\n--- Resum ---")
        print(f"Documents indexats: {indexed_docs} | {chunk_total:,} chunks | {article_total:,} articles")
        print(f"Ja indexats (sense canvis): {skipped_docs}")
        print(f"Documents omesos (sense text): {empty_docs}")
        print(f"Errors: {errors}")

        return {
            "indexed": indexed_docs,
            "skipped": skipped_docs,
            "empty": empty_docs,
            "errors": errors,
            "chunks": chunk_total,
            "articles": article_total,
        }
    finally:
        conn.close()


def _init_db(conn: sqlite3.Connection) -> None:
    for statement in SCHEMA:
        conn.execute(statement)
    conn.commit()


def _delete_existing_document(conn: sqlite3.Connection, collection, doc_id: int) -> None:
    cur = conn.cursor()
    chroma_ids = [
        row[0]
        for row in cur.execute("SELECT chroma_id FROM chunks WHERE doc_id = ? AND chroma_id IS NOT NULL", (doc_id,))
    ]
    for i in range(0, len(chroma_ids), CHROMA_BATCH_SIZE):
        try:
            collection.delete(ids=chroma_ids[i:i + CHROMA_BATCH_SIZE])
        except Exception:
            pass

    cur.execute("DELETE FROM articles WHERE doc_id = ?", (doc_id,))
    cur.execute("DELETE FROM chunks WHERE doc_id = ?", (doc_id,))
    cur.execute("DELETE FROM documents WHERE id = ?", (doc_id,))
    conn.commit()


def _paragraphs_from_pages(pages: list[dict]) -> list[dict]:
    paragraphs = []
    for page in pages:
        text = (page.get("text") or "").replace("\r", "")
        raw_parts = [part.strip() for part in re.split(r"\n\s*\n", text) if part.strip()]
        if not raw_parts:
            raw_parts = [line.strip() for line in text.splitlines() if line.strip()]
        for part in raw_parts:
            paragraphs.append({"text": part, "page": page.get("page", 0)})
    return paragraphs


def _split_long_paragraph(text: str, chunk_size: int, overlap: int) -> list[str]:
    parts = []
    remaining = text.strip()
    while len(remaining) > chunk_size:
        cut = max(0, remaining.rfind(" ", 0, chunk_size))
        if cut < chunk_size * 0.6:
            cut = chunk_size
        part = remaining[:cut].strip()
        parts.append(part)
        remaining = remaining[max(0, cut - overlap):].strip()
    if remaining:
        parts.append(remaining)
    return parts


def _get_embedding_model() -> SentenceTransformer:
    global _MODEL
    if _MODEL is None:
        _MODEL = SentenceTransformer(EMBEDDING_MODEL)
    return _MODEL


def _md5_file(path: Path) -> str:
    hasher = hashlib.md5()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            hasher.update(chunk)
    return hasher.hexdigest()


def _detect_title(text: str) -> str:
    for line in (text or "").splitlines():
        clean = re.sub(r"\s+", " ", line).strip()
        if len(clean) >= 12:
            return clean[:300]
    return ""


def _load_norm_catalog() -> dict:
    json_path = BASE_DIR / "data" / "normativa_annexes.json"
    derogada_aliases: set[str] = set()
    if not json_path.exists():
        return {"derogada_aliases": derogada_aliases}

    try:
        data = json.loads(json_path.read_text(encoding="utf-8"))
    except Exception:
        return {"derogada_aliases": derogada_aliases}

    for entry in data.get("normativa_derogada", []):
        for blob in [entry.get("codi", ""), entry.get("text", "")]:
            alias = _norm(blob)
            if alias:
                derogada_aliases.add(alias)
            num = _extract_numeric_code(blob)
            if num:
                derogada_aliases.add(num)

    return {"derogada_aliases": derogada_aliases}


def _clean_code(text: str, tipus: str | None) -> str:
    raw = re.sub(r"\s+", " ", text or "").strip(" .,-")
    num = _extract_numeric_code(raw)
    if tipus == "UNE":
        return raw.upper().replace(" ", "-")
    if tipus and num:
        prefix = {"RD": "RD", "Llei": "Llei", "Decret": "Decret", "Ordre": "Ordre"}.get(tipus, tipus)
        return f"{prefix} {num}"
    return raw


def _extract_numeric_code(text: str) -> str | None:
    match = re.search(r"\b(\d+/\d{4})\b", text or "", re.IGNORECASE)
    return match.group(1) if match else None


def _norm(text: str) -> str:
    base = unicodedata.normalize("NFKD", text or "")
    base = "".join(ch for ch in base if not unicodedata.combining(ch))
    base = base.lower()
    base = re.sub(r"\s+", " ", base)
    return base.strip()


# ── Classe NormIndexer per a cli.py ──────────────────────────────────────────

class NormIndexer:
    """Façana que encapsula l'indexació i la cerca semàntica."""

    def __init__(
        self,
        chroma_path: str | None = None,
        sqlite_path: str | None = None,
        embedding_model: str | None = None,
        chunk_size: int | None = None,
        chunk_overlap: int | None = None,
    ):
        global CHUNK_SIZE, CHUNK_OVERLAP, EMBEDDING_MODEL, CHROMA_PATH, DB_PATH
        if chroma_path:
            CHROMA_PATH = chroma_path
        if sqlite_path:
            DB_PATH = sqlite_path
        if embedding_model:
            EMBEDDING_MODEL = embedding_model
        if chunk_size:
            CHUNK_SIZE = chunk_size
        if chunk_overlap:
            CHUNK_OVERLAP = chunk_overlap

        self._client = chromadb.PersistentClient(path=CHROMA_PATH)
        self._collection = self._client.get_or_create_collection(name="normativa")
        self._conn = sqlite3.connect(DB_PATH)
        self._conn.row_factory = sqlite3.Row
        _init_db(self._conn)

    # ── Indexació per catàleg JSON ────────────────────────────────────────────

    def index_catalog(self, catalog_path: str, source: str = "") -> dict:
        """Indexa els PDFs referenciats en un catàleg JSON."""
        catalog_path = Path(catalog_path)
        if not catalog_path.exists():
            print(f"  [SKIP] Catàleg no trobat: {catalog_path}")
            return {"indexed": 0, "skipped": 0, "errors": 0}

        with open(catalog_path, encoding="utf-8") as fh:
            data = json.load(fh)

        # Extreure llista de documents (format varia per font)
        if isinstance(data, list):
            entries = data
        elif isinstance(data, dict):
            entries = (
                data.get("documents")
                or data.get("entries")
                or data.get("normes")
                or data.get("data")
                or []
            )
            if not isinstance(entries, list):
                entries = []
        else:
            entries = []

        # Camps possibles on trobar el path local del PDF
        path_fields = [
            "fitxer_local", "path_local", "pdf_local", "url_local",
        ]

        indexed = 0
        skipped = 0
        errors = 0
        empty = 0

        for entry in entries:
            # Trobar el path local del PDF
            pdf_rel = ""
            for pf in path_fields:
                v = entry.get(pf) or ""
                if v and v != "None":
                    pdf_rel = v
                    break

            if not pdf_rel:
                continue

            # Construir path absolut
            pdf_path = Path(pdf_rel)
            if not pdf_path.is_absolute():
                pdf_path = BASE_DIR / pdf_rel

            if not pdf_path.exists():
                continue
            if pdf_path.suffix.lower() not in {".pdf", ".docx"}:
                continue

            try:
                ok = index_document(str(pdf_path), self._collection, self._conn)
                if ok:
                    indexed += 1
                elif _LAST_INDEX_RESULT.get("status") == "empty":
                    empty += 1
                else:
                    skipped += 1
            except Exception as exc:
                errors += 1
                self._conn.rollback()
                print(f"  ERROR indexant {pdf_path.name}: {type(exc).__name__}: {exc}")

        # Fallback: si el catàleg no tenia paths locals, indexa la carpeta downloads/<source>/
        if indexed == 0 and skipped == 0 and errors == 0 and source:
            folder = DOWNLOADS_DIR / source
            if folder.is_dir():
                pdf_files = list(folder.rglob("*.pdf")) + list(folder.rglob("*.docx"))
                if pdf_files:
                    print(f"  [{source}] Catàleg sense paths locals, indexant carpeta {folder.name}/ ({len(pdf_files)} fitxers)...")
                    for pdf_path in sorted(pdf_files):
                        try:
                            ok = index_document(str(pdf_path), self._collection, self._conn)
                            if ok:
                                indexed += 1
                            elif _LAST_INDEX_RESULT.get("status") == "empty":
                                empty += 1
                            else:
                                skipped += 1
                        except Exception as exc:
                            errors += 1
                            self._conn.rollback()
                            print(f"  ERROR indexant {pdf_path.name}: {type(exc).__name__}: {exc}")

        print(f"  [{source}] {indexed} indexats, {skipped} ja existents, "
              f"{empty} sense text, {errors} errors")
        return {"indexed": indexed, "skipped": skipped, "empty": empty, "errors": errors}

    # ── Cerca semàntica ──────────────────────────────────────────────────────

    def search(self, query: str, n_results: int = 5) -> list[dict]:
        """Cerca semàntica contra ChromaDB. Retorna llista de resultats."""
        model = _get_embedding_model()
        query_embedding = model.encode([query], convert_to_numpy=True).tolist()

        results = self._collection.query(
            query_embeddings=query_embedding,
            n_results=n_results,
            include=["documents", "metadatas", "distances"],
        )

        output = []
        docs = results.get("documents", [[]])[0]
        metas = results.get("metadatas", [[]])[0]
        dists = results.get("distances", [[]])[0]

        for doc, meta, dist in zip(docs, metas, dists):
            output.append({
                "document": doc,
                "metadata": {
                    "source": meta.get("doc_codi", ""),
                    "title": meta.get("doc_titol", ""),
                    "reference": meta.get("doc_codi", ""),
                    "page": meta.get("page", 0),
                    "vigent": meta.get("vigent", 1),
                },
                "distance": dist,
            })

        return output

    def close(self):
        if self._conn:
            self._conn.close()


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Indexa documents normatius a ChromaDB + SQLite")
    parser.add_argument("folder", nargs="?", default=None,
                        help="Carpeta a indexar. Si no s'indica, indexa totes les de NORMATIVA_FOLDERS")
    parser.add_argument("--reset", action="store_true",
                        help="Esborra i recrea la col\u00b7leccio ChromaDB abans d'indexar")
    args = parser.parse_args()

    if args.reset:
        try:
            client = chromadb.PersistentClient(path=CHROMA_PATH)
            client.delete_collection("normativa")
            print("[INDEX] Col\u00b7leccio ChromaDB esborrada i recreada.")
        except Exception:
            pass

    if args.folder:
        index_folder(args.folder)
    else:
        totals: dict[str, int] = {
            "indexed": 0, "skipped": 0, "empty": 0, "errors": 0,
            "chunks": 0, "articles": 0,
        }
        for folder in NORMATIVA_FOLDERS:
            if not os.path.isdir(folder):
                print(f"[SKIP] {folder} (no existeix)")
                continue
            print(f"\n--- Indexant {folder} ---")
            result = index_folder(folder)
            for k in totals:
                totals[k] += result.get(k, 0)
        print(
            f"\n{'='*50}"
            f"\nTotal: {totals['indexed']} indexats, "
            f"{totals['skipped']} sense canvis, "
            f"{totals['empty']} omesos (sense text), "
            f"{totals['errors']} errors, "
            f"{totals['chunks']:,} chunks"
        )