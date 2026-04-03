"""
feedback.py — Recull feedback dels usuaris sobre els resultats de cerca.

Cada feedback es guarda a SQLite per:
  1. Analisi de qualitat de la cerca
  2. Futur ajust de pesos (font, tipus, RRF)
  3. Eventual fine-tuning dels embeddings
"""

import os
import sqlite3
import sys
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import config

FEEDBACK_SCHEMA = """
CREATE TABLE IF NOT EXISTS search_feedback (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp TEXT NOT NULL,
    query TEXT NOT NULL,
    doc_codi TEXT,
    doc_titol TEXT,
    source TEXT,
    page INTEGER,
    rank_position INTEGER,
    score REAL,
    relevant INTEGER NOT NULL,
    methods TEXT,
    text_preview TEXT
)
"""


def _get_conn():
    conn = sqlite3.connect(config.SQLITE_PATH)
    conn.execute(FEEDBACK_SCHEMA)
    conn.commit()
    return conn


def record_feedback(
    query: str,
    doc_codi: str,
    doc_titol: str,
    source: str,
    page: int,
    rank_position: int,
    score: float,
    relevant: bool,
    methods: str = "",
    text_preview: str = "",
) -> int:
    """Guarda un feedback (relevant=True/False). Retorna l'ID."""
    conn = _get_conn()
    cur = conn.execute(
        """INSERT INTO search_feedback
           (timestamp, query, doc_codi, doc_titol, source, page,
            rank_position, score, relevant, methods, text_preview)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            datetime.now().isoformat(),
            query,
            doc_codi or "",
            doc_titol or "",
            source or "",
            page or 0,
            rank_position,
            score or 0,
            1 if relevant else 0,
            methods or "",
            (text_preview or "")[:200],
        ),
    )
    conn.commit()
    feedback_id = cur.lastrowid
    conn.close()
    return feedback_id


def get_feedback_stats() -> dict:
    """Retorna estadistiques del feedback recollit."""
    conn = _get_conn()
    conn.row_factory = sqlite3.Row

    total = conn.execute(
        "SELECT COUNT(*) FROM search_feedback"
    ).fetchone()[0]

    positius = conn.execute(
        "SELECT COUNT(*) FROM search_feedback WHERE relevant = 1"
    ).fetchone()[0]

    negatius = total - positius

    problematic = conn.execute("""
        SELECT query, COUNT(*) as n,
               SUM(CASE WHEN relevant = 0 THEN 1 ELSE 0 END) as neg
        FROM search_feedback
        GROUP BY query
        HAVING neg > 0
        ORDER BY neg DESC
        LIMIT 10
    """).fetchall()

    source_quality = conn.execute("""
        SELECT source,
               COUNT(*) as total,
               SUM(CASE WHEN relevant = 1 THEN 1 ELSE 0 END) as pos,
               SUM(CASE WHEN relevant = 0 THEN 1 ELSE 0 END) as neg
        FROM search_feedback
        WHERE source != ''
        GROUP BY source
        ORDER BY neg DESC
    """).fetchall()

    conn.close()

    return {
        "total": total,
        "positius": positius,
        "negatius": negatius,
        "ratio": round(positius / total * 100, 1) if total > 0 else 0,
        "problematic_queries": [dict(r) for r in problematic],
        "source_quality": [dict(r) for r in source_quality],
    }
