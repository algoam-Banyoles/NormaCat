"""
reranker.py — Re-ranking amb cross-encoder multilingue.

Pren els top-N resultats de la cerca hibrida i els reordena
per rellevancia real usant un cross-encoder que veu query i
document junts.

Us:
    from search.reranker import Reranker
    reranker = Reranker()
    reranked = reranker.rerank(query, results, top_k=10)
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sentence_transformers import CrossEncoder

MODEL_NAME = "cross-encoder/ms-marco-MiniLM-L-6-v2"


class Reranker:
    """Cross-encoder reranker per a resultats de cerca."""

    def __init__(self, model_name: str = MODEL_NAME, model_instance=None):
        self._model_name = model_name
        self._model = model_instance

    def _get_model(self) -> CrossEncoder:
        if self._model is None:
            self._model = CrossEncoder(self._model_name)
        return self._model

    def rerank(self, query: str, results: list[dict],
               top_k: int = 10) -> list[dict]:
        """Re-rankeja resultats amb cross-encoder."""
        if not results:
            return []

        model = self._get_model()

        pairs = []
        for r in results:
            text = (r.get("document") or "")[:512]
            pairs.append((query, text))

        scores = model.predict(pairs, show_progress_bar=False)

        for r, score in zip(results, scores):
            r["ce_score"] = float(score)

        results.sort(key=lambda r: r["ce_score"], reverse=True)

        if results:
            max_s = max(r["ce_score"] for r in results)
            min_s = min(r["ce_score"] for r in results)
            span = max_s - min_s if max_s != min_s else 1
            for r in results:
                r["score"] = round(((r["ce_score"] - min_s) / span) * 100, 1)

        return results[:top_k]
