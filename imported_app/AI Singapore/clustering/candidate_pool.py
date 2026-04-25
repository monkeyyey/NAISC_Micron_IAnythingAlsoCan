"""
clustering/candidate_pool.py — ICL candidate pool for LLM prompt building.

Builds a representative set of log lines via DBSCAN clustering (one example
per cluster, with a second-pass refinement by special-character signature).
At runtime, kNN lookup returns the most similar stored candidates to use as
in-context learning examples.

Persistence:
  candidates.json  — list of representative log line strings
  candidates.npy   — numpy matrix of their TF-IDF vectors (dense)
"""

import json
import logging
import os
import re
from typing import Optional

import numpy as np
import scipy.sparse

from clustering.vectoriser import vectorise, _preprocess
from clustering.dbscan import cluster
from config import CANDIDATE_POOL_DIR

logger = logging.getLogger(__name__)

_SPECIAL_CHARS = set("=:_./[]{}@#")

_CANDIDATES_JSON = "candidates.json"
_CANDIDATES_NPY  = "candidates.npy"


def _special_char_sig(line: str) -> frozenset:
    """Return the set of special characters present in the line."""
    return frozenset(c for c in line if c in _SPECIAL_CHARS)


class CandidatePool:
    """
    In-memory candidate pool with disk persistence.
    """

    def __init__(self, pool_dir: str = CANDIDATE_POOL_DIR):
        self.pool_dir = pool_dir
        self.candidates: list[str] = []
        self.vectors: Optional[np.ndarray] = None   # shape (N, D), dense
        self._vectoriser = None                      # fitted TfidfVectorizer
        self._load()

    # ------------------------------------------------------------------
    # Build
    # ------------------------------------------------------------------

    def build(self, log_lines: list[str]) -> None:
        """
        Build the candidate pool from a list of historical log lines.
        1. TF-IDF vectorise
        2. DBSCAN cluster
        3. Select representative per cluster (+ special-char sub-grouping)
        4. Persist
        """
        if not log_lines:
            logger.warning("CandidatePool.build: empty log_lines, skipping")
            return

        matrix, vectoriser = vectorise(log_lines)
        self._vectoriser = vectoriser

        labels = cluster(matrix)
        n_clusters = max(labels) + 1
        logger.info("CandidatePool: %d clusters from %d lines", n_clusters, len(log_lines))

        # Group indices by cluster label (-1 = noise → each gets own entry)
        by_cluster: dict[int, list[int]] = {}
        for idx, label in enumerate(labels):
            by_cluster.setdefault(label, []).append(idx)

        candidates: list[str] = []

        for label, indices in by_cluster.items():
            cluster_lines = [log_lines[i] for i in indices]

            if label == -1:
                # Noise: add each as its own candidate (they're unique)
                candidates.extend(cluster_lines)
                continue

            # Second-pass: sub-group by special character signature
            by_spec: dict[frozenset, list[str]] = {}
            for line in cluster_lines:
                sig = _special_char_sig(line)
                by_spec.setdefault(sig, []).append(line)

            # Pick one representative per sub-group (the first line)
            for sub_lines in by_spec.values():
                candidates.append(sub_lines[0])

        self.candidates = candidates

        # Vectorise candidates
        if candidates:
            preprocessed = [_preprocess(c) for c in candidates]
            cand_matrix = vectoriser.transform(preprocessed)
            self.vectors = cand_matrix.toarray()
        else:
            self.vectors = np.zeros((0, matrix.shape[1]))

        self._save()
        logger.info("CandidatePool: %d candidates saved", len(candidates))

    # ------------------------------------------------------------------
    # Query
    # ------------------------------------------------------------------

    def get_similar(self, query_line: str, k: int = 3) -> list[str]:
        """
        Return the top-k most similar candidate lines to query_line.
        Uses cosine similarity on TF-IDF vectors.
        Returns empty list if pool is empty or vectoriser not available.
        """
        if not self.candidates or self.vectors is None or self._vectoriser is None:
            return []

        preprocessed = _preprocess(query_line)
        query_vec = self._vectoriser.transform([preprocessed]).toarray()[0]

        # Cosine similarity
        norms_cands = np.linalg.norm(self.vectors, axis=1)
        norm_query = np.linalg.norm(query_vec)

        if norm_query == 0 or len(norms_cands) == 0:
            return self.candidates[:k]

        # Avoid division by zero
        safe_norms = np.where(norms_cands == 0, 1e-10, norms_cands)
        similarities = self.vectors.dot(query_vec) / (safe_norms * norm_query)

        top_k = min(k, len(self.candidates))
        top_indices = np.argpartition(similarities, -top_k)[-top_k:]
        top_indices = top_indices[np.argsort(similarities[top_indices])[::-1]]

        return [self.candidates[i] for i in top_indices]

    # ------------------------------------------------------------------
    # Persistence
    # ------------------------------------------------------------------

    def _save(self) -> None:
        import joblib
        os.makedirs(self.pool_dir, exist_ok=True)
        json_path = os.path.join(self.pool_dir, _CANDIDATES_JSON)
        npy_path  = os.path.join(self.pool_dir, _CANDIDATES_NPY)
        vec_path  = os.path.join(self.pool_dir, "vectoriser.joblib")

        with open(json_path, "w", encoding="utf-8") as fh:
            json.dump(self.candidates, fh, ensure_ascii=False, indent=2)

        if self.vectors is not None and len(self.vectors) > 0:
            np.save(npy_path, self.vectors)

        if self._vectoriser is not None:
            joblib.dump(self._vectoriser, vec_path)

    def _load(self) -> None:
        import joblib
        json_path = os.path.join(self.pool_dir, _CANDIDATES_JSON)
        npy_path  = os.path.join(self.pool_dir, _CANDIDATES_NPY)
        vec_path  = os.path.join(self.pool_dir, "vectoriser.joblib")

        if not os.path.exists(json_path):
            return

        try:
            with open(json_path, "r", encoding="utf-8") as fh:
                self.candidates = json.load(fh)
        except (OSError, json.JSONDecodeError) as exc:
            logger.warning("Could not load candidates: %s", exc)
            return

        if os.path.exists(npy_path):
            try:
                self.vectors = np.load(npy_path)
            except Exception as exc:
                logger.warning("Could not load candidate vectors: %s", exc)

        if os.path.exists(vec_path):
            try:
                self._vectoriser = joblib.load(vec_path)
            except Exception as exc:
                logger.warning("Could not reload vectoriser: %s", exc)
