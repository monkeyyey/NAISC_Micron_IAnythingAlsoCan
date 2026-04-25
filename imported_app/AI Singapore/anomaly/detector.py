"""
anomaly/detector.py — Isolation Forest anomaly detection on measurement fields.

Graceful degradation:
  - If not enough data to fit (< 10 records with non-None feature values),
    score() returns 0.0 without raising.
  - If the model file doesn't exist, fit() is called on the first batch.

Scores:
  IsolationForest.score_samples() returns negative anomaly scores.
  We normalise to [-1, 1]: -1 = most anomalous, +1 = most normal.
  Records with score < -0.5 get "anomaly_detected" added to parse_flags.
"""

import logging
import os
from typing import Optional

import joblib
import numpy as np

from config import ANOMALY_MODEL_PATH

logger = logging.getLogger(__name__)

ANOMALY_FLAG_THRESHOLD = -0.5
MIN_FIT_SAMPLES = 10

FEATURE_FIELDS = ["temperature", "pressure", "rf_power", "flow_rate"]


def _extract_features(records: list[dict]) -> tuple[np.ndarray, list[int]]:
    """
    Extract feature matrix from records.
    Returns (feature_matrix, valid_indices) where valid_indices are the
    record indices that had at least one non-None feature value.
    """
    rows = []
    valid_indices = []

    for i, rec in enumerate(records):
        vals = [rec.get(f) for f in FEATURE_FIELDS]
        if all(v is None for v in vals):
            continue
        # Replace None with column mean later — use 0.0 as placeholder
        row = [float(v) if v is not None else np.nan for v in vals]
        rows.append(row)
        valid_indices.append(i)

    if not rows:
        return np.empty((0, len(FEATURE_FIELDS))), []

    matrix = np.array(rows, dtype=np.float64)

    # Impute NaN with column mean
    col_means = np.nanmean(matrix, axis=0)
    col_means = np.nan_to_num(col_means, nan=0.0)
    inds = np.where(np.isnan(matrix))
    matrix[inds] = np.take(col_means, inds[1])

    return matrix, valid_indices


class AnomalyDetector:
    """
    Isolation Forest anomaly detector.
    Persists model to disk using joblib.
    """

    def __init__(self, model_path: str = ANOMALY_MODEL_PATH):
        self.model_path = model_path
        self.is_fitted = False
        self.feature_fields = FEATURE_FIELDS
        self._model = None

        self._load()

    # ------------------------------------------------------------------
    # Training
    # ------------------------------------------------------------------

    def fit(self, records: list[dict]) -> None:
        """
        Train on a batch of records.
        Silently skips if too few records have feature data.
        """
        from sklearn.ensemble import IsolationForest

        matrix, valid_indices = _extract_features(records)

        if len(valid_indices) < MIN_FIT_SAMPLES:
            logger.info(
                "AnomalyDetector.fit: only %d valid records (need %d) — skipping fit",
                len(valid_indices), MIN_FIT_SAMPLES,
            )
            return

        self._model = IsolationForest(contamination=0.05, random_state=42)
        self._model.fit(matrix)
        self.is_fitted = True
        self._save()
        logger.info("AnomalyDetector: fitted on %d records", len(valid_indices))

    # ------------------------------------------------------------------
    # Scoring
    # ------------------------------------------------------------------

    def score(self, record: dict) -> float:
        """
        Score a single record.
        Returns float in [-1, 1]; -1 most anomalous, +1 most normal.
        Returns 0.0 if model not fitted or all feature fields are None.
        """
        if not self.is_fitted or self._model is None:
            return 0.0

        matrix, valid = _extract_features([record])
        if not valid:
            return 0.0

        # score_samples returns negative values; more negative = more anomalous
        raw_score = self._model.score_samples(matrix)[0]

        # Normalise: score_samples is typically in range [-0.5, 0.2]
        # Map to [-1, 1] by clamping then scaling
        normalised = max(-1.0, min(1.0, raw_score * 2.0))
        return float(normalised)

    def score_batch(self, records: list[dict]) -> list[float]:
        """
        Score a batch of records. Returns a list of floats.
        """
        if not self.is_fitted or self._model is None:
            return [0.0] * len(records)

        scores = []
        matrix, valid_indices = _extract_features(records)

        if not valid_indices:
            return [0.0] * len(records)

        raw_scores = self._model.score_samples(matrix)
        normalised = [max(-1.0, min(1.0, s * 2.0)) for s in raw_scores]

        # Map back to original record indices
        score_map = {idx: normalised[i] for i, idx in enumerate(valid_indices)}
        scores = [score_map.get(i, 0.0) for i in range(len(records))]
        return scores

    def annotate_records(self, records: list[dict]) -> list[dict]:
        """
        Add anomaly_score to each record and flag anomalies in parse_flags.
        Modifies records in-place and returns them.
        """
        batch_scores = self.score_batch(records)
        for record, score in zip(records, batch_scores):
            record["anomaly_score"] = score
            if score < ANOMALY_FLAG_THRESHOLD:
                if "parse_flags" not in record or record["parse_flags"] is None:
                    record["parse_flags"] = []
                if "anomaly_detected" not in record["parse_flags"]:
                    record["parse_flags"].append("anomaly_detected")
        return records

    # ------------------------------------------------------------------
    # Persistence
    # ------------------------------------------------------------------

    def _save(self) -> None:
        try:
            joblib.dump(self._model, self.model_path)
            logger.debug("AnomalyDetector: model saved to %s", self.model_path)
        except Exception as exc:
            logger.error("Could not save anomaly model: %s", exc)

    def _load(self) -> None:
        if not os.path.exists(self.model_path):
            return
        try:
            self._model = joblib.load(self.model_path)
            self.is_fitted = True
            logger.info("AnomalyDetector: loaded model from %s", self.model_path)
        except Exception as exc:
            logger.warning("Could not load anomaly model from %s: %s", self.model_path, exc)
            self._model = None
            self.is_fitted = False
