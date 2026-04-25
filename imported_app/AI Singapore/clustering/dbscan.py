"""
clustering/dbscan.py — DBSCAN clustering wrapper.

Uses cosine distance on TF-IDF vectors.
Returns cluster labels (-1 = noise/outlier).
"""

import scipy.sparse


def cluster(matrix: scipy.sparse.csr_matrix) -> list[int]:
    """
    Run DBSCAN on TF-IDF matrix using cosine distance.
    Returns list of integer cluster labels (same length as input).
    Label -1 means noise (no cluster assigned).
    """
    from sklearn.cluster import DBSCAN

    model = DBSCAN(eps=0.3, min_samples=2, metric="cosine", algorithm="brute")
    labels = model.fit_predict(matrix)
    return labels.tolist()
