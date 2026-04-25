"""
clustering/vectoriser.py — TF-IDF vectorisation for log lines.

Numeric tokens are removed before vectorisation because they are variable
and would give false similarity signals.  Stop words are removed.
"""

import re
from typing import Optional

import scipy.sparse

_NUM_RE = re.compile(r"\b\d+(?:\.\d+)?\b")
_SYSLOG_STOP = {
    "the", "a", "an", "is", "are", "was", "were", "be", "been", "being",
    "to", "of", "in", "for", "on", "with", "at", "by", "from", "as",
}


def _preprocess(line: str) -> str:
    """Remove numbers and stop words before vectorisation."""
    line = _NUM_RE.sub("", line.lower())
    tokens = line.split()
    tokens = [t for t in tokens if t not in _SYSLOG_STOP and len(t) > 1]
    return " ".join(tokens)


def vectorise(log_lines: list[str]) -> "tuple[scipy.sparse.csr_matrix, object]":
    """
    Vectorise a list of log lines using TF-IDF.

    Returns (matrix, fitted_vectoriser).
    The fitted vectoriser is returned so it can be used to transform new queries.
    """
    from sklearn.feature_extraction.text import TfidfVectorizer

    preprocessed = [_preprocess(l) for l in log_lines]
    vectoriser = TfidfVectorizer(
        analyzer="word",
        token_pattern=r"[A-Za-z_]\w*",  # letters + underscore only
        min_df=1,
        max_df=0.95,
        sublinear_tf=True,
    )
    matrix = vectoriser.fit_transform(preprocessed)
    return matrix, vectoriser
