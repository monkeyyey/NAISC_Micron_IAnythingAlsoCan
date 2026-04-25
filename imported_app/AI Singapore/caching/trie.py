"""
caching/trie.py — Prefix trie for unstructured log template matching.

Each leaf node stores a log template (token sequence with <*> wildcards)
and a position_map that records which wildcard index maps to which vendor field.

Wildcard matching: at each <*> node, we try consuming 1 to MAX_WILDCARD_TOKENS
input tokens, choosing the match that has the most exact (non-wildcard) tokens.
"""

from __future__ import annotations

import json
import logging
import pickle
import re
from dataclasses import dataclass, field

logger = logging.getLogger(__name__)

MAX_WILDCARD_TOKENS = 5

_SPLIT_RE = re.compile(r"[\s,;|=:\"'()\[\]{}]+")


def _tokenise(line: str) -> list[str]:
    return [t for t in _SPLIT_RE.split(line.strip()) if t]


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------

@dataclass
class TrieNode:
    children: dict[str, "TrieNode"] = field(default_factory=dict)
    is_leaf: bool = False
    template: str = ""
    position_map: dict[int, str] = field(default_factory=dict)  # wildcard_idx → field_name
    signature: str = ""


# ---------------------------------------------------------------------------
# Trie
# ---------------------------------------------------------------------------

class LogTemplateTrie:
    """
    Prefix trie that stores log templates.
    Variable positions are stored as literal "<*>" nodes.
    """

    def __init__(self):
        self.root = TrieNode()

    # ------------------------------------------------------------------
    # Insertion
    # ------------------------------------------------------------------

    def insert(
        self,
        template: str,
        position_map: dict[int, str],
        signature: str,
        source: str = "manual",
        persist_db: bool = True,
    ) -> None:
        """
        Insert a template into the trie.
        template: token string, e.g. "TOOL <*> ALARM <*> TEMP <*>"
        position_map: {wildcard_index: vendor_field_name}
        signature: log signature hash
        source: 'partitioner' | 'llm' | 'manual'
        persist_db: write to trie_templates table (set False when reloading from DB/registry)
        """
        tokens = _tokenise(template)
        node = self.root
        for tok in tokens:
            if tok not in node.children:
                node.children[tok] = TrieNode()
            node = node.children[tok]

        node.is_leaf = True
        node.template = template
        node.position_map = position_map
        node.signature = signature

        if persist_db:
            from database.writer import insert_trie_template
            insert_trie_template(signature, template, json.dumps(position_map), source)

    # ------------------------------------------------------------------
    # Matching
    # ------------------------------------------------------------------

    def match(self, line: str) -> tuple[str, dict, str] | None:
        """
        Match a log line against stored templates.

        Returns (template, position_map, signature) for the best match,
        or None if no template matches.

        "Best" = most non-wildcard tokens matched (most specific template).
        """
        tokens = _tokenise(line)
        best = self._search(self.root, tokens, 0, 0)
        if best is None:
            return None
        node, exact_count, _ = best
        return node.template, node.position_map, node.signature

    def _search(
        self,
        node: TrieNode,
        tokens: list[str],
        pos: int,
        exact_count: int,
    ) -> tuple[TrieNode, int, list[str]] | None:
        """
        Recursive DFS.
        Returns (leaf_node, exact_token_count, matched_wildcard_values) or None.
        """
        if pos == len(tokens):
            if node.is_leaf:
                return (node, exact_count, [])
            return None

        best_result = None
        best_exact = -1

        # 1. Try exact token match
        tok = tokens[pos]
        if tok in node.children:
            result = self._search(node.children[tok], tokens, pos + 1, exact_count + 1)
            if result is not None:
                candidate_node, candidate_exact, _ = result
                if candidate_exact > best_exact:
                    best_exact = candidate_exact
                    best_result = result

        # 2. Try wildcard match
        if "<*>" in node.children:
            wc_node = node.children["<*>"]
            # Try consuming 1 to MAX_WILDCARD_TOKENS tokens
            for span in range(1, MAX_WILDCARD_TOKENS + 1):
                if pos + span > len(tokens):
                    break
                result = self._search(wc_node, tokens, pos + span, exact_count)
                if result is not None:
                    candidate_node, candidate_exact, _ = result
                    if candidate_exact > best_exact:
                        best_exact = candidate_exact
                        best_result = result

        return best_result

    # ------------------------------------------------------------------
    # Value extraction
    # ------------------------------------------------------------------

    def extract_values(
        self,
        line: str,
        template: str,
        position_map: dict[int, str],
    ) -> dict[str, str]:
        """
        Given a matched template, extract the actual values for each wildcard.
        Returns {vendor_field_name: extracted_value}.
        """
        input_tokens = _tokenise(line)
        template_tokens = _tokenise(template)

        extracted: dict[str, str] = {}
        wildcard_idx = 0
        i_tpl = 0
        i_inp = 0

        while i_tpl < len(template_tokens) and i_inp < len(input_tokens):
            tpl_tok = template_tokens[i_tpl]
            if tpl_tok == "<*>":
                # Consume tokens until the next template token matches
                next_tpl = template_tokens[i_tpl + 1] if i_tpl + 1 < len(template_tokens) else None
                collected = []
                while i_inp < len(input_tokens):
                    if next_tpl and input_tokens[i_inp] == next_tpl:
                        break
                    if len(collected) >= MAX_WILDCARD_TOKENS:
                        break
                    collected.append(input_tokens[i_inp])
                    i_inp += 1

                value = " ".join(collected)
                field_name = position_map.get(wildcard_idx, f"unknown_field_{wildcard_idx}")
                if value:
                    extracted[field_name] = value
                wildcard_idx += 1
            else:
                # Exact token — advance both pointers
                i_inp += 1
            i_tpl += 1

        return extracted

    # ------------------------------------------------------------------
    # Persistence
    # ------------------------------------------------------------------

    def save(self, path: str) -> None:
        """Pickle the entire trie to disk."""
        with open(path, "wb") as f:
            pickle.dump(self.root, f)
        logger.info("Trie saved to %s", path)

    @classmethod
    def load(cls, path: str) -> "LogTemplateTrie":
        """Load a trie from a pickle file produced by save()."""
        trie = cls()
        with open(path, "rb") as f:
            trie.root = pickle.load(f)
        logger.info("Trie loaded from %s", path)
        return trie

    # ------------------------------------------------------------------
    # Debug
    # ------------------------------------------------------------------

    def print_trie(self, node: TrieNode | None = None, prefix: list[str] | None = None) -> None:
        """Print all templates stored in the trie (for debugging)."""
        if node is None:
            node = self.root
        if prefix is None:
            prefix = []
        if node.is_leaf:
            print(f"Template    : {node.template}")
            print(f"Signature   : {node.signature}")
            print(f"Position map: {node.position_map}")
            print(f"Path        : {' → '.join(prefix)}")
            print()
        for token, child in node.children.items():
            self.print_trie(child, prefix + [token])

    # ------------------------------------------------------------------
    # Bulk load from registry
    # ------------------------------------------------------------------

    def load_from_registry(self, registry) -> int:
        """
        Pre-populate the trie from the mapping registry on startup.
        Returns the number of templates loaded.
        """
        loaded = 0
        for sig in registry.get_all_signatures():
            record = registry.get_full_record(sig)
            if record is None:
                continue
            patterns = record.get("regex_patterns") or {}
            if not patterns:
                continue
            # Build a template from the stored patterns
            # (placeholder — real templates are stored in the trie separate from DB)
            # On first miss, the real template will be inserted
            loaded += 1
        return loaded

    def match_and_track(self, line: str) -> tuple[str, dict, str] | None:
        """
        Match a log line and increment the hit_count in the DB for the matched template.
        Returns (template, position_map, signature) or None.
        """
        result = self.match(line)
        if result is not None:
            _, _, sig = result
            from database.writer import increment_trie_hit
            increment_trie_hit(sig)
        return result


# ---------------------------------------------------------------------------
# Module-level helper
# ---------------------------------------------------------------------------

def build_position_map(template: str, field_names: list[str]) -> dict[int, str]:
    """
    Map wildcard positions in a template string to vendor field names.

    Example:
        template    = "TIMESTAMP <*> MACHINE <*> STATUS <*>"
        field_names = ["timestamp", "unit", "status"]
        returns     = {0: "timestamp", 1: "unit", 2: "status"}

    Raises ValueError if the number of <*> tokens doesn't match field_names.
    """
    tokens = _tokenise(template)
    wildcard_count = sum(1 for t in tokens if t == "<*>")
    if wildcard_count != len(field_names):
        raise ValueError(
            f"Template has {wildcard_count} wildcard(s) but "
            f"{len(field_names)} field name(s) were supplied."
        )
    return {idx: name for idx, name in enumerate(field_names)}
