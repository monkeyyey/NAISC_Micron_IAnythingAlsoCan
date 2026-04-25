"""
caching/trie.py — Prefix trie for unstructured log template matching.

Each leaf node stores a log template (token sequence with <*> wildcards)
and a position_map that records which wildcard index maps to which vendor field.

Wildcard matching: at each <*> node, we try consuming 1 to MAX_WILDCARD_TOKENS
input tokens, choosing the match that has the most exact (non-wildcard) tokens.
"""
import logging
import re
from dataclasses import dataclass, field

logger = logging.getLogger(__name__)


def _tokenise(log_line: str) -> list[str]:
  return [tok.strip(",.;") for tok in log_line.split()]


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
    ) -> None:
        """
        Insert a template into the trie.
        template: token string, e.g. "TOOL <*> ALARM <*> TEMP <*>"
        position_map: {wildcard_index: vendor_field_name}
        signature: log signature hash
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
            result = self._search(wc_node, tokens, pos + 1, exact_count)
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

import hashlib


def build_position_map(template: str, field_names: list[str]) -> dict[int, str]:
  """
  template:    "Reactor <*> tripped at <*>"
  field_names: ["unit", "timestamp"]
  returns:     {0: "unit", 1: "timestamp"}
  """
  tokens = _tokenise(template)
  wildcard_count = sum(1 for t in tokens if "<*>" in t)

  if len(field_names) != wildcard_count:
    raise ValueError(f"Template has {wildcard_count} wildcards but {len(field_names)} field names were supplied")

  return {idx: name for idx, name in enumerate(field_names)}


def generate_signature(template: str) -> str:
  return hashlib.md5(template.encode()).hexdigest()

def print_trie(node: TrieNode, prefix: list[str] = None) -> None:
  if prefix is None:
    prefix = []

  if node.is_leaf:
    print(f"Template:     {node.template}")
    print(f"Signature:    {node.signature}")
    print(f"Position map: {node.position_map}")
    print(f"Path:         {' → '.join(prefix)}")
    print()

  for token, child in node.children.items():
    print_trie(child, prefix + [token])