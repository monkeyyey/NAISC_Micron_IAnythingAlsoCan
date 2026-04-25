"""
partitioning/iterative_partitioner.py — Offline trie pre-population.

Runs once on a batch of historical log lines to discover stable templates
and pre-populate the prefix trie before runtime.

Algorithm:
  1. Group lines by token count
  2. Within each group, split by first token value
  3. For each subsequent position: constant across all lines → keep; variable → <*>
  4. Stop when group is too small or all positions classified
  5. Insert discovered templates into the trie
"""

from __future__ import annotations

import logging
import re
from collections import defaultdict

from signature.generator import generate_signature

logger = logging.getLogger(__name__)

_SPLIT_RE = re.compile(r"[\s,;|=:\"'()\[\]{}]+")


def _tokenise(line: str) -> list[str]:
    return [t for t in _SPLIT_RE.split(line.strip()) if t]


def iterative_partition(
    log_lines: list[str],
    min_partition_size: int = 2,
) -> list[tuple[str, dict[int, str]]]:
    """
    Discover log templates from a list of raw log lines.

    Returns a list of (template_string, position_map) tuples where:
      - template_string uses <*> for variable positions
      - position_map maps wildcard_index → "unknown_field_{N}" placeholder
        (the LLM fills in real names on the first runtime hit)
    """
    # Discard empty lines
    lines = [l.strip() for l in log_lines if l.strip()]
    if not lines:
        return []

    tokenised = [_tokenise(l) for l in lines]

    # Step 1: Group by token count
    by_length: dict[int, list[list[str]]] = defaultdict(list)
    for tokens in tokenised:
        by_length[len(tokens)].append(tokens)

    templates: list[tuple[str, dict[int, str]]] = []

    for length, group in by_length.items():
        if len(group) < min_partition_size:
            # Too small to generalise — add as-is
            for toks in group:
                template = " ".join(toks)
                templates.append((template, {}))
            continue

        # Step 2: Split by first token
        by_first: dict[str, list[list[str]]] = defaultdict(list)
        for toks in group:
            by_first[toks[0]].append(toks)

        for first_tok, sub_group in by_first.items():
            if len(sub_group) < min_partition_size:
                templates.append((" ".join(sub_group[0]), {}))
                continue

            template_tokens, position_map = _classify_positions(sub_group)
            template_str = " ".join(template_tokens)
            templates.append((template_str, position_map))

    return templates


def _classify_positions(
    token_groups: list[list[str]],
) -> tuple[list[str], dict[int, str]]:
    """
    For each token position across all lines in the group:
      - If all lines share the same token → constant, keep as-is
      - If lines differ              → variable, replace with <*>

    Returns (template_token_list, position_map).
    position_map: {wildcard_index: "unknown_field_N"}
    """
    if not token_groups:
        return [], {}

    length = len(token_groups[0])
    template_tokens: list[str] = []
    position_map: dict[int, str] = {}
    wildcard_idx = 0

    for pos in range(length):
        values = {toks[pos] for toks in token_groups if pos < len(toks)}
        if len(values) == 1:
            template_tokens.append(values.pop())
        else:
            template_tokens.append("<*>")
            position_map[wildcard_idx] = f"unknown_field_{wildcard_idx}"
            wildcard_idx += 1

    return template_tokens, position_map


def populate_trie_from_lines(
    log_lines: list[str],
    trie,
    min_partition_size: int = 2,
) -> int:
    """
    Run iterative_partition and insert all discovered templates into the trie.
    Returns the count of templates inserted.
    """
    templates = iterative_partition(log_lines, min_partition_size)
    inserted = 0
    seen_sigs: set[str] = set()

    for template_str, position_map in templates:
        sig = generate_signature(template_str)
        if sig in seen_sigs:
            continue
        seen_sigs.add(sig)
        trie.insert(template_str, position_map, sig, source="partitioner")
        inserted += 1

    logger.info("Partitioner: discovered %d templates from %d lines", inserted, len(log_lines))
    return inserted
