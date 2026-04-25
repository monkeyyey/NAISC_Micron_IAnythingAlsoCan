"""
llm/client.py — Anthropic API wrapper with batching logic.

Uses claude-opus-4-6 (best quality for critical mapping generation).
Cache misses are always batched — never sent one at a time.

The client accumulates misses in a pending buffer; callers flush the buffer
explicitly via generate_mapping() with a full batch.
"""

import logging
import os
import time
from typing import Optional

try:
    import openai
except ImportError:  # pragma: no cover
    openai = None

from config import LLM_MODEL, LLM_MAX_TOKENS, LLM_TEMPERATURE, BATCH_LLM_MAX_SIZE
from database.writer import insert_llm_failure
from llm.prompt_builder import build_system_prompt, build_user_prompt
from llm.response_parser import parse_llm_response

logger = logging.getLogger(__name__)


class LLMClient:
    """
    OpenAI API client for log field mapping generation.
    Always batches cache misses — never called per line.
    If OPENAI_API_KEY is not set, runs in bypass mode (no LLM calls).
    """

    def __init__(self, model: str = LLM_MODEL):
        self.model = model
        api_key = os.environ.get("OPENAI_API_KEY", "")

        if openai is None:
            self._client = None
            self._system_prompt = ""
            logger.warning(
                "LLMClient: openai package is not installed — running in bypass mode (cache-miss records will have empty mappings)"
            )
            return

        if not api_key:
            self._client = None
            self._system_prompt = ""
            logger.warning(
                "LLMClient: no OPENAI_API_KEY — running in bypass mode (cache-miss records will have empty mappings)"
            )
            return

        openai.api_key = api_key
        self._client = openai
        self._system_prompt = build_system_prompt()
        logger.info("LLMClient initialised with model=%s", model)

    def _is_quota_error(self, exc: Exception) -> bool:
        """Detect whether an OpenAI exception indicates quota exhaustion."""
        if getattr(exc, "http_status", None) == 429:
            return True

        code = getattr(exc, "code", None)
        if code == "insufficient_quota":
            return True

        error_obj = getattr(exc, "error", None)
        if isinstance(error_obj, dict) and error_obj.get("code") == "insufficient_quota":
            return True

        message = str(exc).lower()
        return "insufficient quota" in message or "quota exceeded" in message

    def generate_mapping(
        self,
        log_lines: list[str],
        signatures: list[str],
        examples: list[dict],
    ) -> list[dict]:
        """
        Send a batch of cache-miss log lines to the LLM.

        Args:
            log_lines:  Lines that had no cached mapping (max BATCH_LLM_MAX_SIZE)
            signatures: Corresponding log signatures (same length as log_lines)
            examples:   ICL examples from the candidate pool

        Returns:
            List of validated mapping dicts, one per input line.
            Each dict has keys: signature, fields, confidence, parse_flags
        """
        if not log_lines:
            return []

        if self._client is None:
            return [{"signature": sig, "fields": {}, "confidence": 0.0, "parse_flags": ["llm_bypass"]} for sig in signatures]

        # Enforce batch size limit — callers must pre-chunk
        if len(log_lines) > BATCH_LLM_MAX_SIZE:
            logger.warning(
                "Batch size %d exceeds BATCH_LLM_MAX_SIZE %d — truncating",
                len(log_lines), BATCH_LLM_MAX_SIZE,
            )
            log_lines = log_lines[:BATCH_LLM_MAX_SIZE]
            signatures = signatures[:BATCH_LLM_MAX_SIZE]

        user_prompt = build_user_prompt(log_lines, signatures, examples)

        MAX_LLM_RETRIES = 5
        last_exc = None
        for attempt in range(1, MAX_LLM_RETRIES + 1):
            try:
                response = self._client.chat.completions.create(
                    model=self.model,
                    max_tokens=LLM_MAX_TOKENS,
                    temperature=LLM_TEMPERATURE,
                    messages=[
                        {"role": "system", "content": self._system_prompt},
                        {"role": "user", "content": user_prompt},
                    ],
                )
                raw_text = response.choices[0].message.content
                logger.debug(
                    "LLM response for %d lines", len(log_lines)
                )
                return parse_llm_response(raw_text, signatures)
            except Exception as exc:
                last_exc = exc
                quota_error = self._is_quota_error(exc)
                if quota_error:
                    logger.error(
                        "LLM quota error detected on attempt %d/%d: %s", attempt, MAX_LLM_RETRIES, exc
                    )
                    break

                logger.warning(
                    "LLM attempt %d/%d failed: %s", attempt, MAX_LLM_RETRIES, exc
                )
                if attempt < MAX_LLM_RETRIES:
                    time.sleep(2 ** (attempt - 1))  # 1s, 2s, 4s, 8s before retries 2-5

        # All retries exhausted — log each line for engineer review
        for line, sig in zip(log_lines, signatures):
            insert_llm_failure(sig, line, "batch", str(last_exc), MAX_LLM_RETRIES)
        logger.error(
            "LLM permanently failed for %d lines after %d attempts",
            len(log_lines), MAX_LLM_RETRIES,
        )
        return [
            {
                "signature":   sig,
                "fields":      {},
                "confidence":  0.0,
                "parse_flags": [f"llm_permanent_failure:{type(last_exc).__name__}"],
            }
            for sig in signatures
        ]

    def batch_generate(
        self,
        log_lines: list[str],
        signatures: list[str],
        candidate_pool,
    ) -> list[dict]:
        """
        Generate mappings for any number of lines by splitting into batches
        of BATCH_LLM_MAX_SIZE, fetching ICL examples for each batch, and
        concatenating results.

        Args:
            log_lines:      All cache-miss lines
            signatures:     Corresponding signatures
            candidate_pool: CandidatePool instance for kNN lookup

        Returns:
            Flat list of mapping dicts, same length as log_lines.
        """
        results: list[dict] = []

        for batch_start in range(0, len(log_lines), BATCH_LLM_MAX_SIZE):
            batch_lines = log_lines[batch_start: batch_start + BATCH_LLM_MAX_SIZE]
            batch_sigs  = signatures[batch_start: batch_start + BATCH_LLM_MAX_SIZE]

            # Fetch ICL examples for the first line of the batch
            # (representative of the batch's log type)
            examples: list[dict] = []
            if candidate_pool and batch_lines:
                similar = candidate_pool.get_similar(batch_lines[0], k=3)
                examples = [{"line": l, "mapping": {}} for l in similar]

            batch_results = self.generate_mapping(batch_lines, batch_sigs, examples)
            results.extend(batch_results)

        return results
