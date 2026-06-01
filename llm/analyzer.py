"""
LLM Analysis orchestrator — implements the 3-tier fallback chain:
  1. Groq API (cloud, fast) — only if GROQ_API_KEY is set
  2. Ollama local (llama3.2:3b) — fully offline
  3. Rules engine — always works, keyword matching

Also handles text-to-SQL queries by injecting the warehouse schema.
"""
from __future__ import annotations

import logging
import uuid
from dataclasses import dataclass, field
from enum import Enum

import groq_client
import ollama_client
import rules_engine
from text_to_sql import build_prompt, extract_sql, is_safe_sql

logger = logging.getLogger(__name__)


class QueryType(str, Enum):
    TEXT_TO_SQL = "text2sql"
    INSIGHT = "insight"
    ANOMALY = "anomaly"


@dataclass
class AnalysisResult:
    insight_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    query_type: str = QueryType.INSIGHT
    user_query: str = ""
    generated_sql: str | None = None
    response: str = ""
    model_used: str = ""
    latency_ms: int = 0
    error: str | None = None


def _try_groq(prompt: str, system: str) -> tuple[str, int] | None:
    if not groq_client.is_configured():
        return None
    try:
        text, ms = groq_client.complete(prompt, system)
        return text, ms
    except (groq_client.GroqUnavailable, groq_client.GroqRateLimited) as exc:
        logger.warning("Groq skipped: %s", exc)
        return None


def _try_ollama(prompt: str, system: str) -> tuple[str, int] | None:
    if not ollama_client.is_available():
        return None
    try:
        text, ms = ollama_client.complete(prompt, system)
        return text, ms
    except ollama_client.OllamaUnavailable as exc:
        logger.warning("Ollama skipped: %s", exc)
        return None


def analyze(user_query: str, query_type: QueryType = QueryType.TEXT_TO_SQL) -> AnalysisResult:
    result = AnalysisResult(user_query=user_query, query_type=query_type)

    if query_type == QueryType.TEXT_TO_SQL:
        prompt = build_prompt(user_query)
        system = "Return only a valid ClickHouse SQL SELECT statement. No explanation."

        raw_sql: str | None = None
        for name, attempt in [
            ("groq", lambda: _try_groq(prompt, system)),
            ("ollama", lambda: _try_ollama(prompt, system)),
        ]:
            out = attempt()
            if out:
                raw_sql, result.latency_ms = out
                result.model_used = name
                break

        if raw_sql:
            sql = extract_sql(raw_sql)
            if is_safe_sql(sql):
                result.generated_sql = sql
                result.response = f"Generated SQL query (model: {result.model_used})"
            else:
                result.error = "Generated SQL failed safety check"
                result.response = "Could not generate a safe SELECT query for your question."
        else:
            # Rules engine fallback
            rules_result = rules_engine.analyze(user_query)
            result.generated_sql = rules_result.generated_sql
            result.response = rules_result.response
            result.model_used = "rules_engine"
            result.latency_ms = rules_result.latency_ms

    else:  # INSIGHT or ANOMALY
        system = (
            "You are a concise data analyst. Given aggregated ETL pipeline metrics, "
            "identify key trends, anomalies, or business insights in 3-5 bullet points."
        )
        for name, attempt in [
            ("groq", lambda: _try_groq(user_query, system)),
            ("ollama", lambda: _try_ollama(user_query, system)),
        ]:
            out = attempt()
            if out:
                result.response, result.latency_ms = out
                result.model_used = name
                return result

        rules_result = rules_engine.analyze(user_query)
        result.response = rules_result.response
        result.model_used = "rules_engine"
        result.latency_ms = rules_result.latency_ms

    return result
