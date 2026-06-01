"""Groq cloud LLM client — opt-in via GROQ_API_KEY env var."""
import logging
import os
import time

import httpx

logger = logging.getLogger(__name__)

API_KEY = os.environ.get("GROQ_API_KEY", "")
MODEL = os.environ.get("GROQ_MODEL", "llama-3.1-70b-versatile")
BASE_URL = "https://api.groq.com/openai/v1"
TIMEOUT = 30.0


class GroqUnavailable(Exception):
    pass


class GroqRateLimited(Exception):
    pass


def is_configured() -> bool:
    return bool(API_KEY)


def complete(prompt: str, system: str = "", temperature: float = 0.1) -> tuple[str, int]:
    """Returns (response_text, latency_ms). Raises if Groq is unavailable."""
    if not API_KEY:
        raise GroqUnavailable("GROQ_API_KEY not set")

    t0 = time.monotonic()
    payload = {
        "model": MODEL,
        "messages": [
            {"role": "system", "content": system or "You are a helpful data analyst."},
            {"role": "user", "content": prompt},
        ],
        "temperature": temperature,
        "max_tokens": 2048,
    }
    headers = {"Authorization": f"Bearer {API_KEY}", "Content-Type": "application/json"}

    try:
        with httpx.Client(timeout=TIMEOUT) as client:
            resp = client.post(f"{BASE_URL}/chat/completions", json=payload, headers=headers)
            if resp.status_code == 429:
                raise GroqRateLimited("Groq rate limit hit")
            resp.raise_for_status()
            data = resp.json()
    except (httpx.ConnectError, httpx.TimeoutException) as exc:
        raise GroqUnavailable(f"Groq unreachable: {exc}") from exc

    latency_ms = int((time.monotonic() - t0) * 1000)
    text = data["choices"][0]["message"]["content"]
    return text, latency_ms
