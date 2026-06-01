"""Ollama local LLM client — fully offline, always available."""
import logging
import os
import time

import httpx

logger = logging.getLogger(__name__)

BASE_URL = os.environ.get("OLLAMA_BASE_URL", "http://localhost:11434")
MODEL = os.environ.get("OLLAMA_MODEL", "llama3.2:3b")
TIMEOUT = 120.0


class OllamaUnavailable(Exception):
    pass


def _post(endpoint: str, payload: dict) -> dict:
    try:
        with httpx.Client(timeout=TIMEOUT) as client:
            resp = client.post(f"{BASE_URL}{endpoint}", json=payload)
            resp.raise_for_status()
            return resp.json()
    except (httpx.ConnectError, httpx.TimeoutException) as exc:
        raise OllamaUnavailable(f"Ollama unreachable: {exc}") from exc


def is_available() -> bool:
    try:
        with httpx.Client(timeout=3.0) as client:
            client.get(f"{BASE_URL}/api/tags").raise_for_status()
        return True
    except Exception:
        return False


def complete(prompt: str, system: str = "", temperature: float = 0.1) -> tuple[str, int]:
    """Returns (response_text, latency_ms)."""
    t0 = time.monotonic()
    payload = {
        "model": MODEL,
        "prompt": prompt,
        "system": system,
        "stream": False,
        "options": {"temperature": temperature, "num_predict": 2048},
    }
    data = _post("/api/generate", payload)
    latency_ms = int((time.monotonic() - t0) * 1000)
    return data.get("response", ""), latency_ms
