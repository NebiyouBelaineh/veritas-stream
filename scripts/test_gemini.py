"""
Test script for Gemini via OpenRouter.

GEMINI_API_KEY in .env is an OpenRouter key (sk-or-v1-...).
OpenRouter exposes an OpenAI-compatible API, so we POST to:
  https://openrouter.ai/api/v1/chat/completions

Usage:
    python scripts/test_gemini.py
    python scripts/test_gemini.py "What is 2+2?"
"""

import asyncio
import json
import os
import sys
import urllib.request
import urllib.error
from pathlib import Path

# Load .env from project root
env_path = Path(__file__).parent.parent / ".env"
if env_path.exists():
    for line in env_path.read_text().splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            key, _, value = line.partition("=")
            os.environ.setdefault(key.strip(), value.strip())

API_KEY = os.environ.get("GEMINI_API_KEY")
if not API_KEY:
    print("Error: GEMINI_API_KEY not set in .env or environment.")
    sys.exit(1)

MODEL = os.environ.get("GEMINI_LLM_MODEL", "google/gemini-2.5-flash")

OPENROUTER_URL = "https://openrouter.ai/api/v1/chat/completions"


def chat(prompt: str, model: str = MODEL) -> dict:
    payload = json.dumps({
        "model": model,
        "messages": [{"role": "user", "content": prompt}],
    }).encode()

    req = urllib.request.Request(
        OPENROUTER_URL,
        data=payload,
        headers={
            "Authorization": f"Bearer {API_KEY}",
            "Content-Type": "application/json",
        },
        method="POST",
    )

    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.loads(resp.read())


def main():
    prompt = " ".join(sys.argv[1:]) if len(sys.argv) > 1 else "Say hello and identify yourself."

    print(f"Model : {MODEL}")
    print(f"Prompt: {prompt}")
    print("-" * 60)

    try:
        result = chat(prompt)
    except urllib.error.HTTPError as e:
        body = e.read().decode()
        print(f"HTTP {e.code}: {body}")
        sys.exit(1)

    message = result["choices"][0]["message"]["content"]
    usage = result.get("usage", {})

    print(message)
    print("-" * 60)
    if usage:
        print(f"Tokens: {usage.get('prompt_tokens', '?')} in / {usage.get('completion_tokens', '?')} out")


if __name__ == "__main__":
    main()
