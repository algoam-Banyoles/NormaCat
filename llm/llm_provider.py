"""
llm_provider.py — Unified LLM provider for Project Checker.

Supports four backends:
  - claude   : Anthropic Claude API (best quality, paid)
  - gemini   : Google Gemini API (free tier, good quality)
  - groq     : Groq API (free tier, very fast inference, Llama 3.3 70B)
  - ollama   : Local Ollama server (free, offline, lower quality)

Selection via:
  1. LLM_PROVIDER env var ("claude", "gemini", "groq", "ollama")
  2. Or programmatically via set_provider()
"""

import os
import sys
import time
import threading

# Assegurar que la carpeta arrel de NormaCat esta al path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# ---------------------------------------------------------------------------
# LLMProvider
# ---------------------------------------------------------------------------

class LLMProvider:
    """Unified interface for calling different LLM backends."""

    VALID_BACKENDS = ("claude", "gemini", "groq", "ollama")

    def __init__(self, backend: str = "gemini"):
        backend = backend.lower().strip()
        if backend not in self.VALID_BACKENDS:
            raise ValueError(
                f"Backend '{backend}' not recognised. "
                f"Choose from: {', '.join(self.VALID_BACKENDS)}"
            )
        self.backend = backend
        self.model = self._resolve_model()
        print(f"  [LLM] Provider: {self.backend} | Model: {self.model}")

    # -- model resolution ---------------------------------------------------

    def _resolve_model(self) -> str:
        if self.backend == "claude":
            return os.environ.get("CLAUDE_MODEL", "claude-sonnet-4-5")
        if self.backend == "gemini":
            return os.environ.get("GEMINI_MODEL", "gemini-2.5-flash")
        if self.backend == "groq":
            return os.environ.get("GROQ_MODEL", "llama-3.3-70b-versatile")
        # ollama
        return os.environ.get("OLLAMA_MODEL", "llama3.1:8b")

    # -- public API ---------------------------------------------------------

    def call(
        self,
        system: str,
        user_message: str,
        max_tokens: int = 8192,
        temperature: float = 0,
    ) -> dict:
        """
        Call the configured LLM backend.

        Returns
        -------
        dict with keys:
            text        : str   — The model's text response
            tokens_in   : int   — Input token count (estimated if unavailable)
            tokens_out  : int   — Output token count (estimated if unavailable)
            model       : str   — Actual model name used
            provider    : str   — "claude" | "gemini" | "groq" | "ollama"
            elapsed_s   : float — Wall-clock seconds
        """
        dispatch = {
            "claude": self._call_claude,
            "gemini": self._call_gemini,
            "groq": self._call_groq,
            "ollama": self._call_ollama,
        }
        start = time.time()
        result = dispatch[self.backend](system, user_message, max_tokens, temperature)
        elapsed = time.time() - start
        result["elapsed_s"] = round(elapsed, 2)
        result["provider"] = self.backend
        result["model"] = self.model
        print(
            f"  [LLM] {elapsed:.1f}s | "
            f"{result['tokens_in']}+{result['tokens_out']} tokens | "
            f"model: {self.model}"
        )
        return result

    # -- Claude -------------------------------------------------------------

    def _call_claude(
        self, system: str, user_message: str, max_tokens: int, temperature: float
    ) -> dict:
        try:
            import anthropic
        except ImportError:
            raise ImportError(
                "Provider 'claude' requires 'anthropic'. "
                "Install with: pip install anthropic"
            )

        api_key = os.environ.get("ANTHROPIC_API_KEY")
        if not api_key:
            raise RuntimeError(
                "Provider 'claude' requires env var 'ANTHROPIC_API_KEY'"
            )

        client = anthropic.Anthropic(api_key=api_key, timeout=300.0)

        last_err = None
        for attempt in range(1, 4):  # up to 3 attempts (1 + 2 retries)
            try:
                response = client.messages.create(
                    model=self.model,
                    max_tokens=max_tokens,
                    temperature=temperature,
                    system=system,
                    messages=[{"role": "user", "content": user_message}],
                )
                text = "".join(
                    block.text for block in response.content if hasattr(block, "text")
                )
                return {
                    "text": text,
                    "tokens_in": response.usage.input_tokens,
                    "tokens_out": response.usage.output_tokens,
                }
            except Exception as e:
                last_err = e
                if attempt < 3:
                    print(f"  [LLM] Claude attempt {attempt} failed: {e}. Retrying in 30s...")
                    time.sleep(30)
        raise RuntimeError(f"Claude API failed after 3 attempts: {last_err}") from last_err

    # -- Gemini -------------------------------------------------------------

    # Gemini model fallback chain and thinking model detection
    _GEMINI_FALLBACK_CHAIN = [
        "gemini-2.5-flash",       # Best quality, 20-250 RPD
        "gemini-2.5-flash-lite",  # Good quality, 1000 RPD
        "gemini-2.5-pro",         # Highest quality, 100 RPD
    ]
    _THINKING_MODELS = {"gemini-2.5-flash", "gemini-2.5-pro"}

    def _call_gemini(
        self, system: str, user_message: str, max_tokens: int, temperature: float
    ) -> dict:
        try:
            from google import genai
        except ImportError:
            raise ImportError(
                "Provider 'gemini' requires 'google-genai'. "
                "Install with: pip install google-genai"
            )

        api_key = os.environ.get("GEMINI_API_KEY")
        if not api_key:
            raise RuntimeError(
                "Provider 'gemini' requires env var 'GEMINI_API_KEY'"
            )

        client = genai.Client(api_key=api_key)

        # Rate limiter: max ~8 RPM (free tier = 10 RPM, leave margin)
        import time as _time
        if not hasattr(self, '_gemini_last_call'):
            self._gemini_last_call = 0
        min_interval = 8.0
        elapsed_since_last = _time.time() - self._gemini_last_call
        if elapsed_since_last < min_interval:
            wait = min_interval - elapsed_since_last
            print(f"  [LLM] Gemini rate limit: esperant {wait:.0f}s...",
                  file=sys.stderr, flush=True)
            _time.sleep(wait)
        self._gemini_last_call = _time.time()

        # Build fallback model list: primary first, then chain
        primary_model = self.model
        models_to_try = [primary_model]
        for m in self._GEMINI_FALLBACK_CHAIN:
            if m not in models_to_try:
                models_to_try.append(m)

        start_time = _time.time()
        last_err = None
        actual_model = primary_model

        for model_name in models_to_try:
            actual_model = model_name

            # Build config per model (thinking vs non-thinking)
            config = self._gemini_config(genai, system, max_tokens,
                                         temperature, model_name)

            # Try this model with retries for transient errors
            for attempt in range(1, 3):
                try:
                    response = client.models.generate_content(
                        model=model_name,
                        contents=user_message,
                        config=config,
                    )

                    elapsed = _time.time() - start_time

                    # Extract text, skipping thinking/reasoning parts
                    text = self._extract_gemini_text(response)

                    # Token counts
                    tokens_in = tokens_out = 0
                    meta = getattr(response, "usage_metadata", None)
                    if meta:
                        tokens_in = getattr(meta, "prompt_token_count", 0) or 0
                        tokens_out = getattr(meta, "candidates_token_count", 0) or 0
                    if not tokens_in:
                        tokens_in = (len(system) + len(user_message)) // 4
                    if not tokens_out:
                        tokens_out = len(text) // 4

                    fb = " (fallback)" if model_name != primary_model else ""
                    print(f"  [LLM] Gemini OK: {model_name}{fb} | "
                          f"{elapsed:.1f}s | {tokens_in}+{tokens_out} tok",
                          file=sys.stderr, flush=True)

                    return {
                        "text": text,
                        "tokens_in": tokens_in,
                        "tokens_out": tokens_out,
                        "model": model_name,
                        "provider": "gemini",
                        "elapsed_s": round(elapsed, 1),
                    }

                except Exception as e:
                    error_str = str(e)
                    is_quota = ("429" in error_str
                                or "RESOURCE_EXHAUSTED" in error_str
                                or "quota" in error_str.lower())

                    if is_quota:
                        print(f"  [LLM] Gemini {model_name}: quota exhaurida",
                              file=sys.stderr, flush=True)
                        last_err = e
                        break  # skip retries, go to next model

                    # Non-quota error: retry once then raise
                    last_err = e
                    if attempt < 2:
                        print(f"  [LLM] Gemini {model_name} error: {e}. "
                              f"Reintent en 15s...",
                              file=sys.stderr, flush=True)
                        _time.sleep(15)
                    else:
                        raise
            # If we broke out of retry loop due to quota, continue to next model

        raise RuntimeError(
            f"Gemini: tots els models exhaurits ({', '.join(models_to_try)}). "
            f"Ultim error: {last_err}"
        ) from last_err

    def _gemini_config(self, genai, system, max_tokens, temperature, model_name):
        """Build GenerateContentConfig adapted per thinking vs non-thinking."""
        if model_name in self._THINKING_MODELS:
            effective_max = max(max_tokens * 10, 65536)
            try:
                return genai.types.GenerateContentConfig(
                    system_instruction=system,
                    max_output_tokens=effective_max,
                    temperature=temperature,
                    thinking_config=genai.types.ThinkingConfig(
                        thinking_budget=max_tokens,
                    ),
                )
            except (TypeError, AttributeError):
                return genai.types.GenerateContentConfig(
                    system_instruction=system,
                    max_output_tokens=effective_max,
                    temperature=temperature,
                )
        else:
            # Non-thinking (flash-lite, etc): standard config
            return genai.types.GenerateContentConfig(
                system_instruction=system,
                max_output_tokens=max_tokens,
                temperature=temperature,
            )

    @staticmethod
    def _extract_gemini_text(response) -> str:
        """Extract text from Gemini response, skipping thinking parts."""
        try:
            if hasattr(response, 'candidates') and response.candidates:
                parts = []
                for part in response.candidates[0].content.parts:
                    if hasattr(part, 'thought') and part.thought:
                        continue
                    if hasattr(part, 'text') and part.text:
                        parts.append(part.text)
                if parts:
                    return "\n".join(parts)
        except Exception:
            pass
        return response.text or ""

    # -- Groq ----------------------------------------------------------------

    def _call_groq(
        self, system: str, user_message: str, max_tokens: int, temperature: float
    ) -> dict:
        try:
            from openai import OpenAI
        except ImportError:
            raise ImportError(
                "Provider 'groq' requires 'openai'. "
                "Install with: pip install openai"
            )

        api_key = os.environ.get("GROQ_API_KEY")
        if not api_key:
            raise RuntimeError(
                "Provider 'groq' requires env var 'GROQ_API_KEY'. "
                "Get a free key at https://console.groq.com"
            )

        client = OpenAI(
            api_key=api_key,
            base_url="https://api.groq.com/openai/v1",
        )

        # Rate limiter: ~10 RPM on free tier, keep conservative margin
        if not hasattr(self, '_groq_last_call'):
            self._groq_last_call = 0
        min_interval = 6.0
        elapsed_since_last = time.time() - self._groq_last_call
        if elapsed_since_last < min_interval:
            wait = min_interval - elapsed_since_last
            print(f"  [LLM] Groq rate limit: esperant {wait:.0f}s...",
                  file=sys.stderr, flush=True)
            time.sleep(wait)
        self._groq_last_call = time.time()

        last_err = None
        for attempt in range(1, 4):
            try:
                response = client.chat.completions.create(
                    model=self.model,
                    messages=[
                        {"role": "system", "content": system},
                        {"role": "user", "content": user_message},
                    ],
                    max_tokens=max_tokens,
                    temperature=temperature,
                )

                text = response.choices[0].message.content or ""
                tokens_in = getattr(response.usage, 'prompt_tokens', 0) or 0
                tokens_out = getattr(response.usage, 'completion_tokens', 0) or 0

                return {
                    "text": text,
                    "tokens_in": tokens_in,
                    "tokens_out": tokens_out,
                }

            except Exception as e:
                error_str = str(e)
                if "429" in error_str or "rate_limit" in error_str.lower():
                    wait = 30 * attempt
                    print(f"  [LLM] Groq attempt {attempt} rate limited. "
                          f"Retrying in {wait}s...",
                          file=sys.stderr, flush=True)
                    time.sleep(wait)
                    last_err = e
                    continue
                raise

        raise RuntimeError(
            f"Groq API failed after 3 attempts: {last_err}"
        ) from last_err

    # -- Ollama -------------------------------------------------------------

    def _call_ollama(
        self, system: str, user_message: str, max_tokens: int, temperature: float
    ) -> dict:
        import requests

        base_url = os.environ.get("OLLAMA_URL", "http://localhost:11434")
        url = f"{base_url}/api/chat"

        payload = {
            "model": self.model,
            "messages": [
                {"role": "system", "content": system},
                {"role": "user", "content": user_message},
            ],
            "stream": False,
            "format": "json",
            "options": {
                "temperature": temperature,
                "num_predict": max_tokens,
                "num_ctx": 8192,
            },
        }

        try:
            resp = requests.post(url, json=payload, timeout=900)
            resp.raise_for_status()
        except requests.ConnectionError:
            raise RuntimeError(
                f"Ollama server not reachable at {base_url}. "
                "Install from https://ollama.ai and run: ollama serve"
            )
        except requests.RequestException as e:
            raise RuntimeError(f"Ollama request failed: {e}") from e

        data = resp.json()
        text = data.get("message", {}).get("content", "")

        tokens_in = data.get("prompt_eval_count", 0) or 0
        tokens_out = data.get("eval_count", 0) or 0
        if not tokens_in:
            tokens_in = (len(system) + len(user_message)) // 4
        if not tokens_out:
            tokens_out = len(text) // 4

        return {
            "text": text,
            "tokens_in": tokens_in,
            "tokens_out": tokens_out,
        }


# ---------------------------------------------------------------------------
# Module-level singleton & convenience functions
# ---------------------------------------------------------------------------

_provider: LLMProvider | None = None
_lock = threading.Lock()


def get_provider() -> LLMProvider:
    """Get or create the singleton LLM provider."""
    global _provider
    if _provider is None:
        with _lock:
            if _provider is None:  # double-check
                backend = os.environ.get("LLM_PROVIDER", "gemini").lower()
                _provider = LLMProvider(backend)
    return _provider


def set_provider(backend: str):
    """Force a specific backend."""
    global _provider
    with _lock:
        _provider = LLMProvider(backend)


def call_llm(system: str, user_message: str, **kwargs) -> dict:
    """Convenience wrapper — calls get_provider().call(...)."""
    return get_provider().call(system, user_message, **kwargs)


# ---------------------------------------------------------------------------
# Quick self-test
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    from env_utils import load_local_env
    load_local_env()

    import sys
    backend = sys.argv[1] if len(sys.argv) > 1 else "claude"
    set_provider(backend)

    result = call_llm(
        system="Ets un assistent de test. Respon en una sola frase curta.",
        user_message="Digues 'hola' i el nom del model que ets.",
    )
    print(f"\n--- Result ---")
    print(f"Provider : {result['provider']}")
    print(f"Model    : {result['model']}")
    print(f"Tokens   : {result['tokens_in']} in + {result['tokens_out']} out")
    print(f"Time     : {result['elapsed_s']}s")
    print(f"Response : {result['text'][:200]}")
