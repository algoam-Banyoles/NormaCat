"""
norm_resolver.py — Normalise raw normative reference strings.

Parses any common phrasing of a law, decree, UNE/ISO norm, or named
technical code into a canonical (type, number, year) dict so that
different phrasings can be matched to the same catalog entry.

Pure functions, no I/O.
"""

from __future__ import annotations

import re
import unicodedata

# ─── Named-norm alias table ───────────────────────────────────────────────────
# Maps each canonical type to all accepted alias strings.
# Checked BEFORE generic regex patterns so specific RD aliases win.

_NAMED_ALIASES: dict[str, list[str]] = {
    "IAP":   ["IAP-11", "IAP 11", "IAP-98", "IAP 98",
               "Instrucció IAP", "Instruccio IAP", "Instrucción IAP"],
    "PG3":   ["PG-3", "PG3", "Plec General", "Pliego General"],
    "EHE":   ["EHE-08", "EHE 08", "EHE-99", "EHE 99",
               "Instrucció EHE", "Instruccion EHE", "EHE"],
    "NCSE":  ["NCSE-02", "NCSE 02", "NCSE-94", "NCSE 94", "NCSE"],
    "RIPCI": ["RIPCI", "RD 513/2017"],
    "REBT":  ["REBT", "RD 842/2002"],
    "RITE":  ["RITE", "RD 1027/2007"],
    "EAE":   ["EAE", "RD 751/2011"],
    "CTE":   ["CTE", "Codi Tècnic", "Codi Tecnic",
               "Código Técnico", "Codigo Tecnico"],
}

# Reverse lookup: normalised alias text → canonical type.
# Sorted later by descending length for longest-match-first search.
_NAMED_LOOKUP: dict[str, str] = {}

def _ascii_lower(s: str) -> str:
    return unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode().lower()

for _type, _aliases in _NAMED_ALIASES.items():
    for _alias in _aliases:
        _NAMED_LOOKUP[_alias.lower()] = _type
        _NAMED_LOOKUP[_ascii_lower(_alias)] = _type

_NAMED_SORTED = sorted(_NAMED_LOOKUP.items(), key=lambda x: -len(x[0]))

# ─── Regex patterns ───────────────────────────────────────────────────────────

# RD / Real Decreto / Reial Decret  (2- or 4-digit year)
_RE_RD = re.compile(
    r"\b(?:Reial\s+Decret|Real\s+Decreto|R\.?\s*D\.?)\s*"
    r"(?:n[úu]m\.?\s*)?(\d+)\s*/\s*(\d{2,4})\b",
    re.IGNORECASE,
)

# Llei / Ley  (not "L" alone — too short, too many false positives)
_RE_LLEI = re.compile(
    r"\b(?:Llei|Ley)\s+(?:n[úu]m\.?\s*)?(\d+)\s*/\s*(\d{2,4})\b",
    re.IGNORECASE,
)

# Decret / Decreto — negative lookbehind to skip "Reial/Real Decret/o"
_RE_DECRET = re.compile(
    r"(?<!\bReial\s)(?<!\bReal\s)\b(?:Decret|Decreto)\s+"
    r"(?:n[úu]m\.?\s*)?(\d+)\s*/\s*(\d{2,4})\b",
    re.IGNORECASE,
)

# Ordre / Orden — captures the full code like "HAC/1074/2014"
_RE_ORDRE = re.compile(
    r"\b(?:Ordre|Orden)\s+([A-Z]+(?:/[A-Z]+)?/\d+/\d{4})\b",
    re.IGNORECASE,
)

# EU Directives  (format: year/number/suffix)
_RE_DIRECTIVA = re.compile(
    r"\bDirecti(?:va|ve)\s+(\d+)/(\d+)/(?:CE|UE|EU|CEE)\b",
    re.IGNORECASE,
)

# NTE codes — e.g. NTE-EHV-012, NTE EHV 012, NTE-EHV012
_RE_NTE = re.compile(
    r"\bNTE[- ]?([A-Z]{2,4})[- ]?(\d{2,3})\b",
    re.IGNORECASE,
)

# UNE (including UNE-EN, UNE-EN ISO, UNE-ISO)
# group(1) = prefix (e.g. "UNE-EN ISO"),  group(2) = code (e.g. "9001:2015")
_RE_UNE = re.compile(
    r"\b(UNE(?:\s*[-]\s*EN)?(?:\s*[-]\s*ISO|\s+ISO)?(?:\s+EN)?)\s+"
    r"(\d+(?:[-:/]\d+)*)\b",
    re.IGNORECASE,
)

# ISO (standalone — not part of a UNE reference)
# Negative lookbehind avoids double-matching "UNE-EN ISO 9001"
_RE_ISO = re.compile(
    r"(?<!\bUNE[- ]EN\s)(?<!\bUNE\s)\bISO[/ ]?(\w[\d\s\-:.]*\d)\b",
    re.IGNORECASE,
)

# EN (standalone European norm — not preceded by UNE-)
_RE_EN = re.compile(
    r"(?<!UNE[-\s])\bEN\s+(\d+(?:[-:/]\d+)*)\b",
    re.IGNORECASE,
)

# ─── Code normalisation ───────────────────────────────────────────────────────

def _normalize_year(y: str) -> str:
    """Convert 2-digit year to full year; pass 4-digit year unchanged."""
    y = y.strip()
    if len(y) == 4:
        return y
    n = int(y)
    return f"19{y.zfill(2)}" if n > 50 else f"20{y.zfill(2)}"


def _clean_norm_code(code: str) -> str:
    """Normalise a UNE/ISO code fragment: uppercase, strip year suffix,
    collapse spaces/hyphens."""
    code = code.strip().upper()
    # Remove trailing year-and-beyond: ":2015", "/2015", ":2018/1M:2026", etc.
    code = re.sub(r"[:/]\d{4}.*$", "", code)
    # Spaces → hyphens, collapse repeated hyphens
    code = re.sub(r"\s+", "-", code)
    code = re.sub(r"-{2,}", "-", code)
    return code.strip("-")


# ─── Public API ───────────────────────────────────────────────────────────────

def resolve(raw_text: str) -> dict | None:
    """Parse *raw_text* and return a canonical dict, or None if unrecognised.

    Returned dict keys:
        type    — "RD", "LLEI", "DECRET", "ORDRE", "DIRECTIVA",
                  "UNE", "ISO", "EN", "NTE", or a named-norm type
        number  — numeric part (str) or None
        year    — 4-digit year (str) or None
        suffix  — canonical code string for UNE/ISO/EN/NTE, or None
        raw     — original input unchanged
    """
    if not raw_text or not raw_text.strip():
        return None

    text = raw_text.strip()
    text_lower = text.lower()
    text_ascii = _ascii_lower(text)

    # ── 1. Named aliases (longest match first) ────────────────────────────────
    for alias, typ in _NAMED_SORTED:
        if alias in text_lower or alias in text_ascii:
            return {
                "type": typ,
                "number": None,
                "year": None,
                "suffix": None,
                "raw": raw_text,
            }

    # ── 2. RD ─────────────────────────────────────────────────────────────────
    m = _RE_RD.search(text)
    if m:
        return {
            "type": "RD",
            "number": m.group(1),
            "year": _normalize_year(m.group(2)),
            "suffix": None,
            "raw": raw_text,
        }

    # ── 3. Llei / Ley ─────────────────────────────────────────────────────────
    m = _RE_LLEI.search(text)
    if m:
        return {
            "type": "LLEI",
            "number": m.group(1),
            "year": _normalize_year(m.group(2)),
            "suffix": None,
            "raw": raw_text,
        }

    # ── 4. Decret / Decreto ───────────────────────────────────────────────────
    m = _RE_DECRET.search(text)
    if m:
        return {
            "type": "DECRET",
            "number": m.group(1),
            "year": _normalize_year(m.group(2)),
            "suffix": None,
            "raw": raw_text,
        }

    # ── 5. Ordre / Orden ──────────────────────────────────────────────────────
    m = _RE_ORDRE.search(text)
    if m:
        return {
            "type": "ORDRE",
            "number": None,
            "year": None,
            "suffix": m.group(1).upper(),
            "raw": raw_text,
        }

    # ── 6. Directiva ──────────────────────────────────────────────────────────
    m = _RE_DIRECTIVA.search(text)
    if m:
        return {
            "type": "DIRECTIVA",
            "number": m.group(1),
            "year": m.group(2),
            "suffix": None,
            "raw": raw_text,
        }

    # ── 7. NTE ────────────────────────────────────────────────────────────────
    m = _RE_NTE.search(text)
    if m:
        suffix = f"NTE-{m.group(1).upper()}-{m.group(2)}"
        return {
            "type": "NTE",
            "number": m.group(2),
            "year": None,
            "suffix": suffix,
            "raw": raw_text,
        }

    # ── 8. UNE (including UNE-EN, UNE-EN ISO) ────────────────────────────────
    m = _RE_UNE.search(text)
    if m:
        prefix = _clean_norm_code(m.group(1))   # "UNE-EN-ISO"
        code   = _clean_norm_code(m.group(2))   # "9001"
        suffix = f"{prefix}-{code}"
        return {
            "type": "UNE",
            "number": None,
            "year": None,
            "suffix": suffix,
            "raw": raw_text,
        }

    # ── 9. ISO (standalone) ───────────────────────────────────────────────────
    m = _RE_ISO.search(text)
    if m:
        code = _clean_norm_code(m.group(1))
        return {
            "type": "ISO",
            "number": None,
            "year": None,
            "suffix": f"ISO-{code}",
            "raw": raw_text,
        }

    # ── 10. EN (standalone) ───────────────────────────────────────────────────
    m = _RE_EN.search(text)
    if m:
        code = _clean_norm_code(m.group(1))
        return {
            "type": "EN",
            "number": None,
            "year": None,
            "suffix": f"EN-{code}",
            "raw": raw_text,
        }

    return None


def normalize_code(resolved: dict) -> str:
    """Return a single canonical string key for deduplication/lookup."""
    typ    = resolved.get("type", "")
    number = resolved.get("number")
    year   = resolved.get("year")
    suffix = resolved.get("suffix")

    if typ in ("UNE", "ISO", "EN", "NTE"):
        return (suffix or "").upper()
    if typ == "ORDRE":
        return f"ORDRE-{(suffix or '').upper()}"
    if number and year:
        return f"{typ}-{number}/{year}"
    if number:
        return f"{typ}-{number}"
    if suffix:
        return f"{typ}-{suffix.upper()}"
    return typ


# ─── __main__ self-test ───────────────────────────────────────────────────────

if __name__ == "__main__":
    import sys
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    _TESTS: list[tuple[str, dict]] = [
        ("RD 1627/1997",
         {"type": "RD",        "number": "1627", "year": "1997"}),
        ("Real Decreto 1627/1997",
         {"type": "RD",        "number": "1627", "year": "1997"}),
        ("Reial Decret 1627/1997, de 24 d'octubre",
         {"type": "RD",        "number": "1627", "year": "1997"}),
        ("R.D. 1627/97",
         {"type": "RD",        "number": "1627", "year": "1997"}),
        ("Llei 9/2017, de 8 de novembre",
         {"type": "LLEI",      "number": "9",    "year": "2017"}),
        ("Directiva 89/391/CEE",
         {"type": "DIRECTIVA", "number": "89",   "year": "391"}),
        ("UNE-EN ISO 9001:2015",
         {"type": "UNE",       "suffix": "UNE-EN-ISO-9001"}),
        ("ISO 9001:2015",
         {"type": "ISO",       "suffix": "ISO-9001"}),
        ("NTE-EHV 012",
         {"type": "NTE",       "suffix": "NTE-EHV-012"}),
        ("Codi Tècnic de l'Edificació CTE",
         {"type": "CTE"}),
    ]

    passed = 0
    for raw, expected in _TESTS:
        result = resolve(raw)
        ok = result is not None and all(
            result.get(k) == v for k, v in expected.items()
        )
        tag = "PASS" if ok else "FAIL"
        if ok:
            passed += 1
        key = normalize_code(result) if result else "None"
        print(f"  [{tag}] {raw!r:50s}  ->  {key}")

    print(f"\n{passed}/{len(_TESTS)} passed")
