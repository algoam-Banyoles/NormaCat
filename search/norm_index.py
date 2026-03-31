"""
norm_index.py — In-memory index of all downloaded normative catalogs.

Loads and indexes:
  • normativa_annexes.json
  • normativa_adif/_catalogo/catalogo_adif.json
  • normativa_iso/_catalogo/catalogo_iso.json
  • normativa_une/_catalogo/catalogo_une.json
  • normativa_dgc/_catalogo/catalogo_completo.json

Provides O(1) lookup of any normative reference by its canonical code,
with fuzzy fallback for near-miss number typos.

Usage:
    from norm_index import NormIndex
    idx = NormIndex(".")           # base_dir = project root
    result = idx.lookup("RD 1627/1997")
    # → {"status": "VIGENT", "source": "ANNEXES", "title": "...", ...}
"""

from __future__ import annotations

import json
import logging
import os
import re

from search.norm_resolver import normalize_code, resolve

logger = logging.getLogger(__name__)

# ─── Priority order when the same key appears in multiple catalogs ────────────
_PRIORITY = {"DEROGADA": 3, "VIGENT": 2, "REFERENCIA": 1, "PENDENT": 0}


class NormIndex:
    """In-memory lookup index built from all downloaded normative catalogs."""

    def __init__(self, base_dir: str = None) -> None:
        if base_dir is None:
            from config import PROJECT_ROOT
            base_dir = PROJECT_ROOT
        self._base_dir = os.path.abspath(base_dir)
        # canonical_key → entry dict
        self._index: dict[str, dict] = {}
        # loaded-entry counts per source (includes unresolvable entries)
        self._counts: dict[str, int] = {
            "ANNEXES": 0, "DGC": 0, "ADIF": 0, "ISO": 0, "UNE": 0,
            "BOE": 0, "INDUSTRIA": 0, "PJCAT": 0, "TERRITORI": 0,
            "ACA": 0, "CTE": 0, "ERA": 0, "MITMA_F": 0,
        }
        self._load_all()

    def _catalog_path(self, old_folder: str, filename: str) -> str:
        """Resol el path d'un cataleg, suportant estructura antiga i nova.

        Estructura antiga: normativa_adif/_catalogo/catalogo_adif.json
        Estructura NormaCat: catalogs/adif/catalogo_adif.json
        """
        # Provar estructura NormaCat (catalogs/<source>/filename)
        source = old_folder.replace("normativa_", "")
        new_path = os.path.join(self._base_dir, "catalogs", source, filename)
        if os.path.exists(new_path):
            return new_path
        # Provar estructura original (normativa_X/_catalogo/filename)
        old_path = os.path.join(self._base_dir, old_folder, "_catalogo", filename)
        if os.path.exists(old_path):
            return old_path
        return new_path  # retornar el nou path (generara FileNotFoundError)

    # ─── Internal helpers ─────────────────────────────────────────────────────

    def _store(self, key: str, entry: dict) -> None:
        """Insert *entry* under *key*.

        Cross-source: DEROGADA > VIGENT > REFERENCIA (authoritative derogation).
        Same-source:  VIGENT > DEROGADA (newer edition supersedes old within
        the same catalog, e.g. UNE-EN 10025-2:2019 VIGENTE vs :2004 ANULADA).
        """
        existing = self._index.get(key)
        if existing is None:
            self._index[key] = entry
            return

        same_source = entry.get("source") == existing.get("source")
        if same_source:
            # Within the same catalog, VIGENT wins over DEROGADA
            # (a newer vigent edition supersedes an older anulada one).
            if entry["status"] == "VIGENT" and existing["status"] == "DEROGADA":
                self._index[key] = entry
            elif entry["status"] == "DEROGADA" and existing["status"] == "VIGENT":
                pass  # keep existing VIGENT
            elif _PRIORITY.get(entry["status"], 0) > _PRIORITY.get(existing["status"], 0):
                self._index[key] = entry
        else:
            # Cross-source: original priority (DEROGADA > VIGENT)
            if _PRIORITY.get(entry["status"], 0) > _PRIORITY.get(existing["status"], 0):
                self._index[key] = entry

    def _store_direct(self, key: str, entry: dict) -> None:
        """Store entry under a raw key (bypasses norm_resolver).
        Lower priority: only stores if key not already present."""
        if key and key not in self._index:
            self._index[key] = entry

    def _index_entry(self, ref_text: str, entry: dict) -> bool:
        """Resolve *ref_text*, store *entry* if resolvable; return True on hit."""
        resolved = resolve(ref_text)
        if resolved is None:
            return False
        self._store(normalize_code(resolved), entry)
        return True

    # ─── Catalog loaders ─────────────────────────────────────────────────────

    def _load_annexes(self) -> None:
        # Provar data/ (NormaCat) o data/ (original)
        path = os.path.join(self._base_dir, "data", "normativa_annexes.json")
        if not os.path.exists(path):
            logger.warning("normativa_annexes.json not found — skipped")
            return

        with open(path, encoding="utf-8") as fh:
            data = json.load(fh)

        count = 0

        # annex entries (default VIGENT, may be DEROGADA via observacions)
        for annex in data.get("annexes", []):
            norms = annex.get("normativa", [])
            # tolerate flat list at top level too
            if isinstance(annex, dict) and "codi" in annex:
                norms = [annex]
            for norm in norms:
                codi = norm.get("codi", "")
                text = norm.get("text", "")
                obs  = norm.get("observacions", "") or ""

                # Detect PASSIVE derogation: "derogat per X", "substituïda per X".
                # Must NOT match ACTIVE form "Deroga X" (this norm derogates others).
                is_derogada = bool(re.search(
                    r"DEROGAD[AO]\s+per\b|[Dd]erogat\s+per\b"
                    r"|substitu[ïi]da\s+per\b|[Ss]ubstituir\s+per\b"
                    r"|ha\s+estat\s+derogad|ha\s+sido\s+derogad"
                    r"|anulad[ao]\s+per\b|anulado\s+por\b"
                    r"|[Nn]o\s+(?:vigent|vigente)",
                    obs, re.IGNORECASE
                ))
                status = "DEROGADA" if is_derogada else "VIGENT"

                sub = None
                if is_derogada:
                    m = re.search(
                        r"(?:substitu[ïi]da|reemplazada|substituir)\s+per\s+(.+?)(?:\.|$)",
                        obs, re.IGNORECASE
                    )
                    if m:
                        sub = m.group(1).strip()

                entry = {
                    "status": status,
                    "source": "ANNEXES",
                    "title": text or codi,
                    "raw_ref": codi,
                    "substituted_by": sub,
                }
                # Try codi first, fall back to first 120 chars of text
                if not self._index_entry(codi, entry):
                    self._index_entry(text[:120], entry)
                count += 1

        # explicitly derogated list
        derogada_list = data.get("normativa_derogada", [])
        # may be a list of entries or a dict organised by id
        if isinstance(derogada_list, dict):
            derogada_list = list(derogada_list.values())
        for norm in derogada_list:
            if not isinstance(norm, dict):
                continue
            codi = norm.get("codi", "")
            text = norm.get("text", "")
            obs  = norm.get("observacions", "") or ""
            entry = {
                "status": "DEROGADA",
                "source": "ANNEXES",
                "title": text or codi,
                "raw_ref": codi,
                "substituted_by": obs or None,
            }
            if not self._index_entry(codi, entry):
                self._index_entry(text[:120], entry)
            count += 1

        self._counts["ANNEXES"] = count
        logger.debug("ANNEXES: %d entries loaded", count)

    def _load_adif(self) -> None:
        path = self._catalog_path("normativa_adif", "catalogo_adif.json")
        if not os.path.exists(path):
            logger.warning("catalogo_adif.json not found — skipped")
            return

        with open(path, encoding="utf-8") as fh:
            entries = json.load(fh)

        count = 0
        for entry in entries:
            titulo = entry.get("titulo", "")
            codigo = entry.get("codigo", "")
            estado = (entry.get("estado") or "").lower()

            status = "DEROGADA" if any(
                w in estado for w in ("derogad", "retirad", "anulad")
            ) else "VIGENT"

            catalog_entry = {
                "status": status,
                "source": "ADIF",
                "title": titulo,
                "raw_ref": codigo,
                "substituted_by": None,
            }
            # ADIF titles are plain descriptions; try codigo first (NTE codes)
            if not self._index_entry(codigo, catalog_entry):
                self._index_entry(titulo, catalog_entry)
            count += 1

        self._counts["ADIF"] = count
        logger.debug("ADIF: %d entries loaded", count)

    def _load_iso(self) -> None:
        path = self._catalog_path("normativa_iso", "catalogo_iso.json")
        if not os.path.exists(path):
            logger.warning("catalogo_iso.json not found — skipped")
            return

        with open(path, encoding="utf-8") as fh:
            entries = json.load(fh)

        _iso_vigent = {"VIGENT", "PUBLISHED", "PUBLICADA"}
        _iso_derog  = {"RETIRADA", "WITHDRAWN", "DEROGADA", "ANULADA"}

        count = 0
        for entry in entries:
            ref   = entry.get("referencia", "")
            titol = entry.get("titol", "")
            estat = (entry.get("estat") or "").upper()
            sub   = entry.get("substituida_per", "") or None

            if estat in _iso_derog:
                status = "DEROGADA"
            else:
                status = "VIGENT"  # includes DESCONEGUT — assume valid until known otherwise

            catalog_entry = {
                "status": status,
                "source": "ISO",
                "title": titol or ref,
                "raw_ref": ref,
                "substituted_by": sub,
            }
            self._index_entry(ref, catalog_entry)
            count += 1

        self._counts["ISO"] = count
        logger.debug("ISO: %d entries loaded", count)

    def _load_une(self) -> None:
        path = self._catalog_path("normativa_une", "catalogo_une.json")
        if not os.path.exists(path):
            logger.warning("catalogo_une.json not found — skipped")
            return

        with open(path, encoding="utf-8") as fh:
            entries = json.load(fh)

        _une_derog = {"ANULADA", "DEROGADA", "WITHDRAWN", "RETIRADA"}

        count = 0
        for entry in entries:
            ref   = entry.get("referencia", "")
            estat = (entry.get("estat") or "").upper()
            desc  = entry.get("descripcio", "") or ""

            status = "DEROGADA" if estat in _une_derog else "VIGENT"

            catalog_entry = {
                "status": status,
                "source": "UNE",
                "title": desc or ref,
                "raw_ref": ref,
                "substituted_by": None,
            }
            self._index_entry(ref, catalog_entry)
            count += 1

        self._counts["UNE"] = count
        logger.debug("UNE: %d entries loaded", count)

    def _load_dgc(self) -> None:
        path = self._catalog_path("normativa_dgc", "catalogo_completo.json")
        if not os.path.exists(path):
            logger.warning("catalogo_completo.json not found — skipped")
            return

        with open(path, encoding="utf-8") as fh:
            data = json.load(fh)

        # Accept both flat list and legacy dict-of-lists
        if isinstance(data, dict):
            entries: list = []
            for val in data.values():
                if isinstance(val, list):
                    entries.extend(val)
                elif isinstance(val, dict):
                    entries.append(val)
        else:
            entries = data

        _dgc_status = {
            "normativa":  "VIGENT",
            "referencia": "REFERENCIA",
            "historica":  "DEROGADA",
        }

        count = 0
        for entry in entries:
            titol = entry.get("titol", "")
            tipus = (entry.get("tipus") or "normativa").lower()
            status = _dgc_status.get(tipus, "VIGENT")

            catalog_entry = {
                "status": status,
                "source": "DGC",
                "title": titol,
                "raw_ref": titol,
                "substituted_by": None,
            }
            self._index_entry(titol, catalog_entry)
            count += 1

        self._counts["DGC"] = count
        logger.debug("DGC: %d entries loaded", count)

    def _load_boe(self) -> None:
        path = self._catalog_path("normativa_boe", "catalogo_boe.json")
        if not os.path.exists(path):
            logger.warning("catalogo_boe.json not found — skipped")
            return
        with open(path, encoding="utf-8") as fh:
            entries = json.load(fh)
        if not isinstance(entries, list):
            logger.warning("catalogo_boe.json format unexpected — skipped")
            return
        count = 0
        for entry in entries:
            codi      = (entry.get("codi") or entry.get("id") or "").strip()
            titol     = (entry.get("text") or "").strip()
            estat_raw = (entry.get("estat") or "vigent").lower()
            if "derog" in estat_raw or "anulad" in estat_raw or "historic" in estat_raw:
                status = "DEROGADA"
            else:
                status = "VIGENT"
            ref_text = titol or codi
            if not ref_text:
                continue
            catalog_entry = {
                "status": status, "source": "BOE", "title": titol,
                "raw_ref": ref_text, "substituted_by": None, "codi": codi,
            }
            before = len(self._index)
            self._index_entry(ref_text, catalog_entry)
            if codi and codi != ref_text:
                self._index_entry(codi, catalog_entry)
            # direct keys: normalised codi + title
            if codi:
                codi_norm = " ".join(codi.upper().split())
                self._store_direct(codi_norm, catalog_entry)
                codi_alt = re.sub(r"[-/]", " ", codi_norm).strip()
                if codi_alt != codi_norm:
                    self._store_direct(codi_alt, catalog_entry)
            if titol and len(titol) >= 10:
                self._store_direct(titol[:120].upper(), catalog_entry)
            if len(self._index) > before:
                count += 1
        self._counts["BOE"] = count
        logger.debug("BOE: %d entries loaded", count)

    def _load_industria(self) -> None:
        path = self._catalog_path("normativa_industria", "catalogo_industria.json")
        if not os.path.exists(path):
            logger.warning("catalogo_industria.json not found — skipped")
            return
        with open(path, encoding="utf-8") as fh:
            entries = json.load(fh)
        if not isinstance(entries, list):
            logger.warning("catalogo_industria.json format unexpected — skipped")
            return
        count = 0
        for entry in entries:
            boe_id    = (entry.get("boe_id") or "").strip()
            titol     = (entry.get("titol") or "").strip()
            estat_raw = (entry.get("estat") or "vigent").lower()
            if "derog" in estat_raw or "anulad" in estat_raw:
                status = "DEROGADA"
            else:
                status = "VIGENT"
            ref_text = titol or boe_id
            if not ref_text:
                continue
            catalog_entry = {
                "status": status, "source": "INDUSTRIA", "title": titol,
                "raw_ref": ref_text, "substituted_by": None, "codi": boe_id,
            }
            before = len(self._index)
            self._index_entry(ref_text, catalog_entry)
            if boe_id and boe_id != ref_text:
                self._index_entry(boe_id, catalog_entry)
            if boe_id:
                codi_norm = " ".join(boe_id.upper().split())
                self._store_direct(codi_norm, catalog_entry)
                codi_alt = re.sub(r"[-/]", " ", codi_norm).strip()
                if codi_alt != codi_norm:
                    self._store_direct(codi_alt, catalog_entry)
            if titol and len(titol) >= 10:
                self._store_direct(titol[:120].upper(), catalog_entry)
            if len(self._index) > before:
                count += 1
        self._counts["INDUSTRIA"] = count
        logger.debug("INDUSTRIA: %d entries loaded", count)

    def _load_pjcat(self) -> None:
        path = self._catalog_path("normativa_pjcat", "catalogo_pjcat.json")
        if not os.path.exists(path):
            logger.warning("catalogo_pjcat.json not found — skipped")
            return
        with open(path, encoding="utf-8") as fh:
            entries = json.load(fh)
        if not isinstance(entries, list):
            logger.warning("catalogo_pjcat.json format unexpected — skipped")
            return
        count = 0
        for entry in entries:
            codi         = (entry.get("codi") or entry.get("id") or "").strip()
            titol        = (entry.get("text") or "").strip()
            derogada_per = (entry.get("derogada_per") or "").strip()
            estat_raw    = (entry.get("estat") or "vigent").lower()
            if "derog" in estat_raw or "anulad" in estat_raw or derogada_per:
                status = "DEROGADA"
            else:
                status = "VIGENT"
            ref_text = titol or codi
            if not ref_text:
                continue
            catalog_entry = {
                "status": status, "source": "PJCAT", "title": titol,
                "raw_ref": ref_text, "substituted_by": derogada_per or None, "codi": codi,
            }
            before = len(self._index)
            self._index_entry(ref_text, catalog_entry)
            if codi and codi != ref_text:
                self._index_entry(codi, catalog_entry)
            if codi:
                codi_norm = " ".join(codi.upper().split())
                self._store_direct(codi_norm, catalog_entry)
                codi_alt = re.sub(r"[-/]", " ", codi_norm).strip()
                if codi_alt != codi_norm:
                    self._store_direct(codi_alt, catalog_entry)
            if titol and len(titol) >= 10:
                self._store_direct(titol[:120].upper(), catalog_entry)
            if len(self._index) > before:
                count += 1
        self._counts["PJCAT"] = count
        logger.debug("PJCAT: %d entries loaded", count)

    def _load_territori(self) -> None:
        path = self._catalog_path("normativa_territori", "catalogo_territori.json")
        if not os.path.exists(path):
            logger.warning("catalogo_territori.json not found — skipped")
            return
        with open(path, encoding="utf-8") as fh:
            entries = json.load(fh)
        if not isinstance(entries, list):
            logger.warning("catalogo_territori.json format unexpected — skipped")
            return
        count = 0
        for entry in entries:
            codi      = (entry.get("codi") or entry.get("id") or "").strip()
            titol     = (entry.get("text") or "").strip()
            deroga    = (entry.get("deroga") or "").strip()
            estat_raw = (entry.get("estat") or "vigent").lower()
            if "derog" in estat_raw or "anulad" in estat_raw:
                status = "DEROGADA"
            else:
                status = "VIGENT"
            ref_text = titol or codi
            if not ref_text:
                continue
            catalog_entry = {
                "status": status, "source": "TERRITORI", "title": titol,
                "raw_ref": ref_text, "substituted_by": deroga or None, "codi": codi,
            }
            before = len(self._index)
            self._index_entry(ref_text, catalog_entry)
            if codi and codi != ref_text:
                self._index_entry(codi, catalog_entry)
            if codi:
                codi_norm = " ".join(codi.upper().split())
                self._store_direct(codi_norm, catalog_entry)
                codi_alt = re.sub(r"[-/]", " ", codi_norm).strip()
                if codi_alt != codi_norm:
                    self._store_direct(codi_alt, catalog_entry)
            if titol and len(titol) >= 10:
                self._store_direct(titol[:120].upper(), catalog_entry)
            if len(self._index) > before:
                count += 1
        self._counts["TERRITORI"] = count
        logger.debug("TERRITORI: %d entries loaded", count)

    def _load_wrapped(self, source: str, folder: str, filename: str) -> None:
        """Load a catalog whose JSON is a metadata-wrapped dict with a 'documents' list."""
        path = self._catalog_path(folder, filename)
        if not os.path.exists(path):
            logger.warning("%s not found — skipped", filename)
            self._counts[source] = 0
            return
        with open(path, encoding="utf-8") as fh:
            data = json.load(fh)
        # Resolve document list from wrapper dict or bare list
        entries = None
        if isinstance(data, list):
            entries = data
        elif isinstance(data, dict):
            for key in ("documents", "normes", "items", "resultats", "legislacio", "llista", "data"):
                if key in data and isinstance(data[key], list):
                    entries = data[key]
                    break
        if not entries:
            logger.warning(
                "%s: no document list found (keys: %s)",
                filename,
                list(data.keys()) if isinstance(data, dict) else "list",
            )
            self._counts[source] = 0
            return
        count = 0
        for entry in entries:
            if not isinstance(entry, dict):
                continue
            titol = (
                entry.get("titol") or entry.get("title") or
                entry.get("text") or entry.get("nom") or ""
            ).strip()
            codi      = (entry.get("codi") or entry.get("id") or entry.get("boe_id") or "").strip()
            estat_raw = (entry.get("estat") or entry.get("status") or "vigent").lower()
            if "derog" in estat_raw or "anulad" in estat_raw or "historic" in estat_raw:
                status = "DEROGADA"
            else:
                status = "VIGENT"
            ref_text = titol or codi
            if not ref_text:
                continue
            catalog_entry = {
                "status": status, "source": source, "title": titol,
                "raw_ref": ref_text, "substituted_by": None, "codi": codi,
            }
            before = len(self._index)
            self._index_entry(ref_text, catalog_entry)
            if codi and codi != ref_text:
                self._index_entry(codi, catalog_entry)
            if codi:
                codi_norm = " ".join(codi.upper().split())
                self._store_direct(codi_norm, catalog_entry)
                codi_alt = re.sub(r"[-/]", " ", codi_norm).strip()
                if codi_alt != codi_norm:
                    self._store_direct(codi_alt, catalog_entry)
            if titol and len(titol) >= 10:
                self._store_direct(titol[:120].upper(), catalog_entry)
            if len(self._index) > before:
                count += 1
        self._counts[source] = count
        logger.debug("%s: %d entries loaded", source, count)

    def _load_aca(self) -> None:
        self._load_wrapped("ACA", "normativa_aca", "catalogo_aca.json")

    def _load_cte(self) -> None:
        self._load_wrapped("CTE", "normativa_cte", "catalogo_cte.json")

    def _load_era(self) -> None:
        self._load_wrapped("ERA", "normativa_era", "catalogo_era.json")

    def _load_mitma_ferroviari(self) -> None:
        self._load_wrapped("MITMA_F", "normativa_mitma_ferroviari", "catalogo_mitma_ferroviari.json")

    def _load_all(self) -> None:
        self._index = {}
        self._counts = {
            "ANNEXES": 0, "DGC": 0, "ADIF": 0, "ISO": 0, "UNE": 0,
            "BOE": 0, "INDUSTRIA": 0, "PJCAT": 0, "TERRITORI": 0,
            "ACA": 0, "CTE": 0, "ERA": 0, "MITMA_F": 0,
        }
        self._load_annexes()
        self._load_adif()
        self._load_iso()
        self._load_une()
        self._load_dgc()
        self._load_boe()
        self._load_industria()
        self._load_pjcat()
        self._load_territori()
        self._load_aca()
        self._load_cte()
        self._load_era()
        self._load_mitma_ferroviari()

    # ─── Public API ───────────────────────────────────────────────────────────

    def lookup(self, raw_text: str) -> dict | None:
        """Look up *raw_text* in the index.

        Returns:
          • The matching entry dict (with all stored fields) on exact hit.
          • The same dict with ``"fuzzy": True`` added on near-miss hit.
          • ``{"status": "PENDENT", "source": None, ...}`` when resolvable
            but not found in any catalog.
          • ``None`` when the string cannot be parsed by norm_resolver.
        """
        resolved = resolve(raw_text)
        if resolved is None:
            return None

        key = normalize_code(resolved)

        # Exact match
        if key in self._index:
            return self._index[key]

        # Fuzzy: same type, numeric number within ±4
        # IMPORTANT: when the query specifies a year, only candidates with
        # the SAME year (or no year) are considered.  This prevents
        # RD 1247/2008 from fuzzy-matching RD 1247/1995 (different norm).
        typ = resolved.get("type")
        num = resolved.get("number")
        query_year = resolved.get("year")  # e.g. "2008" or None
        if typ and num and isinstance(num, str) and num.isdigit():
            num_int = int(num)
            best: dict | None = None
            best_dist = 5  # exclusive upper bound
            prefix = f"{typ}-"
            for k, entry in self._index.items():
                if not k.startswith(prefix):
                    continue
                rest = k[len(prefix):]
                parts = rest.split("/")
                cand = parts[0]
                if not cand.isdigit():
                    continue
                # Year gate: if query has year AND candidate has year,
                # they must match exactly.
                if query_year and len(parts) >= 2 and parts[1].isdigit():
                    if parts[1] != query_year:
                        continue
                dist = abs(int(cand) - num_int)
                if dist < best_dist:
                    best = dict(entry)
                    best["fuzzy"] = True
                    best_dist = dist
            if best:
                return best

        # Direct key fallback: try raw text as a key (for non-parseable catalog entries)
        raw_upper = raw_text.strip().upper()
        if raw_upper in self._index:
            return self._index[raw_upper]
        raw_compact = " ".join(raw_upper.split())
        if raw_compact in self._index:
            return self._index[raw_compact]

        return {
            "status": "PENDENT",
            "source": None,
            "title": None,
            "raw_ref": raw_text,
            "substituted_by": None,
        }

    def stats(self) -> dict:
        """Return counts: total indexed, per source (loaded), per status."""
        status_counts: dict[str, int] = {}
        for entry in self._index.values():
            s = entry.get("status", "UNKNOWN")
            status_counts[s] = status_counts.get(s, 0) + 1
        return {
            "total_indexed": len(self._index),
            "per_source":    dict(self._counts),
            "per_status":    status_counts,
        }

    def reload(self) -> None:
        """Re-read all catalog files from disk."""
        self._load_all()


# ─── __main__ ─────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import sys

    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")

    logging.basicConfig(level=logging.WARNING)

    base = sys.argv[1] if len(sys.argv) > 1 else "."
    idx = NormIndex(base)
    s = idx.stats()
    src = s["per_source"]
    total = s["total_indexed"]
    print(f"Index carregat: {total} normes indexades")
    for font, n in sorted(src.items()):
        if n > 0:
            print(f"  {font}: {n}")
    print(f"Estat: {s['per_status']}")
