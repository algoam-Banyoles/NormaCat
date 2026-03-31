"""
catalog_linker.py — Reconciles catalog JSON entries with PDF files on disk.

For each normativa source:
  1. Loads the JSON catalog
  2. Scans PDF directories recursively
  3. Matches PDFs to catalog entries (exact path -> filename -> URL -> fuzzy title)
  4. Updates the catalog JSON with fitxer_local / path_local paths
  5. Reports orphan PDFs and missing PDFs

Usage:
    python scrapers/catalog_linker.py              # link and update JSONs
    python scrapers/catalog_linker.py --dry-run    # report only
"""
from __future__ import annotations

import argparse
import json
import os
import re
import shutil
from datetime import datetime
from difflib import SequenceMatcher
from urllib.parse import unquote, urlparse

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# ── Source definitions ─────────────────────────────────────────

SOURCES = {
    "dgc": {
        "label": "DGC",
        "catalog": os.path.join("normativa_dgc", "_catalogo", "catalogo_completo.json"),
        "pdf_dirs": ["normativa_dgc"],
        "path_field": "fitxer_local",
        "id_field": "titol",
        "code_fields": ["titol"],
        "url_field": "url_original",
    },
    "adif": {
        "label": "ADIF",
        "catalog": os.path.join("normativa_adif", "_catalogo", "catalogo_adif_complet.json"),
        "pdf_dirs": ["normativa_adif"],
        "path_field": None,  # nested in fitxers[]
        "id_field": "codigo",
        "code_fields": ["codigo", "titulo"],
        "url_field": None,
        "nested_pdf": True,  # special handling
    },
    "boe": {
        "label": "BOE",
        "catalog": os.path.join("normativa_boe", "_catalogo", "catalogo_boe.json"),
        "pdf_dirs": ["normativa_boe"],
        "path_field": "path_local",
        "id_field": "id",
        "code_fields": ["id", "codi"],
        "url_field": "url_pdf",
    },
    "cte": {
        "label": "CTE",
        "catalog": os.path.join("normativa_cte", "_catalogo", "catalogo_cte.json"),
        "pdf_dirs": ["normativa_cte"],
        "path_field": "path_local",
        "id_field": "codi",
        "code_fields": ["codi"],
        "url_field": "url_pdf",
    },
    "territori": {
        "label": "Territori",
        "catalog": os.path.join("normativa_territori", "_catalogo", "catalogo_territori.json"),
        "pdf_dirs": ["normativa_territori"],
        "path_field": "path_local",
        "id_field": "codi",
        "code_fields": ["codi", "id"],
        "url_field": "url_pdf",
    },
    "industria": {
        "label": "Industria",
        "catalog": os.path.join("normativa_industria", "_catalogo", "catalogo_industria.json"),
        "pdf_dirs": ["normativa_industria"],
        "path_field": "path_local",
        "id_field": "boe_id",
        "code_fields": ["boe_id"],
        "url_field": "url_pdf",
    },
    "pjcat": {
        "label": "PJCAT",
        "catalog": os.path.join("normativa_pjcat", "_catalogo", "catalogo_pjcat.json"),
        "pdf_dirs": ["normativa_pjcat"],
        "path_field": "path_local",
        "id_field": "id",
        "code_fields": ["id", "codi"],
        "url_field": "url_pdf",
    },
}


def _norm(path_str: str) -> str:
    """Normalize a path for comparison: lowercase, forward slashes."""
    return path_str.replace("\\", "/").lower().strip()


def _clean_name(filename: str) -> str:
    """Convert filename to a comparable string: no ext, underscores->spaces, lower."""
    stem = os.path.splitext(filename)[0]
    return re.sub(r"[_\-]+", " ", stem).lower().strip()


def _url_basename(url: str) -> str:
    """Extract the filename from a URL."""
    if not url:
        return ""
    parsed = urlparse(url)
    return unquote(os.path.basename(parsed.path)).lower()


# ── Scan PDFs ──────────────────────────────────────────────────

def _scan_pdfs(pdf_dirs: list[str]) -> dict[str, str]:
    """Scan directories for PDFs. Returns {normalized_relative_path: absolute_path}."""
    pdfs = {}
    for dir_rel in pdf_dirs:
        dir_abs = os.path.join(BASE_DIR, dir_rel)
        if not os.path.exists(dir_abs):
            continue
        for root, dirs, files in os.walk(dir_abs):
            dirs[:] = [d for d in dirs if not d.startswith("_")]
            for f in files:
                if f.lower().endswith(".pdf"):
                    abs_path = os.path.join(root, f)
                    rel_path = os.path.relpath(abs_path, BASE_DIR)
                    pdfs[_norm(rel_path)] = abs_path
    return pdfs


# ── Match logic ────────────────────────────────────────────────

def _match_entries(
    entries: list[dict],
    pdfs: dict[str, str],
    source_cfg: dict,
) -> tuple[dict[int, str], list[str], list[str]]:
    """Match catalog entries to PDF files.

    Returns:
        matched: {entry_index: normalized_pdf_path}
        orphan_pdfs: list of PDF paths not matched to any entry
        missing_pdfs: list of entry IDs that have no PDF
    """
    path_field = source_cfg.get("path_field")
    code_fields = source_cfg.get("code_fields", [])
    url_field = source_cfg.get("url_field")
    is_nested = source_cfg.get("nested_pdf", False)

    matched = {}           # entry_idx -> norm_pdf_path
    used_pdfs = set()      # norm_pdf_paths that have been matched
    pdf_name_index = {}    # lowercase_filename -> norm_path (for filename matching)

    for norm_path in pdfs:
        fname = os.path.basename(norm_path).lower()
        pdf_name_index.setdefault(fname, []).append(norm_path)

    # Pass 1: EXACT — entry already has a path that matches a real file
    for idx, entry in enumerate(entries):
        if is_nested:
            # ADIF: check fitxers[].fitxer_local
            for fobj in entry.get("fitxers", []):
                local = fobj.get("fitxer_local", "")
                if local:
                    norm_local = _norm(local)
                    if norm_local in pdfs:
                        matched[idx] = norm_local
                        used_pdfs.add(norm_local)
                        break
        elif path_field:
            local = entry.get(path_field, "")
            if local:
                norm_local = _norm(local)
                if norm_local in pdfs:
                    matched[idx] = norm_local
                    used_pdfs.add(norm_local)
            # Also check alternative path fields
            for alt in ("fitxer_local", "path_local", "pdf_local"):
                if alt == path_field:
                    continue
                local2 = entry.get(alt, "")
                if local2 and idx not in matched:
                    norm_local2 = _norm(local2)
                    if norm_local2 in pdfs:
                        matched[idx] = norm_local2
                        used_pdfs.add(norm_local2)

    # Pass 2: FILENAME — PDF filename contains entry code
    for idx, entry in enumerate(entries):
        if idx in matched:
            continue
        for cf in code_fields:
            code = str(entry.get(cf, "")).strip()
            if not code or len(code) < 3:
                continue
            code_lower = code.lower()
            # Check if any PDF filename contains this code
            for fname, norm_paths in pdf_name_index.items():
                # Normalize the code for matching (remove slashes, spaces)
                code_clean = re.sub(r"[/\\,\s]+", "", code_lower)
                fname_clean = re.sub(r"[/\\,\s]+", "", fname)
                if code_clean in fname_clean:
                    for np in norm_paths:
                        if np not in used_pdfs:
                            matched[idx] = np
                            used_pdfs.add(np)
                            break
                    if idx in matched:
                        break
            if idx in matched:
                break

    # Pass 3: URL — PDF filename matches URL basename
    if url_field:
        for idx, entry in enumerate(entries):
            if idx in matched:
                continue
            url = entry.get(url_field, "")
            url_base = _url_basename(url)
            if url_base and url_base in pdf_name_index:
                for np in pdf_name_index[url_base]:
                    if np not in used_pdfs:
                        matched[idx] = np
                        used_pdfs.add(np)
                        break

    # Pass 4: FUZZY — title similarity
    for idx, entry in enumerate(entries):
        if idx in matched:
            continue
        title = ""
        for tf in ("titol", "titulo", "title", "text"):
            title = str(entry.get(tf, "")).strip()
            if title:
                break
        if not title or len(title) < 10:
            continue
        title_clean = _clean_name(title)

        best_score = 0.0
        best_pdf = None
        for norm_path in pdfs:
            if norm_path in used_pdfs:
                continue
            pdf_fname = os.path.basename(norm_path)
            pdf_clean = _clean_name(pdf_fname)
            score = SequenceMatcher(None, title_clean[:80], pdf_clean).ratio()
            if score > best_score:
                best_score = score
                best_pdf = norm_path

        if best_score >= 0.6 and best_pdf:
            matched[idx] = best_pdf
            used_pdfs.add(best_pdf)

    # Compute orphans and missing
    orphan_pdfs = sorted(p for p in pdfs if p not in used_pdfs)

    missing_pdfs = []
    for idx, entry in enumerate(entries):
        if idx in matched:
            continue
        # Get a human-readable identifier
        for cf in code_fields:
            code = entry.get(cf, "")
            if code:
                missing_pdfs.append(str(code))
                break
        else:
            missing_pdfs.append(f"entry_{idx}")

    return matched, orphan_pdfs, missing_pdfs


# ── Main linking function ─────────────────────────────────────

def link_all(base_dir: str = None, dry_run: bool = False) -> dict:
    """Link all catalog entries to their PDF files.

    Args:
        base_dir: project root directory (defaults to parent of scrapers/)
        dry_run: if True, report only — don't modify JSONs

    Returns:
        Report dict with stats per source.
    """
    global BASE_DIR
    if base_dir:
        BASE_DIR = base_dir

    report = {"data": datetime.now().strftime("%Y-%m-%d"), "sources": {}}
    print("=== Catalog Linker ===")

    for source_key, cfg in SOURCES.items():
        catalog_path = os.path.join(BASE_DIR, cfg["catalog"])
        if not os.path.exists(catalog_path):
            print(f"  {cfg['label']:12s} SKIP — catalog not found")
            report["sources"][source_key] = {"status": "missing"}
            continue

        # Load catalog
        with open(catalog_path, "r", encoding="utf-8") as f:
            raw_data = json.load(f)

        # Handle different structures
        if isinstance(raw_data, list):
            entries = raw_data
        elif isinstance(raw_data, dict):
            for key in ("documents", "normes", "entries"):
                if key in raw_data and isinstance(raw_data[key], list):
                    entries = raw_data[key]
                    break
            else:
                entries = []
        else:
            entries = []

        # Scan PDFs
        pdfs = _scan_pdfs(cfg["pdf_dirs"])

        # Match
        matched, orphans, missing = _match_entries(entries, pdfs, cfg)

        # Update entries with matched paths
        path_field = cfg.get("path_field") or "path_local"
        is_nested = cfg.get("nested_pdf", False)
        updates = 0

        if not dry_run and matched:
            for idx, norm_pdf in matched.items():
                # Convert back to OS-relative path
                rel_path = norm_pdf.replace("/", os.sep)
                if is_nested:
                    # ADIF: update fitxers[0].fitxer_local
                    fitxers = entries[idx].get("fitxers", [])
                    if fitxers:
                        old = fitxers[0].get("fitxer_local", "")
                        if _norm(old) != norm_pdf:
                            fitxers[0]["fitxer_local"] = rel_path
                            fitxers[0]["descarregat"] = True
                            updates += 1
                    else:
                        entries[idx].setdefault("fitxers", []).append({
                            "fitxer_local": rel_path,
                            "descarregat": True,
                        })
                        updates += 1
                else:
                    old = entries[idx].get(path_field, "")
                    if _norm(old) != norm_pdf:
                        entries[idx][path_field] = rel_path
                        updates += 1

            if updates > 0:
                # Backup
                bak_path = catalog_path + ".bak"
                shutil.copy2(catalog_path, bak_path)

                # Write updated catalog
                if isinstance(raw_data, list):
                    out_data = entries
                else:
                    # Find the key holding the entries and replace
                    for key in ("documents", "normes", "entries"):
                        if key in raw_data:
                            raw_data[key] = entries
                            break
                    out_data = raw_data

                with open(catalog_path, "w", encoding="utf-8") as f:
                    json.dump(out_data, f, ensure_ascii=False, indent=2)

        n_entries = len(entries)
        n_pdfs = len(pdfs)
        n_matched = len(matched)
        n_orphans = len(orphans)
        n_missing = len(missing)

        print(
            f"  {cfg['label']:12s} {n_entries} entries, {n_pdfs} PDFs "
            f"-> {n_matched} linked, {n_orphans} orphans, {n_missing} missing"
            + (f" ({updates} updated)" if updates > 0 else "")
        )

        report["sources"][source_key] = {
            "total_entries": n_entries,
            "total_pdfs": n_pdfs,
            "matched": n_matched,
            "updated": updates,
            "orphan_pdfs": orphans[:50],  # limit for report size
            "missing_pdfs": missing[:50],
        }

    # Save report
    report_dir = os.path.join(BASE_DIR, "data")
    os.makedirs(report_dir, exist_ok=True)
    report_path = os.path.join(report_dir, "link_report.json")
    with open(report_path, "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2)
    print(f"\n  Report: {report_path}")

    return report


# ── CLI ────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Link normativa catalogs to PDF files")
    parser.add_argument("--dry-run", action="store_true", help="Report only, don't modify JSONs")
    parser.add_argument("--base-dir", default=None, help="Project root directory")
    args = parser.parse_args()

    link_all(base_dir=args.base_dir or BASE_DIR, dry_run=args.dry_run)
