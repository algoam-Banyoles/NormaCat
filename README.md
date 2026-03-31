# NormaCat — Cercador de Normativa Tecnica

Eina de scraping, indexacio i cerca semantica de normativa tecnica
per a projectes d'infraestructures a Catalunya.

## Fonts suportades

| Font | Descripcio |
|------|-----------|
| ACA | Agencia Catalana de l'Aigua |
| ADIF | Normes Tecniques d'Enginyeria (NTE) d'ADIF |
| BOE | Butlleti Oficial de l'Estat — OpenData API |
| CTE | Codi Tecnic de l'Edificacio |
| DGC | Direccio General de Carreteres (Ministeri Transports) |
| ERA | Agencia Ferroviaria Europea / CENELEC |
| Industria | Reglaments del Ministeri d'Industria |
| ISO | Cataleg obert ISO (CSV) |
| MITMA Ferroviari | Normativa ferroviaria MITMA |
| PJCAT | Portal Juridic de Catalunya (ELI API) |
| Territori | Normativa territorial i urbanistica |
| UNE | Normes UNE (via curl_cffi) |

## Us rapid

```bash
# 1. Descarregar normativa
python cli.py scrape --source all

# 2. Indexar a ChromaDB
python cli.py index --source all

# 3. Cercar
python cli.py search "drenatge transversal en carreteres"
```

## Arquitectura

```
scrapers/          indexer/          search/
  norm_scraper       norm_indexer      norm_checker
  adif_scraper       (ChromaDB +      norm_index
  iso_catalog         SQLite)         norm_resolver
  une_catalog
  boe_scraper        catalogs/ ->    llm/
  pjcat_scraper      JSON catalogs     llm_provider
  ...                                  (Claude/Gemini/Groq)
       |                |                   |
       v                v                   v
  [Fonts web]    [ChromaDB + SQLite]   [Consulta LN]
```

## Requisits

- Python 3.11+
- Entorn virtual recomanat (`python -m venv .venv`)
- Variables d'entorn per LLM: `GEMINI_API_KEY`, `ANTHROPIC_API_KEY` o `GROQ_API_KEY`

## Installacio

```bash
cd NormaCat
python -m venv .venv
.venv\Scripts\activate   # Windows
pip install -r requirements.txt
```

## Nota

Aquest projecte es una extraccio del modul normatiu de
Project Checker (DGIM, contracte PTOP-2026-7).
