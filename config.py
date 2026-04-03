"""NormaCat — Configuració central."""
from pathlib import Path

# Arrel del projecte NormaCat
PROJECT_ROOT = Path(__file__).resolve().parent

# Carpetes de dades
CATALOGS_DIR = PROJECT_ROOT / "catalogs"
DB_DIR = PROJECT_ROOT / "db"
DATA_DIR = PROJECT_ROOT / "data"

# SQLite i ChromaDB
SQLITE_PATH = str(DB_DIR / "normativa.db")
CHROMA_PATH = str(DB_DIR / "chroma_db")

# Embedding model
EMBEDDING_MODEL = "paraphrase-multilingual-MiniLM-L12-v2"

# Chunk settings
CHUNK_SIZE = 800
CHUNK_OVERLAP = 150

# Scraper delay (segons entre peticions)
SCRAPER_DELAY = 1.0

# LLM defaults
DEFAULT_LLM_PROVIDER = "gemini"  # gemini | groq | claude

# Registre de fonts normatives
SOURCES = {
    "aca":              {"label": "ACA",              "catalog": "catalogs/aca/catalogo_aca.json"},
    "adif":             {"label": "ADIF NTEs",        "catalog": "catalogs/adif/catalogo_adif.json"},
    "boe":              {"label": "BOE",              "catalog": "catalogs/boe/catalogo_boe.json"},
    "cte":              {"label": "CTE",              "catalog": "catalogs/cte/catalogo_cte.json"},
    "dgc":              {"label": "DGC",              "catalog": "catalogs/dgc/catalogo_completo.json"},
    "era":              {"label": "ERA/CENELEC",      "catalog": "catalogs/era/catalogo_era.json"},
    "eurlex":           {"label": "EUR-Lex UE",      "catalog": "catalogs/eurlex/catalogo_eurlex.json"},
    "industria":        {"label": "Indústria",        "catalog": "catalogs/industria/catalogo_industria.json"},
    "iso":              {"label": "ISO",              "catalog": "catalogs/iso/catalogo_iso.json"},
    "mitma_ferroviari": {"label": "MITMA Ferroviari", "catalog": "catalogs/mitma_ferroviari/catalogo_mitma_ferroviari.json"},
    "pjcat":            {"label": "PJCAT",            "catalog": "catalogs/pjcat/catalogo_pjcat.json"},
    "territori":        {"label": "Territori",        "catalog": "catalogs/territori/catalogo_territori.json"},
    "une":              {"label": "UNE",              "catalog": "catalogs/une/catalogo_une.json"},
}
