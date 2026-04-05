"""
tipologia.py — Definicions de tipologies de projecte i afinitats de fonts.

Cada tipologia te:
  - sources_boost: fonts amb boost positiu (normativa especifica)
  - sources_penalize: fonts a penalitzar (soroll per a aquesta tipologia)
  - Les fonts no mencionades: pes neutre (transversals)

Boosts s'apliquen com a multiplicadors al score RRF:
  - boost = 1.8 -> +80% (font molt rellevant)
  - boost = 1.4 -> +40% (font rellevant)
  - boost = 1.0 -> neutre (transversal)
  - boost = 0.5 -> -50% (font poc rellevant)
  - boost = 0.3 -> -70% (font soroll per a aquesta tipologia)
"""

TIPOLOGIES = {
    "C": {
        "nom": "Carreteres",
        "descripcio": "Carreteres, autovies, vials urbans, rotondes",
        "sources_boost": {
            "dgc":        1.8,
            "territori":  1.5,
            "eurlex":     1.2,
        },
        "sources_penalize": {
            "adif":       0.3,
            "era":        0.3,
        },
    },
    "F": {
        "nom": "Ferrocarril (FGC)",
        "descripcio": "Linies FGC, estacions, tunels ferroviaris",
        "sources_boost": {
            "adif":              1.8,
            "era":               1.5,
            "eurlex":            1.4,
            "mitma_ferroviari":  1.4,
        },
        "sources_penalize": {
            "dgc":        0.5,
            "aca":        0.4,
        },
    },
    "M": {
        "nom": "Metro",
        "descripcio": "Linies de metro, estacions subterranies, tunels",
        "sources_boost": {
            "adif":       1.6,
            "industria":  1.5,
            "cte":        1.4,
            "eurlex":     1.3,
        },
        "sources_penalize": {
            "dgc":        0.4,
            "aca":        0.4,
        },
    },
    "T": {
        "nom": "Tramvia",
        "descripcio": "Linies de tramvia, plataformes reservades, parades",
        "sources_boost": {
            "territori":  1.8,
            "adif":       1.4,
            "pjcat":      1.4,
            "dgc":        1.2,
        },
        "sources_penalize": {
            "aca":        0.4,
        },
    },
    "A": {
        "nom": "Estacions de bus",
        "descripcio": "Terminals, intercanviadors, parades de bus",
        "sources_boost": {
            "territori":  1.6,
            "pjcat":      1.5,
            "cte":        1.5,
            "industria":  1.4,
        },
        "sources_penalize": {
            "adif":       0.3,
            "dgc":        0.5,
            "era":        0.3,
        },
    },
    "B": {
        "nom": "Carrils bici",
        "descripcio": "Vies ciclistes, carrils bici, aparcaments bici",
        "sources_boost": {
            "territori":  1.8,
            "dgc":        1.4,
            "pjcat":      1.3,
        },
        "sources_penalize": {
            "adif":       0.3,
            "era":        0.3,
            "aca":        0.4,
        },
    },
    "P": {
        "nom": "Parcs de vehicles / Aparcaments",
        "descripcio": "Aparcaments soterrats/superficie, cotxeres",
        "sources_boost": {
            "cte":        1.8,
            "industria":  1.8,
            "eurlex":     1.3,
            "boe":        1.3,
        },
        "sources_penalize": {
            "adif":       0.3,
            "dgc":        0.4,
            "era":        0.3,
            "aca":        0.4,
        },
    },
}

# Fonts transversals: MAI es penalitzen
TRANSVERSAL_SOURCES = {
    "boe", "eurlex", "industria", "cte",
}

# Temes transversals: queries amb aquests termes ignoren la tipologia
TRANSVERSAL_KEYWORDS = [
    "seguretat i salut", "seguridad y salud", "prevencion riesgos",
    "coordinador seguretat", "1627/1997",
    "medi ambient", "medio ambiente", "impacte ambiental", "impacto ambiental",
    "avaluacio ambiental", "evaluacion ambiental",
    "residus", "residuos", "gestio residus",
    "accessibilitat", "accesibilidad", "mobilitat reduida", "movilidad reducida",
    "contractacio", "contratacion", "LCSP", "9/2017", "plec", "pliego",
    "pressupost", "presupuesto", "amidaments", "mediciones",
    "control qualitat", "control calidad", "assaig", "ensayo",
    "expropiacio", "expropiacion",
    "serveis afectats", "servicios afectados",
    "topografia", "cartografia",
]


def get_source_multiplier(tipologia: str, source: str, query: str = "") -> float:
    """Retorna el multiplicador de score per a una font dins una tipologia."""
    if not tipologia or tipologia not in TIPOLOGIES:
        return 1.0

    tip = TIPOLOGIES[tipologia]
    source_lower = source.lower()

    # Temes transversals -> neutre
    if query:
        query_lower = query.lower()
        for keyword in TRANSVERSAL_KEYWORDS:
            if keyword in query_lower:
                return 1.0

    # Fonts transversals: nomes boost, mai penalitzar
    if source_lower in TRANSVERSAL_SOURCES:
        boost = tip.get("sources_boost", {}).get(source_lower, 1.0)
        return max(boost, 1.0)

    # Boost especific
    if source_lower in tip.get("sources_boost", {}):
        return tip["sources_boost"][source_lower]

    # Penalitzacio especifica
    if source_lower in tip.get("sources_penalize", {}):
        return tip["sources_penalize"][source_lower]

    return 1.0


def get_tipologies_list() -> list[dict]:
    """Retorna llista de tipologies per al selector de la web."""
    return [
        {"code": code, "nom": t["nom"], "descripcio": t["descripcio"]}
        for code, t in TIPOLOGIES.items()
    ]
