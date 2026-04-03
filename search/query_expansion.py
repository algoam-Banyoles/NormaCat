"""
query_expansion.py — Expansio bilingue CA<->ES de consultes.

Tradueix automaticament termes tecnics d'infraestructures
entre catala i castella per millorar la cerca BM25.

Us:
    from search.query_expansion import expand_query
    queries = expand_query("senyalitzacio horitzontal viaria")
"""

import re
import unicodedata


def _strip_accents(text: str) -> str:
    nfkd = unicodedata.normalize("NFKD", text)
    return "".join(c for c in nfkd if not unicodedata.combining(c))


# ── Diccionari tecnic CA -> ES ──────────────────────────────────────────────────

_CA_TO_ES = {
    # CARRETERES / VIALITAT
    "senyalitzacio": ["senalizacion"],
    "senyalitzacio horitzontal": ["senalizacion horizontal", "marcas viales"],
    "senyalitzacio vertical": ["senalizacion vertical", "senales verticales"],
    "marques viaries": ["marcas viales"],
    "abalisament": ["balizamiento"],
    "ferm": ["firme", "pavimento"],
    "ferms": ["firmes", "pavimentos"],
    "calcada": ["calzada"],
    "voral": ["arcen"],
    "vorals": ["arcenes"],
    "carretera": ["carretera"],
    "carreteres": ["carreteras"],
    "revolt": ["curva"],
    "revolts": ["curvas"],
    "rasant": ["rasante"],
    "tracat": ["trazado"],
    "seccio transversal": ["seccion transversal"],
    "seccio tipus": ["seccion tipo"],
    "interseccio": ["interseccion"],
    "rotonda": ["rotonda", "glorieta"],
    "encreuament": ["cruce", "interseccion"],
    "pas a nivell": ["paso a nivel"],
    "pas de vianants": ["paso de peatones"],
    "vianants": ["peatones"],
    "velocitat": ["velocidad"],
    "enllumenat": ["alumbrado", "iluminacion"],
    "amplada": ["anchura", "ancho"],
    "pendent": ["pendiente"],
    "peralt": ["peralte"],
    "peralts": ["peraltes"],
    "barrera": ["barrera"],
    "defensa": ["defensa", "barrera"],
    "defenses": ["defensas", "barreras"],
    "glera": ["mediana"],
    # DRENATGE / HIDROLOGIA
    "drenatge": ["drenaje"],
    "drenatge transversal": ["drenaje transversal"],
    "drenatge longitudinal": ["drenaje longitudinal"],
    "clavegueram": ["alcantarillado", "saneamiento"],
    "sanejament": ["saneamiento"],
    "aigues pluvials": ["aguas pluviales"],
    "aigues residuals": ["aguas residuales"],
    "inundacio": ["inundacion"],
    "avinguda": ["avenida", "crecida"],
    "cabal": ["caudal"],
    "conca": ["cuenca"],
    "escorrentia": ["escorrentia"],
    "obra de fabrica": ["obra de fabrica"],
    "pont": ["puente"],
    "ponts": ["puentes"],
    "passera": ["pasarela"],
    "tub": ["tubo", "tuberia"],
    "caixo": ["cajon"],
    "marc": ["marco"],
    "llera": ["cauce"],
    # ESTRUCTURES
    "estructura": ["estructura"],
    "formigo": ["hormigon"],
    "formigo armat": ["hormigon armado"],
    "formigo pretensat": ["hormigon pretensado"],
    "acer": ["acero"],
    "armadura": ["armadura"],
    "fonamentacio": ["cimentacion"],
    "fonament": ["cimiento", "cimentacion"],
    "pilotatge": ["pilotaje"],
    "pilot": ["pilote"],
    "mur": ["muro"],
    "murs": ["muros"],
    "mur de contencio": ["muro de contencion"],
    "estrep": ["estribo"],
    "pila": ["pila"],
    "bigues": ["vigas"],
    "biga": ["viga"],
    "llosa": ["losa"],
    "pretesat": ["pretensado"],
    "posttesat": ["postesado"],
    "junta de dilatacio": ["junta de dilatacion"],
    "aparell de recolzament": ["aparato de apoyo", "apoyo"],
    "galib": ["galibo"],
    "carrega": ["carga"],
    "carregues": ["cargas"],
    "accions": ["acciones"],
    "sobrecarrega": ["sobrecarga"],
    # GEOTECNIA
    "geotecnia": ["geotecnia"],
    "talus": ["talud"],
    "talussos": ["taludes"],
    "terraple": ["terraplen"],
    "desmunt": ["desmonte"],
    "excavacio": ["excavacion"],
    "moviment de terres": ["movimiento de tierras"],
    "sol": ["suelo"],
    "sols": ["suelos"],
    "roca": ["roca"],
    "estabilitat": ["estabilidad"],
    "compactacio": ["compactacion"],
    # FERROVIARI
    "via": ["via"],
    "ferrocarril": ["ferrocarril"],
    "ferroviari": ["ferroviario"],
    "catenaria": ["catenaria"],
    "electrificacio": ["electrificacion"],
    "estacio": ["estacion"],
    "andana": ["anden"],
    "andanes": ["andenes"],
    "tunel": ["tunel"],
    "tunels": ["tuneles"],
    "ventilacio": ["ventilacion"],
    "material rodant": ["material rodante"],
    "enclavament": ["enclavamiento"],
    "interoperabilitat": ["interoperabilidad"],
    # METRO / TRAMVIA
    "metro": ["metro"],
    "tramvia": ["tranvia"],
    "plataforma": ["plataforma"],
    "plataforma reservada": ["plataforma reservada"],
    # EDIFICACIO / INSTAL.LACIONS
    "edificacio": ["edificacion"],
    "incendis": ["incendios"],
    "proteccio contra incendis": ["proteccion contra incendios"],
    "evacuacio": ["evacuacion"],
    "accessibilitat": ["accesibilidad"],
    "mobilitat reduida": ["movilidad reducida"],
    "ascensor": ["ascensor"],
    "ascensors": ["ascensores"],
    "escala mecanica": ["escalera mecanica"],
    "baixa tensio": ["baja tension"],
    "alta tensio": ["alta tension"],
    "quadre electric": ["cuadro electrico"],
    "climatitzacio": ["climatizacion"],
    "calefaccio": ["calefaccion"],
    "fontaneria": ["fontaneria"],
    "aparcament": ["aparcamiento", "estacionamiento"],
    "aparcaments": ["aparcamientos"],
    # MEDI AMBIENT
    "medi ambient": ["medio ambiente"],
    "impacte ambiental": ["impacto ambiental"],
    "avaluacio ambiental": ["evaluacion ambiental"],
    "contaminacio": ["contaminacion"],
    "soroll": ["ruido"],
    "residus": ["residuos"],
    "biodiversitat": ["biodiversidad"],
    "paisatge": ["paisaje"],
    "restauracio": ["restauracion"],
    # SEGURETAT I SALUT
    "seguretat": ["seguridad"],
    "seguretat i salut": ["seguridad y salud"],
    "prevencio": ["prevencion"],
    "riscos laborals": ["riesgos laborales"],
    "pla de seguretat": ["plan de seguridad"],
    "estudi de seguretat": ["estudio de seguridad"],
    "equips de proteccio": ["equipos de proteccion"],
    # CONTRACTACIO / LEGAL
    "contractacio": ["contratacion"],
    "licitacio": ["licitacion"],
    "plec de condicions": ["pliego de condiciones"],
    "plec de prescripcions": ["pliego de prescripciones"],
    "pressupost": ["presupuesto"],
    "amidaments": ["mediciones"],
    "preus unitaris": ["precios unitarios"],
    "projecte constructiu": ["proyecto constructivo"],
    "projecte d'obra": ["proyecto de obra"],
    "obra civil": ["obra civil"],
    "control de qualitat": ["control de calidad"],
    # VEHICLES / ITV
    "opacimetre": ["opacimetro", "opacidad", "humos diesel"],
    "itv": ["itv", "inspeccion tecnica vehiculos"],
    "emissions": ["emisiones"],
    "contaminants": ["contaminantes"],
    "vehicles": ["vehiculos"],
    "homologacio": ["homologacion"],
    # URBANISME
    "urbanisme": ["urbanismo"],
    "planejament": ["planeamiento"],
    "ordenacio": ["ordenacion"],
    "sostenibilitat": ["sostenibilidad"],
    "eficiencia energetica": ["eficiencia energetica"],
}

# Generar diccionari invers ES -> CA
_ES_TO_CA = {}
for ca_term, es_terms in _CA_TO_ES.items():
    for es_term in es_terms:
        es_key = _strip_accents(es_term).lower()
        if es_key not in _ES_TO_CA:
            _ES_TO_CA[es_key] = []
        if ca_term not in _ES_TO_CA[es_key]:
            _ES_TO_CA[es_key].append(ca_term)


def _detect_language(text: str) -> str:
    """Detecta si el text es catala o castella (heuristica simple)."""
    text_lower = text.lower()
    ca_markers = ["senyalitz", "drenatge", "formigo", "calcada",
                  "amplada", "vianant", "enllumenat", "fonament", "ferroviari",
                  "incendis", "accessibilitat", "abalisament",
                  "terraple", "talussos", "geotecnia", "sanejament",
                  "clavegueram", "ferm ", "vorals", "revolt"]
    es_markers = ["senalizacion", "drenaje", "hormigon", "calzada",
                  "anchura", "peaton", "alumbrado", "cimentacion",
                  "ferroviario", "incendios", "accesibilidad", "balizamiento",
                  "terraplen", "taludes", "saneamiento",
                  "firme ", "arcen", "curva"]

    ca_count = sum(1 for m in ca_markers if m in text_lower)
    es_count = sum(1 for m in es_markers if m in text_lower)

    if ca_count > es_count:
        return "ca"
    elif es_count > ca_count:
        return "es"
    return "unknown"


def expand_query(query: str) -> list[str]:
    """Expandeix una consulta amb traduccions CA<->ES."""
    query_clean = _strip_accents(query).lower().strip()
    if not query_clean:
        return [query_clean]

    variants = set()
    variants.add(query_clean)

    lang = _detect_language(query_clean)

    if lang != "es":
        _expand_with_dict(query_clean, _CA_TO_ES, variants)

    if lang != "ca":
        _expand_with_dict(query_clean, _ES_TO_CA, variants)

    return list(variants)


def _expand_with_dict(query: str, dictionary: dict, variants: set):
    """Substitueix termes usant el diccionari (longest match first)."""
    sorted_keys = sorted(dictionary.keys(), key=len, reverse=True)

    for key in sorted_keys:
        if key in query:
            translations = dictionary[key]
            for trans in translations:
                expanded = query.replace(key, trans)
                if expanded != query:
                    variants.add(expanded)
            for trans in translations:
                if len(trans.split()) >= 2:
                    variants.add(trans)


def expand_for_bm25(query: str) -> str:
    """Retorna una FTS5 query expandida amb OR de totes les variants."""
    variants = expand_query(query)

    all_words = set()
    for v in variants:
        words = [w for w in re.sub(r'[^\w\s]', ' ', v).split() if len(w) > 2]
        all_words.update(words)

    return " OR ".join(sorted(all_words))
