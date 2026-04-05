"""
eval_dataset.py — Genera dataset sintetic i valida amb LLM.

Pipeline:
  1. Genera queries dels 7 tipus de projecte x 15 annexes
  2. Executa cada query contra HybridSearcher
  3. Gemini Flash puntua rellevancia de cada resultat (0-3)
  4. Guarda a search_feedback

Us:
    python tools/eval_dataset.py generate   # Genera queries JSON
    python tools/eval_dataset.py evaluate   # Executa + valida amb LLM
    python tools/eval_dataset.py report     # Mostra estadistiques
"""

import argparse
import json
import os
import re
import sys
import time
from datetime import datetime
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import config
from env_utils import load_local_env
load_local_env()

DATASET_PATH = config.DATA_DIR / "eval_queries.json"
RESULTS_PATH = config.DATA_DIR / "eval_results.json"

# ══════════════════════════════════════════════════════════════
# QUERIES SINTETIQUES
# ══════════════════════════════════════════════════════════════

SYNTHETIC_QUERIES = [
    # A01 ANTECEDENTS
    {"text": "contingut minim projecte constructiu obra publica",
     "tipologia": "G", "annex": "A01",
     "expected_sources": ["pjcat", "boe"],
     "expected_terms": ["LCSP", "231", "projecte"]},
    {"text": "requisits ordre d'estudi i antecedents administratius",
     "tipologia": "G", "annex": "A01",
     "expected_sources": ["pjcat"],
     "expected_terms": ["LOP", "3/2007"]},

    # A02 QUALITAT I MEDI AMBIENT
    {"text": "pla de control de qualitat obra civil",
     "tipologia": "G", "annex": "A02",
     "expected_sources": ["territori", "pjcat"],
     "expected_terms": ["qualitat", "control", "assaig"]},
    {"text": "gestio de residus de construccio i demolicio",
     "tipologia": "G", "annex": "A02",
     "expected_sources": ["boe", "pjcat"],
     "expected_terms": ["residus", "residuos", "7/2022"]},
    {"text": "avaluacio impacte ambiental projectes infraestructures",
     "tipologia": "G", "annex": "A02",
     "expected_sources": ["pjcat", "eurlex", "boe"],
     "expected_terms": ["impacte", "impacto", "ambiental"]},

    # A03 ESTUDIS FUNCIONALS
    {"text": "parametres de tracat carretera C-80",
     "tipologia": "C", "annex": "A03",
     "expected_sources": ["dgc"],
     "expected_terms": ["tracat", "trazado", "3.1-IC"]},
    {"text": "radi minim en planta per a velocitat de projecte 80 km/h",
     "tipologia": "C", "annex": "A03",
     "expected_sources": ["dgc"],
     "expected_terms": ["radi", "radio", "planta"]},
    {"text": "galib ferroviari FGC ample metric",
     "tipologia": "F", "annex": "A03",
     "expected_sources": ["adif"],
     "expected_terms": ["galib", "galibo", "metric"]},
    {"text": "seccio transversal tipus carretera convencional",
     "tipologia": "C", "annex": "A03",
     "expected_sources": ["dgc"],
     "expected_terms": ["seccio", "seccion", "transversal"]},

    # A04 PLANEJAMENT I URBANISME
    {"text": "classificacio del sol urbanisme Catalunya",
     "tipologia": "G", "annex": "A04",
     "expected_sources": ["pjcat", "territori"],
     "expected_terms": ["urbanisme", "urbanismo", "sol"]},

    # A05 TRACAT / GEOMETRIA
    {"text": "norma 3.1-IC tracat instruccio de carreteres",
     "tipologia": "C", "annex": "A05",
     "expected_sources": ["dgc"],
     "expected_terms": ["3.1-IC", "trazado"]},
    {"text": "pendents longitudinals maximes en rampes",
     "tipologia": "C", "annex": "A05",
     "expected_sources": ["dgc"],
     "expected_terms": ["pendent", "pendiente", "rampa"]},
    {"text": "amplada minima plataforma tramvia",
     "tipologia": "T", "annex": "A05",
     "expected_sources": ["territori", "dgc"],
     "expected_terms": ["tramvia", "tranvia", "amplada"]},
    {"text": "amplada minima carril bici segregat",
     "tipologia": "B", "annex": "A05",
     "expected_sources": ["territori", "dgc"],
     "expected_terms": ["carril", "bici", "amplada"]},

    # A06 FERMS I PAVIMENTS
    {"text": "mescles bituminoses capa de rodadura",
     "tipologia": "C", "annex": "A06",
     "expected_sources": ["dgc"],
     "expected_terms": ["mezcla", "bituminosa", "rodadura", "542"]},
    {"text": "article 542 PG-3 mescla bituminosa tipus AC",
     "tipologia": "C", "annex": "A06",
     "expected_sources": ["dgc"],
     "expected_terms": ["542", "AC", "PG-3"]},
    {"text": "esplanada i ferm en carreteres categories de transit",
     "tipologia": "C", "annex": "A06",
     "expected_sources": ["dgc"],
     "expected_terms": ["esplanada", "explanada", "6.1-IC"]},
    {"text": "reg d'imprimacio i reg d'adherencia ferms",
     "tipologia": "C", "annex": "A06",
     "expected_sources": ["dgc"],
     "expected_terms": ["riego", "imprimacion", "adherencia"]},

    # A07 DRENATGE
    {"text": "drenatge transversal carreteres periode de retorn",
     "tipologia": "C", "annex": "A07",
     "expected_sources": ["dgc"],
     "expected_terms": ["drenaje", "transversal", "5.2-IC"]},
    {"text": "drenatge longitudinal cunetes i col.lectors",
     "tipologia": "C", "annex": "A07",
     "expected_sources": ["dgc", "aca"],
     "expected_terms": ["drenaje", "cuneta", "colector"]},
    {"text": "dimensionament hidraulic obres de fabrica",
     "tipologia": "C", "annex": "A07",
     "expected_sources": ["dgc", "aca"],
     "expected_terms": ["hidraulico", "obra", "fabrica"]},

    # A08 INSTAL.LACIONS FERROVIARIES
    {"text": "catenaria i electrificacio ferroviaria",
     "tipologia": "F", "annex": "A08",
     "expected_sources": ["adif"],
     "expected_terms": ["catenaria", "electrificacion"]},
    {"text": "senyalitzacio ferroviaria ERTMS i enclavaments",
     "tipologia": "F", "annex": "A08",
     "expected_sources": ["adif", "eurlex"],
     "expected_terms": ["ERTMS", "enclavamiento", "senalizacion"]},
    {"text": "superestructura de via balast i travesses",
     "tipologia": "F", "annex": "A08",
     "expected_sources": ["adif"],
     "expected_terms": ["via", "balasto", "traviesa"]},

    # A09 INSTAL.LACIONS NO FERROVIARIES
    {"text": "instal.lacio electrica baixa tensio REBT",
     "tipologia": "G", "annex": "A09",
     "expected_sources": ["industria", "boe"],
     "expected_terms": ["REBT", "842/2002", "baja tension"]},
    {"text": "climatitzacio i ventilacio RITE edificis",
     "tipologia": "G", "annex": "A09",
     "expected_sources": ["industria", "boe"],
     "expected_terms": ["RITE", "1027/2007", "ventilacion"]},
    {"text": "proteccio contra incendis en aparcaments soterrats",
     "tipologia": "P", "annex": "A09",
     "expected_sources": ["industria", "cte", "boe"],
     "expected_terms": ["incendios", "aparcamiento", "RIPCI"]},
    {"text": "enllumenat public carrers i vials eficiencia energetica",
     "tipologia": "C", "annex": "A09",
     "expected_sources": ["industria", "boe"],
     "expected_terms": ["alumbrado", "iluminacion", "eficiencia"]},

    # A10 SENYALITZACIO
    {"text": "senyalitzacio horitzontal viaria marcas viales",
     "tipologia": "C", "annex": "A10",
     "expected_sources": ["dgc"],
     "expected_terms": ["marcas viales", "8.2-IC", "horizontal"]},
    {"text": "senyalitzacio vertical carreteres norma 8.1-IC",
     "tipologia": "C", "annex": "A10",
     "expected_sources": ["dgc"],
     "expected_terms": ["8.1-IC", "senalizacion", "vertical"]},
    {"text": "senyalitzacio d'obres en carreteres 8.3-IC",
     "tipologia": "C", "annex": "A10",
     "expected_sources": ["dgc"],
     "expected_terms": ["8.3-IC", "obras", "senalizacion"]},
    {"text": "abalisament viari balises i captafars",
     "tipologia": "C", "annex": "A10",
     "expected_sources": ["territori", "dgc"],
     "expected_terms": ["balizamiento", "captafaro"]},

    # A11 ACCESSIBILITAT
    {"text": "accessibilitat persones mobilitat reduida estacions",
     "tipologia": "M", "annex": "A11",
     "expected_sources": ["pjcat", "boe", "eurlex"],
     "expected_terms": ["accessibilitat", "accesibilidad", "209/2023"]},
    {"text": "codi accessibilitat Catalunya Decret 209/2023",
     "tipologia": "G", "annex": "A11",
     "expected_sources": ["pjcat"],
     "expected_terms": ["209/2023", "accessibilitat"]},

    # A12 SUBMINISTRAMENTS
    {"text": "connexio xarxa electrica subministrament",
     "tipologia": "G", "annex": "A12",
     "expected_sources": ["industria"],
     "expected_terms": ["electrica", "suministro"]},

    # A13 INTEGRACIO URBANA
    {"text": "reposicio serveis afectats xarxes existents",
     "tipologia": "G", "annex": "A13",
     "expected_sources": ["pjcat", "boe"],
     "expected_terms": ["serveis", "servicios", "afectados"]},

    # A15 ESTRUCTURES
    {"text": "codi estructural formigo armat RD 470/2021",
     "tipologia": "G", "annex": "A15",
     "expected_sources": ["boe", "dgc"],
     "expected_terms": ["estructural", "470/2021", "hormigon"]},
    {"text": "accions en ponts de carretera IAP-11",
     "tipologia": "C", "annex": "A15",
     "expected_sources": ["dgc"],
     "expected_terms": ["IAP", "puente", "acciones"]},
    {"text": "fonamentacions profundes pilotatge",
     "tipologia": "G", "annex": "A15",
     "expected_sources": ["dgc"],
     "expected_terms": ["cimentacion", "pilote", "profunda"]},
    {"text": "murs de contencio empenta de terres",
     "tipologia": "G", "annex": "A15",
     "expected_sources": ["dgc"],
     "expected_terms": ["muro", "contencion", "empuje"]},

    # SEGURETAT I SALUT
    {"text": "estudi seguretat i salut en obres de construccio",
     "tipologia": "G", "annex": "",
     "expected_sources": ["boe", "pjcat"],
     "expected_terms": ["1627/1997", "seguridad", "salud"]},
    {"text": "coordinacio seguretat en fase de projecte i execucio",
     "tipologia": "G", "annex": "",
     "expected_sources": ["boe"],
     "expected_terms": ["coordinador", "seguridad"]},

    # CONTRACTACIO
    {"text": "plec prescripcions tecniques particulars LCSP",
     "tipologia": "G", "annex": "",
     "expected_sources": ["pjcat", "boe"],
     "expected_terms": ["pliego", "prescripciones", "9/2017"]},

    # NORMATIVA UE
    {"text": "directiva avaluacio impacte ambiental projectes UE",
     "tipologia": "G", "annex": "",
     "expected_sources": ["eurlex"],
     "expected_terms": ["2011/92", "impacto", "ambiental"]},
    {"text": "reglament productes construccio marcatge CE",
     "tipologia": "G", "annex": "",
     "expected_sources": ["eurlex"],
     "expected_terms": ["305/2011", "productos", "construccion"]},
    {"text": "directiva interoperabilitat sistema ferroviari",
     "tipologia": "F", "annex": "",
     "expected_sources": ["eurlex", "era"],
     "expected_terms": ["2016/797", "interoperabilidad"]},
    {"text": "seguretat en tunels viaris directiva europea",
     "tipologia": "C", "annex": "",
     "expected_sources": ["eurlex", "dgc"],
     "expected_terms": ["2004/54", "tunel", "seguridad"]},

    # TIPOLOGIES ESPECIFIQUES
    {"text": "estacio d'autobusos marquesina i zona d'espera",
     "tipologia": "A", "annex": "",
     "expected_sources": ["territori", "pjcat"],
     "expected_terms": ["estacio", "autobus", "marquesina"]},
    {"text": "parc de vehicles cotxeres ventilacio i incendis",
     "tipologia": "P", "annex": "",
     "expected_sources": ["cte", "industria"],
     "expected_terms": ["ventilacion", "incendios", "aparcamiento"]},
    {"text": "via verda carril bici senyalitzacio ciclista",
     "tipologia": "B", "annex": "",
     "expected_sources": ["territori", "dgc"],
     "expected_terms": ["ciclista", "bici", "senalizacion"]},
    {"text": "plataforma tramvia segregada galib vehicle",
     "tipologia": "T", "annex": "",
     "expected_sources": ["territori"],
     "expected_terms": ["tramvia", "plataforma", "galib"]},
    {"text": "ventilacio tunel metro sistema d'extraccio de fums",
     "tipologia": "M", "annex": "",
     "expected_sources": ["adif", "industria"],
     "expected_terms": ["ventilacion", "tunel", "humos"]},

    # ══════════════════════════════════════════════════════════
    # RONDA 2: 100 QUERIES ADDICIONALS
    # ══════════════════════════════════════════════════════════

    # CARRETERES: TRACAT I GEOMETRIA (10)
    {"text": "distancia de visibilitat de parada en carreteres",
     "tipologia": "C", "annex": "A05",
     "expected_sources": ["dgc"],
     "expected_terms": ["visibilidad", "parada", "3.1-IC"]},
    {"text": "peralts maxims en corbes de carreteres convencionals",
     "tipologia": "C", "annex": "A05",
     "expected_sources": ["dgc"],
     "expected_terms": ["peralte", "curva", "convencional"]},
    {"text": "acords verticals parabola de transicio rasant",
     "tipologia": "C", "annex": "A05",
     "expected_sources": ["dgc"],
     "expected_terms": ["acuerdo", "vertical", "rasante"]},
    {"text": "velocitat de projecte i velocitat especifica norma 3.1-IC",
     "tipologia": "C", "annex": "A05",
     "expected_sources": ["dgc"],
     "expected_terms": ["velocidad", "proyecto", "3.1-IC"]},
    {"text": "numero de carrils en funcio del nivell de servei IMD",
     "tipologia": "C", "annex": "A03",
     "expected_sources": ["dgc"],
     "expected_terms": ["carriles", "nivel", "servicio", "IMD"]},
    {"text": "carrers de servei i vials laterals en autovies",
     "tipologia": "C", "annex": "A05",
     "expected_sources": ["dgc"],
     "expected_terms": ["via", "servicio", "lateral"]},
    {"text": "interseccions rotondes dimensionament geometric",
     "tipologia": "C", "annex": "A05",
     "expected_sources": ["dgc"],
     "expected_terms": ["rotonda", "glorieta", "interseccion"]},
    {"text": "rampes i pendents longitudinals maxims vies urbanes",
     "tipologia": "C", "annex": "A05",
     "expected_sources": ["dgc", "territori"],
     "expected_terms": ["pendiente", "rampa", "urbana"]},
    {"text": "bermes i vorals amplada minima per categoria",
     "tipologia": "C", "annex": "A05",
     "expected_sources": ["dgc"],
     "expected_terms": ["arcen", "berma", "anchura"]},
    {"text": "coordinacio planta-alcat consistencia del tracat",
     "tipologia": "C", "annex": "A05",
     "expected_sources": ["dgc"],
     "expected_terms": ["coordinacion", "planta", "alzado"]},

    # CARRETERES: FERMS I PAVIMENTS (8)
    {"text": "categories de transit pesat norma 6.1-IC seccions de ferm",
     "tipologia": "C", "annex": "A06",
     "expected_sources": ["dgc"],
     "expected_terms": ["6.1-IC", "trafico", "seccion", "firme"]},
    {"text": "zahorra artificial base granular prescripcions PG-3",
     "tipologia": "C", "annex": "A06",
     "expected_sources": ["dgc"],
     "expected_terms": ["zahorra", "510", "PG-3"]},
    {"text": "sol estabilitzat amb ciment suelocemento article 512",
     "tipologia": "C", "annex": "A06",
     "expected_sources": ["dgc"],
     "expected_terms": ["suelocemento", "512", "cemento"]},
    {"text": "paviment de formigo llosa continua juntes",
     "tipologia": "C", "annex": "A06",
     "expected_sources": ["dgc"],
     "expected_terms": ["hormigon", "pavimento", "junta"]},
    {"text": "mescla bituminosa drenant capa de rodadura PA",
     "tipologia": "C", "annex": "A06",
     "expected_sources": ["dgc"],
     "expected_terms": ["drenante", "PA", "rodadura"]},
    {"text": "betum modificat amb polimers PMB caracteristiques",
     "tipologia": "C", "annex": "A06",
     "expected_sources": ["dgc"],
     "expected_terms": ["betun", "PMB", "polimero"]},
    {"text": "deflexions i capacitat portant del ferm viga benkelman",
     "tipologia": "C", "annex": "A06",
     "expected_sources": ["dgc"],
     "expected_terms": ["deflexion", "capacidad", "firme"]},
    {"text": "textura superficial i resistencia al lliscament SRT",
     "tipologia": "C", "annex": "A06",
     "expected_sources": ["dgc"],
     "expected_terms": ["textura", "resistencia", "deslizamiento"]},

    # DRENATGE I HIDROLOGIA (7)
    {"text": "periode de retorn per a obres de drenatge transversal 5.2-IC",
     "tipologia": "C", "annex": "A07",
     "expected_sources": ["dgc"],
     "expected_terms": ["periodo", "retorno", "5.2-IC"]},
    {"text": "cunetes triangulars i trapezoidals dimensionament",
     "tipologia": "C", "annex": "A07",
     "expected_sources": ["dgc"],
     "expected_terms": ["cuneta", "triangular", "trapezoidal"]},
    {"text": "tubs de formigo armat diametre minim obres de drenatge",
     "tipologia": "C", "annex": "A07",
     "expected_sources": ["dgc"],
     "expected_terms": ["tubo", "hormigon", "diametro"]},
    {"text": "estudi hidrologic conca vessant metode racional",
     "tipologia": "C", "annex": "A07",
     "expected_sources": ["dgc", "aca"],
     "expected_terms": ["hidrologico", "cuenca", "racional"]},
    {"text": "cabal de disseny inundabilitat zones urbanes ACA",
     "tipologia": "C", "annex": "A07",
     "expected_sources": ["aca"],
     "expected_terms": ["caudal", "inundabilidad"]},
    {"text": "col.lectors i xarxa de sanejament pluvials",
     "tipologia": "C", "annex": "A07",
     "expected_sources": ["aca", "dgc"],
     "expected_terms": ["colector", "saneamiento", "pluvial"]},
    {"text": "drenatge subterrani drens i lamines impermeabilitzants",
     "tipologia": "C", "annex": "A07",
     "expected_sources": ["dgc"],
     "expected_terms": ["dren", "subterraneo", "impermeabilizante"]},

    # ESTRUCTURES (10)
    {"text": "sobrecarrega d'us en ponts de carretera IAP-11",
     "tipologia": "C", "annex": "A15",
     "expected_sources": ["dgc"],
     "expected_terms": ["IAP", "sobrecarga", "puente"]},
    {"text": "armat minim en pilars i bigues formigo codi estructural",
     "tipologia": "G", "annex": "A15",
     "expected_sources": ["dgc", "boe"],
     "expected_terms": ["armado", "minimo", "pilar"]},
    {"text": "control de fissuracio obertura maxima de fissura",
     "tipologia": "G", "annex": "A15",
     "expected_sources": ["dgc", "boe"],
     "expected_terms": ["fisuracion", "abertura", "fisura"]},
    {"text": "resistencia caracteristica formigo fck exposicio ambiental",
     "tipologia": "G", "annex": "A15",
     "expected_sources": ["dgc", "boe"],
     "expected_terms": ["resistencia", "fck", "exposicion"]},
    {"text": "recubriment minim armadures durabilitat formigo",
     "tipologia": "G", "annex": "A15",
     "expected_sources": ["dgc", "boe"],
     "expected_terms": ["recubrimiento", "armadura", "durabilidad"]},
    {"text": "pretesat per post-tensio perdues de tensio ancoratges",
     "tipologia": "G", "annex": "A15",
     "expected_sources": ["dgc"],
     "expected_terms": ["pretensado", "perdidas", "anclaje"]},
    {"text": "unions soldades en estructures metal.liques CTE DB-SE-A",
     "tipologia": "G", "annex": "A15",
     "expected_sources": ["cte", "dgc"],
     "expected_terms": ["soldadura", "metalica", "DB-SE"]},
    {"text": "accio sismica NCSE-02 acceleracio basica",
     "tipologia": "G", "annex": "A15",
     "expected_sources": ["dgc", "boe"],
     "expected_terms": ["sismica", "NCSE", "aceleracion"]},
    {"text": "proves de carrega estatica en ponts acabats",
     "tipologia": "C", "annex": "A15",
     "expected_sources": ["dgc"],
     "expected_terms": ["prueba", "carga", "estatica"]},
    {"text": "juntes de dilatacio aparells de recolzament neopre",
     "tipologia": "C", "annex": "A15",
     "expected_sources": ["dgc"],
     "expected_terms": ["junta", "dilatacion", "neopreno", "apoyo"]},

    # GEOTECNIA (6)
    {"text": "assaigs SPT i CPTU per a fonamentacions profundes",
     "tipologia": "G", "annex": "A15",
     "expected_sources": ["dgc"],
     "expected_terms": ["SPT", "CPTU", "cimentacion"]},
    {"text": "capacitat portant del terreny tensio admissible fonaments",
     "tipologia": "G", "annex": "A15",
     "expected_sources": ["dgc"],
     "expected_terms": ["capacidad", "portante", "admisible"]},
    {"text": "estabilitat de talussos factor de seguretat minim",
     "tipologia": "C", "annex": "A15",
     "expected_sources": ["dgc"],
     "expected_terms": ["estabilidad", "talud", "factor", "seguridad"]},
    {"text": "murs pantalla i murs de contencio carregues de terres",
     "tipologia": "G", "annex": "A15",
     "expected_sources": ["dgc"],
     "expected_terms": ["pantalla", "contencion", "empuje"]},
    {"text": "millora del terreny columnes de grava jet grouting",
     "tipologia": "G", "annex": "A15",
     "expected_sources": ["dgc"],
     "expected_terms": ["mejora", "terreno", "columna"]},
    {"text": "classificacio de sols per a terraplens aprofitament",
     "tipologia": "C", "annex": "A15",
     "expected_sources": ["dgc"],
     "expected_terms": ["clasificacion", "suelo", "terraplen"]},

    # FERROVIARI ESPECIFIC (10)
    {"text": "NTE ADIF superestructura de via balast travesses",
     "tipologia": "F", "annex": "A08",
     "expected_sources": ["adif"],
     "expected_terms": ["superestructura", "balasto", "traviesa"]},
    {"text": "tensio de catenaria regulacio automatica compensadors",
     "tipologia": "F", "annex": "A08",
     "expected_sources": ["adif"],
     "expected_terms": ["catenaria", "regulacion", "compensador"]},
    {"text": "sistema ERTMS nivell 2 especificacions tecniques",
     "tipologia": "F", "annex": "A08",
     "expected_sources": ["adif", "eurlex"],
     "expected_terms": ["ERTMS", "nivel", "especificacion"]},
    {"text": "andanes accessibles alcada i separacio amb via",
     "tipologia": "F", "annex": "A08",
     "expected_sources": ["adif"],
     "expected_terms": ["anden", "altura", "accesible"]},
    {"text": "passarel.les i passos inferiors en estacions ferroviaries",
     "tipologia": "F", "annex": "A08",
     "expected_sources": ["adif"],
     "expected_terms": ["pasarela", "paso inferior", "estacion"]},
    {"text": "desviaments i aparells de via ferroviaria",
     "tipologia": "F", "annex": "A08",
     "expected_sources": ["adif"],
     "expected_terms": ["desvio", "aparato", "via"]},
    {"text": "proteccio electrica subestacions de traccio",
     "tipologia": "F", "annex": "A08",
     "expected_sources": ["adif"],
     "expected_terms": ["subestacion", "traccion", "proteccion"]},
    {"text": "telecomunicacions ferroviaries fibra optica GSM-R",
     "tipologia": "F", "annex": "A08",
     "expected_sources": ["adif"],
     "expected_terms": ["telecomunicacion", "fibra", "GSM-R"]},
    {"text": "tancaments i limits de propietat via ferroviaria",
     "tipologia": "F", "annex": "A08",
     "expected_sources": ["adif"],
     "expected_terms": ["cerramiento", "limite", "propiedad"]},
    {"text": "drenatge de plataforma ferroviaria cunetes i baixants",
     "tipologia": "F", "annex": "A07",
     "expected_sources": ["adif"],
     "expected_terms": ["drenaje", "plataforma", "cuneta"]},

    # INSTAL.LACIONS ELECTRIQUES (6)
    {"text": "quadres electrics distribucio ITC-BT-17 quadre general",
     "tipologia": "G", "annex": "A09",
     "expected_sources": ["industria"],
     "expected_terms": ["cuadro", "ITC-BT", "distribucion"]},
    {"text": "posada a terra instal.lacions electriques resistencia",
     "tipologia": "G", "annex": "A09",
     "expected_sources": ["industria"],
     "expected_terms": ["puesta", "tierra", "resistencia"]},
    {"text": "canalitzacions electriques safates i tubs protectors",
     "tipologia": "G", "annex": "A09",
     "expected_sources": ["industria"],
     "expected_terms": ["canalizacion", "bandeja", "tubo"]},
    {"text": "proteccio contra contactes directes i indirectes",
     "tipologia": "G", "annex": "A09",
     "expected_sources": ["industria"],
     "expected_terms": ["contacto", "directo", "indirecto"]},
    {"text": "grup electrogen emergencia potencia calcul",
     "tipologia": "G", "annex": "A09",
     "expected_sources": ["industria"],
     "expected_terms": ["grupo", "electrogeno", "emergencia"]},
    {"text": "linies subterranies mitja tensio cables XLPE",
     "tipologia": "G", "annex": "A09",
     "expected_sources": ["industria"],
     "expected_terms": ["subterranea", "media", "tension", "cable"]},

    # PROTECCIO CONTRA INCENDIS (8)
    {"text": "sectors d'incendi en edificis CTE DB-SI superficie maxima",
     "tipologia": "G", "annex": "A09",
     "expected_sources": ["cte"],
     "expected_terms": ["sector", "incendio", "DB-SI"]},
    {"text": "resistencia al foc d'elements estructurals R-120",
     "tipologia": "G", "annex": "A09",
     "expected_sources": ["cte"],
     "expected_terms": ["resistencia", "fuego", "R-120"]},
    {"text": "recorreguts d'evacuacio longitud maxima sortides",
     "tipologia": "G", "annex": "A09",
     "expected_sources": ["cte"],
     "expected_terms": ["evacuacion", "recorrido", "salida"]},
    {"text": "RIPCI instal.lacions proteccio contra incendis RD 513/2017",
     "tipologia": "G", "annex": "A09",
     "expected_sources": ["industria", "boe"],
     "expected_terms": ["RIPCI", "513/2017"]},
    {"text": "deteccio d'incendis centraleta analogica llacos",
     "tipologia": "G", "annex": "A09",
     "expected_sources": ["industria"],
     "expected_terms": ["deteccion", "incendio", "central"]},
    {"text": "hidrants exteriors xarxa contra incendis cabal minim",
     "tipologia": "G", "annex": "A09",
     "expected_sources": ["industria", "cte"],
     "expected_terms": ["hidrante", "incendio", "caudal"]},
    {"text": "seguretat contra incendis tunels ferroviaris ETI SRT",
     "tipologia": "F", "annex": "A09",
     "expected_sources": ["eurlex", "adif"],
     "expected_terms": ["incendio", "tunel", "SRT", "1303/2014"]},
    {"text": "evacuacio i autoproteccio establiments publics",
     "tipologia": "G", "annex": "A09",
     "expected_sources": ["cte", "boe"],
     "expected_terms": ["evacuacion", "autoproteccion"]},

    # TRAMVIA (8) — AMPLIAT
    {"text": "regulacio tramvies Catalunya normativa autonomica",
     "tipologia": "T", "annex": "",
     "expected_sources": ["pjcat", "territori"],
     "expected_terms": ["tranvia", "Catalunya"]},
    {"text": "senyalitzacio semaforica prioritat tramvia",
     "tipologia": "T", "annex": "A10",
     "expected_sources": ["territori"],
     "expected_terms": ["semaforica", "prioridad", "tranvia"]},
    {"text": "catenaria tramviaria tensio i sistema de suspensio",
     "tipologia": "T", "annex": "A08",
     "expected_sources": ["adif"],
     "expected_terms": ["catenaria", "tranvia", "tension"]},
    {"text": "seccio transversal plataforma tramviaria en zona urbana",
     "tipologia": "T", "annex": "A05",
     "expected_sources": ["territori"],
     "expected_terms": ["seccion", "plataforma", "urbana"]},
    {"text": "accessibilitat parades de tramvia persones PMR",
     "tipologia": "T", "annex": "A11",
     "expected_sources": ["pjcat"],
     "expected_terms": ["accesibilidad", "parada", "tranvia"]},
    {"text": "superestructura via tramvia carril Phoenix embegut",
     "tipologia": "T", "annex": "A08",
     "expected_sources": ["adif"],
     "expected_terms": ["carril", "Phoenix", "embebido"]},
    {"text": "soroll i vibracions tramvia en zona residencial",
     "tipologia": "T", "annex": "A02",
     "expected_sources": ["pjcat", "boe"],
     "expected_terms": ["ruido", "vibracion", "residencial"]},
    {"text": "creuament tramvia amb transit rodat semafors",
     "tipologia": "T", "annex": "A10",
     "expected_sources": ["territori"],
     "expected_terms": ["cruce", "tranvia", "semaforo"]},

    # APARCAMENT (7) — AMPLIAT
    {"text": "ventilacio forcada aparcament soterrat deteccio CO",
     "tipologia": "P", "annex": "A09",
     "expected_sources": ["industria", "cte"],
     "expected_terms": ["ventilacion", "CO", "aparcamiento"]},
    {"text": "rampes acces aparcament pendent maxima amplada minima",
     "tipologia": "P", "annex": "A05",
     "expected_sources": ["cte"],
     "expected_terms": ["rampa", "pendiente", "aparcamiento"]},
    {"text": "places aparcament reserves PMR accessibilitat",
     "tipologia": "P", "annex": "A11",
     "expected_sources": ["pjcat"],
     "expected_terms": ["plaza", "PMR", "accesibilidad"]},
    {"text": "estructura aparcament soterrat carregues sobrecarrega us",
     "tipologia": "P", "annex": "A15",
     "expected_sources": ["dgc", "cte"],
     "expected_terms": ["estructura", "sobrecarga", "aparcamiento"]},
    {"text": "impermeabilitzacio coberta aparcament soterrat",
     "tipologia": "P", "annex": "A06",
     "expected_sources": ["cte"],
     "expected_terms": ["impermeabilizacion", "cubierta"]},
    {"text": "instal.lacio electrica aparcament classificacio zona REBT",
     "tipologia": "P", "annex": "A09",
     "expected_sources": ["industria"],
     "expected_terms": ["electrica", "aparcamiento", "ITC-BT"]},
    {"text": "recarrega vehicles electrics punts aparcament directiva",
     "tipologia": "P", "annex": "A09",
     "expected_sources": ["eurlex", "industria"],
     "expected_terms": ["recarga", "vehiculo", "electrico"]},

    # BUS I INTERMODAL (5)
    {"text": "disseny terminal autobusos interurba darsenes",
     "tipologia": "A", "annex": "",
     "expected_sources": ["territori"],
     "expected_terms": ["terminal", "autobus", "darsena"]},
    {"text": "accessibilitat parades bus plataforma reservada",
     "tipologia": "A", "annex": "A11",
     "expected_sources": ["pjcat"],
     "expected_terms": ["accesibilidad", "parada", "autobus"]},
    {"text": "intercanviador modal transport public connexions",
     "tipologia": "A", "annex": "",
     "expected_sources": ["territori", "pjcat"],
     "expected_terms": ["intercambiador", "transporte", "publico"]},
    {"text": "carril bus segregat senyalitzacio prioritat semaforica",
     "tipologia": "A", "annex": "A10",
     "expected_sources": ["territori", "dgc"],
     "expected_terms": ["carril", "bus", "prioridad"]},
    {"text": "marquesina parada bus mobiliari urba normativa",
     "tipologia": "A", "annex": "",
     "expected_sources": ["territori", "pjcat"],
     "expected_terms": ["marquesina", "parada", "mobiliario"]},

    # CARRIL BICI (5)
    {"text": "disseny vies ciclistes segregades amplada radis minims",
     "tipologia": "B", "annex": "A05",
     "expected_sources": ["territori", "dgc"],
     "expected_terms": ["ciclista", "segregada", "radio"]},
    {"text": "senyalitzacio ciclista pictogrames i marques viaries",
     "tipologia": "B", "annex": "A10",
     "expected_sources": ["dgc", "territori"],
     "expected_terms": ["ciclista", "pictograma", "marca"]},
    {"text": "interseccions carril bici amb calcada solucions tipus",
     "tipologia": "B", "annex": "A05",
     "expected_sources": ["territori"],
     "expected_terms": ["interseccion", "carril", "bici"]},
    {"text": "enllumenat vies ciclistes nivells il.luminacio",
     "tipologia": "B", "annex": "A09",
     "expected_sources": ["industria"],
     "expected_terms": ["alumbrado", "ciclista", "iluminacion"]},
    {"text": "aparcament bicicletes segur cobert places minimes",
     "tipologia": "B", "annex": "",
     "expected_sources": ["territori"],
     "expected_terms": ["aparcamiento", "bicicleta", "plaza"]},

    # METRO (5)
    {"text": "ventilacio mecanica tunels metro extraccio de fums",
     "tipologia": "M", "annex": "A09",
     "expected_sources": ["adif", "industria"],
     "expected_terms": ["ventilacion", "tunel", "humos"]},
    {"text": "portes d'andana sistemes de proteccio viatgers metro",
     "tipologia": "M", "annex": "A08",
     "expected_sources": ["adif"],
     "expected_terms": ["puerta", "anden", "proteccion"]},
    {"text": "accessibilitat estacions metro ascensors escales mecaniques",
     "tipologia": "M", "annex": "A11",
     "expected_sources": ["pjcat", "adif"],
     "expected_terms": ["accesibilidad", "ascensor", "escalera"]},
    {"text": "impermeabilitzacio tunel metro llosa drenant",
     "tipologia": "M", "annex": "A15",
     "expected_sources": ["adif"],
     "expected_terms": ["impermeabilizacion", "tunel", "drenante"]},
    {"text": "instal.lacions contra incendis en estacions subterranies",
     "tipologia": "M", "annex": "A09",
     "expected_sources": ["adif", "industria"],
     "expected_terms": ["incendio", "estacion", "subterranea"]},

    # MEDI AMBIENT (5)
    {"text": "mesures correctores impacte acustic en fase d'obra",
     "tipologia": "G", "annex": "A02",
     "expected_sources": ["pjcat", "boe"],
     "expected_terms": ["acustico", "correctora", "obra"]},
    {"text": "gestio de terres excavacio reutilitzacio valoritzacio",
     "tipologia": "G", "annex": "A02",
     "expected_sources": ["boe", "pjcat"],
     "expected_terms": ["tierra", "excavacion", "reutilizacion"]},
    {"text": "permeabilitat ecologica passos de fauna pas inferior",
     "tipologia": "C", "annex": "A02",
     "expected_sources": ["dgc", "pjcat"],
     "expected_terms": ["fauna", "paso", "permeabilidad"]},
    {"text": "restauracio vegetal revegetacio talussos hidrosembra",
     "tipologia": "C", "annex": "A02",
     "expected_sources": ["dgc"],
     "expected_terms": ["revegetacion", "hidrosiembra", "talud"]},
    {"text": "contaminacio aigues subterranies mesures preventives",
     "tipologia": "G", "annex": "A02",
     "expected_sources": ["aca", "pjcat"],
     "expected_terms": ["contaminacion", "subterranea", "preventiva"]},

    # NORMATIVA DEROGADA (tests de vigencia) (5)
    {"text": "EHE-08 instruccio formigo estructural esta vigent",
     "tipologia": "G", "annex": "A15",
     "expected_sources": ["boe", "dgc"],
     "expected_terms": ["EHE", "estructural", "hormigon"]},
    {"text": "RD 57/2005 ascensors normativa actual vigent",
     "tipologia": "G", "annex": "A09",
     "expected_sources": ["boe", "industria"],
     "expected_terms": ["ascensor", "57/2005"]},
    {"text": "norma UNE-EN 779 filtres aire classificacio vigent",
     "tipologia": "G", "annex": "A09",
     "expected_sources": [],
     "expected_terms": ["filtro", "779"]},
    {"text": "reglament aparells a pressio RD 2060/2008 vigencia",
     "tipologia": "G", "annex": "A09",
     "expected_sources": ["industria", "boe"],
     "expected_terms": ["presion", "2060/2008"]},
    {"text": "ordre MAM/304/2002 residus construccio demolicio estat",
     "tipologia": "G", "annex": "A02",
     "expected_sources": ["boe"],
     "expected_terms": ["MAM", "304/2002", "residuos"]},

    # CONSULTES EN CASTELLA (5)
    {"text": "instruccion de carreteras 3.1-IC trazado",
     "tipologia": "C", "annex": "A05",
     "expected_sources": ["dgc"],
     "expected_terms": ["3.1-IC", "trazado"]},
    {"text": "proteccion contra incendios aparcamientos subterraneos",
     "tipologia": "P", "annex": "A09",
     "expected_sources": ["cte", "industria"],
     "expected_terms": ["incendio", "aparcamiento"]},
    {"text": "codigo estructural hormigon armado recubrimientos",
     "tipologia": "G", "annex": "A15",
     "expected_sources": ["dgc", "boe"],
     "expected_terms": ["estructural", "recubrimiento"]},
    {"text": "senalizacion horizontal marcas viales retrorreflexion",
     "tipologia": "C", "annex": "A10",
     "expected_sources": ["dgc"],
     "expected_terms": ["marcas", "viales", "retrorreflexion"]},
    {"text": "pliego prescripciones tecnicas generales PG-3 aridos",
     "tipologia": "C", "annex": "A06",
     "expected_sources": ["dgc"],
     "expected_terms": ["PG-3", "arido"]},
]


# ══════════════════════════════════════════════════════════════
# GENERACIO
# ══════════════════════════════════════════════════════════════

def generate_dataset():
    """Genera el fitxer JSON de queries."""
    config.DATA_DIR.mkdir(parents=True, exist_ok=True)

    with open(DATASET_PATH, "w", encoding="utf-8") as f:
        json.dump(SYNTHETIC_QUERIES, f, ensure_ascii=False, indent=2)

    per_tipologia = {}
    per_annex = {}
    for q in SYNTHETIC_QUERIES:
        t = q["tipologia"]
        a = q["annex"] or "(sense annex)"
        per_tipologia[t] = per_tipologia.get(t, 0) + 1
        per_annex[a] = per_annex.get(a, 0) + 1

    print(f"  Dataset generat: {len(SYNTHETIC_QUERIES)} queries")
    print(f"  Fitxer: {DATASET_PATH}")
    print(f"\n  Per tipologia:")
    labels = {"C": "Carreteres", "F": "Ferrocarril", "M": "Metro",
              "T": "Tramvia", "A": "Bus", "B": "Bici", "P": "Aparcament",
              "G": "General"}
    for t, n in sorted(per_tipologia.items()):
        print(f"    {labels.get(t, t):15s} {n}")
    print(f"\n  Per annex:")
    for a, n in sorted(per_annex.items()):
        print(f"    {a:15s} {n}")


# ══════════════════════════════════════════════════════════════
# AVALUACIO AMB LLM
# ══════════════════════════════════════════════════════════════

LLM_EVAL_SYSTEM = """Ets un avaluador de rellevancia de cerca per a un sistema de
normativa tecnica d'infraestructures de mobilitat a Catalunya.

Se t'ha proporcionat una CONSULTA d'un enginyer de supervisio
i un FRAGMENT de text recuperat d'un document normatiu.

Puntua la RELLEVANCIA del fragment respecte a la consulta amb:
  3 = Molt rellevant: respon directament la consulta
  2 = Rellevant: conte informacio util relacionada
  1 = Marginalment rellevant: tracta el tema pero no respon
  0 = Irrellevant: no te res a veure amb la consulta

Respon NOMES amb un numero (0, 1, 2 o 3). Res mes."""

LLM_EVAL_USER = """CONSULTA: {query}

FRAGMENT (font: {source}, document: {doc_codi}):
{text}

Puntuacio (0-3):"""

DELAY_BETWEEN_CALLS = 1.5


def evaluate_with_llm(provider: str = "gemini"):
    """Executa queries, valida resultats amb LLM, guarda feedback."""

    if not DATASET_PATH.exists():
        print("  ERROR: Cal primer generar el dataset:")
        print("    python tools/eval_dataset.py generate")
        return

    with open(DATASET_PATH, encoding="utf-8") as f:
        queries = json.load(f)

    import sqlite3
    from search.hybrid_search import HybridSearcher
    from search.feedback import record_feedback
    from llm.llm_provider import LLMProvider

    print(f"  Inicialitzant HybridSearcher...")
    searcher = HybridSearcher(
        sqlite_path=config.SQLITE_PATH,
        chroma_path=config.CHROMA_PATH,
        embedding_model=config.EMBEDDING_MODEL,
    )

    print(f"  Inicialitzant LLM ({provider})...")
    llm = LLMProvider(backend=provider)

    # Carregar queries ja avaluades (per saltar-les)
    already_evaluated = set()
    try:
        conn = sqlite3.connect(config.SQLITE_PATH)
        for row in conn.execute("SELECT DISTINCT query FROM search_feedback"):
            already_evaluated.add(row[0])
        conn.close()
    except Exception:
        pass

    # Carregar resultats previs si existeixen
    all_results = []
    if RESULTS_PATH.exists():
        try:
            with open(RESULTS_PATH, encoding="utf-8") as f:
                all_results = json.load(f)
        except Exception:
            pass

    total_feedback = 0
    total_relevant = 0
    total_irrelevant = 0
    skipped = 0

    pending = [q for q in queries if q["text"] not in already_evaluated]
    print(f"\n  Total queries: {len(queries)}, ja avaluades: {len(already_evaluated)}, pendents: {len(pending)}")
    print(f"  Estimacio: {len(pending) * 5 * DELAY_BETWEEN_CALLS / 60:.0f} minuts\n")

    for qi, q in enumerate(pending, 1):
        query_text = q["text"]
        tipologia = q["tipologia"]
        annex = q.get("annex", "")

        print(f"  [{qi:3d}/{len(pending)}] {query_text[:60]}...", flush=True)

        try:
            results = searcher.search(query_text, top_k=5)
        except Exception as exc:
            print(f"    ERROR cerca: {exc}")
            continue

        query_evals = []

        for ri, r in enumerate(results):
            meta = r.get("metadata", {})
            text = (r.get("document") or "")[:400]
            source = meta.get("source", "")
            doc_codi = meta.get("doc_codi", "")
            doc_titol = meta.get("doc_titol", "")
            page = meta.get("page", 0)
            score = r.get("score", 0)
            methods = r.get("methods", [])

            time.sleep(DELAY_BETWEEN_CALLS)
            try:
                user_msg = LLM_EVAL_USER.format(
                    query=query_text,
                    source=source,
                    doc_codi=doc_codi,
                    text=text,
                )
                result_llm = llm.call(
                    system=LLM_EVAL_SYSTEM,
                    user_message=user_msg,
                    max_tokens=10,
                    temperature=0,
                )

                raw_score = result_llm.get("text", "").strip()
                m = re.search(r"[0-3]", raw_score)
                llm_score = int(m.group()) if m else -1

            except Exception as exc:
                print(f"    LLM error resultat {ri+1}: {exc}")
                llm_score = -1
                continue

            relevant = llm_score >= 2

            record_feedback(
                query=query_text,
                doc_codi=doc_codi,
                doc_titol=doc_titol,
                source=source,
                page=page,
                rank_position=ri + 1,
                score=score,
                relevant=relevant,
                methods=",".join(methods) if methods else "",
                text_preview=text[:200],
            )
            total_feedback += 1
            if relevant:
                total_relevant += 1
            else:
                total_irrelevant += 1

            query_evals.append({
                "rank": ri + 1,
                "source": source,
                "doc_codi": doc_codi,
                "llm_score": llm_score,
                "search_score": score,
                "relevant": relevant,
            })

            marker = "+" if relevant else "-"
            print(f"    {marker} #{ri+1} [{source:6s}] {doc_codi[:30]:30s} LLM:{llm_score}",
                  flush=True)

        all_results.append({
            "query": query_text,
            "tipologia": tipologia,
            "annex": annex,
            "results": query_evals,
        })

    config.DATA_DIR.mkdir(parents=True, exist_ok=True)
    with open(RESULTS_PATH, "w", encoding="utf-8") as f:
        json.dump(all_results, f, ensure_ascii=False, indent=2)

    print(f"\n  {'='*50}")
    print(f"  AVALUACIO COMPLETADA")
    print(f"  {'='*50}")
    print(f"  Queries avaluades:  {len(queries)}")
    print(f"  Resultats avaluats: {total_feedback}")
    print(f"  Rellevants (>=2):   {total_relevant} "
          f"({total_relevant/max(total_feedback,1)*100:.0f}%)")
    print(f"  Irrellevants (<=1): {total_irrelevant} "
          f"({total_irrelevant/max(total_feedback,1)*100:.0f}%)")
    print(f"  Resultats desats a: {RESULTS_PATH}")


# ══════════════════════════════════════════════════════════════
# INFORME
# ══════════════════════════════════════════════════════════════

def generate_report():
    """Genera informe d'avaluacio a partir dels resultats."""

    if not RESULTS_PATH.exists():
        print("  ERROR: Cal primer executar l'avaluacio:")
        print("    python tools/eval_dataset.py evaluate")
        return

    with open(RESULTS_PATH, encoding="utf-8") as f:
        all_results = json.load(f)

    per_tipologia = {}
    per_source = {}
    per_rank = {1: [], 2: [], 3: [], 4: [], 5: []}

    for entry in all_results:
        tip = entry["tipologia"]
        if tip not in per_tipologia:
            per_tipologia[tip] = {"total": 0, "relevant": 0}

        for r in entry["results"]:
            llm_score = r.get("llm_score", -1)
            if llm_score < 0:
                continue

            relevant = llm_score >= 2

            per_tipologia[tip]["total"] += 1
            if relevant:
                per_tipologia[tip]["relevant"] += 1

            src = r.get("source", "?")
            if src not in per_source:
                per_source[src] = {"total": 0, "relevant": 0, "scores": []}
            per_source[src]["total"] += 1
            if relevant:
                per_source[src]["relevant"] += 1
            per_source[src]["scores"].append(llm_score)

            rank = r.get("rank", 0)
            if rank in per_rank:
                per_rank[rank].append(llm_score)

    labels = {"C": "Carreteres", "F": "Ferrocarril", "M": "Metro",
              "T": "Tramvia", "A": "Bus", "B": "Bici", "P": "Aparcament",
              "G": "General"}

    print(f"\n  {'='*60}")
    print(f"  INFORME D'AVALUACIO NormaCat")
    print(f"  {'='*60}")

    print(f"\n  Per tipologia:")
    for tip in sorted(per_tipologia.keys()):
        d = per_tipologia[tip]
        ratio = d["relevant"] / d["total"] * 100 if d["total"] > 0 else 0
        label = labels.get(tip, tip)
        bar = "#" * int(ratio / 5) + "." * (20 - int(ratio / 5))
        print(f"    {label:15s} {bar} {ratio:5.1f}%  ({d['relevant']}/{d['total']})")

    print(f"\n  Per font:")
    for src in sorted(per_source.keys(), key=lambda s: -per_source[s]["total"]):
        d = per_source[src]
        ratio = d["relevant"] / d["total"] * 100 if d["total"] > 0 else 0
        avg = sum(d["scores"]) / len(d["scores"]) if d["scores"] else 0
        src_label = config.SOURCES.get(src, {}).get("label", src)
        print(f"    {src_label:20s} {ratio:5.1f}% rellevant  "
              f"(avg LLM: {avg:.1f})  n={d['total']}")

    print(f"\n  Per posicio al ranking:")
    for rank in sorted(per_rank.keys()):
        scores = per_rank[rank]
        if not scores:
            continue
        avg = sum(scores) / len(scores)
        rel = sum(1 for s in scores if s >= 2) / len(scores) * 100
        print(f"    #{rank}: avg={avg:.1f}  {rel:.0f}% rellevant  (n={len(scores)})")

    problematic = []
    for entry in all_results:
        relevant_count = sum(1 for r in entry["results"]
                           if r.get("llm_score", 0) >= 2)
        if relevant_count == 0 and entry["results"]:
            problematic.append(entry["query"])

    if problematic:
        print(f"\n  Queries SENSE cap resultat rellevant ({len(problematic)}):")
        for q in problematic:
            print(f"    - {q}")


# ══════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════

def main():
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")

    parser = argparse.ArgumentParser(
        description="Dataset sintetic i avaluacio automatica NormaCat"
    )
    parser.add_argument("command", choices=["generate", "evaluate", "report"],
                        help="generate=crea queries, evaluate=valida amb LLM, report=estadistiques")
    parser.add_argument("--provider", default="gemini",
                        help="LLM per avaluar (default: gemini)")
    args = parser.parse_args()

    print("=" * 50)
    print("  NormaCat -- Avaluacio Automatica")
    print("=" * 50)
    print()

    if args.command == "generate":
        generate_dataset()
    elif args.command == "evaluate":
        evaluate_with_llm(provider=args.provider)
    elif args.command == "report":
        generate_report()


if __name__ == "__main__":
    main()
