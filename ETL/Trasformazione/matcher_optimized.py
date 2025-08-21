import pandas as pd
import pymongo
import json
import csv
from pymongo import MongoClient
from rapidfuzz import fuzz
import re
import unicodedata
from collections import Counter
import controllo_nomi
from datetime import datetime
from rapidfuzz import fuzz
from rapidfuzz.fuzz import token_set_ratio
from tqdm import tqdm
import logging
import gc

"""
Script ottimizzato per il matching dei nomi dei professori nella lista MIUR con gli autori OpenAlex.
Versione con preprocessing per migliorare le performance e logging dettagliato.
"""

from matching_functionsV4 import (
    preprocess_professor_data,
    preprocess_authors_data,
    calculate_name_score_optimized, 
    find_best_matches_optimized, 
    resolve_matches,
    cleanup_preprocessed_data
)

# CONFIGURATION
RUBRICA_CSV = "data/tabelle/RubricaMIURStatEnriched.csv"
RESULTS_CSV = "ETL/Matching/match_authors_OA_MIUR.csv"
MONGO_URI = "mongodb://localhost:27017/"
DATABASE_NAME = "unisurf" 
OA_AUTH_COLLECTION_NAME = "oa_authors_test"
OUTPUT_FILE = f"data/tabelle_ponte/professor_author_matches_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"

HIGH_THRESHOLD = 0.85
LOW_THRESHOLD = 0.6
SCORE_APPEND_THRESHOLD = 40  # score minimo per essere aggiunti alla lista di score
RESOLVE_THRESHOLD = 86.0      # Abbassata da 88.0 per più match

# SETUP LOGGING
def setup_logging():
    """Setup logging dettagliato"""
    log_filename = f"matching_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_filename, encoding='utf-8'),
            logging.StreamHandler()
        ]
    )
    
    logger = logging.getLogger(__name__)
    logger.info("=== AVVIO SCRIPT MATCHING PROFESSORI-AUTORI ===")
    logger.info(f"File di log: {log_filename}")
    logger.info(f"Soglia append: {SCORE_APPEND_THRESHOLD}")
    logger.info(f"Soglia resolve: {RESOLVE_THRESHOLD}")
    
    return logger

logger = setup_logging()

# FUNCTIONS

def get_authors_by_institution_id(institution_id, collection):
    """
    Ottiene gli autori associati ad un institution_id tramite query Mongo

    Args:
        institution_id: id dell'istituzione OpenAlex
        collection: collection MongoDB in cui cercare gli autori
    Returns:
        Un dizionario che ha come chiave l'id OpenAlex dell'autore e i campi di interesse per il matching
    """
    logger.debug(f"Querying authors for institution {institution_id}")
    
    pipeline = [
        {"$unwind": "$affiliations"},
        {"$match": {"affiliations.institution.id": institution_id}},
        {"$project": {
            "id": 1,
            "orcid": 1,
            "display_name": 1,
            "display_name_alternatives": 1
        }}
    ]
    
    try:
        cursor = collection.aggregate(pipeline)
        authors_dict = {}
        
        for doc in cursor:
            author_id = doc.get("id")
            if not author_id:
                continue

            # Disabilitate per motivi di performance
            # alternatives = doc.get("display_name_alternatives", [])
            # if not isinstance(alternatives, list):
            #     alternatives = []
                
            authors_dict[author_id] = {
                "id": author_id,
                "orcid": doc.get("orcid"),
                "display_name": doc.get("display_name", ""),
                # "display_name_alternatives": alternatives
            }
        
        logger.info(f"Found {len(authors_dict)} authors for institution {institution_id}")
        return authors_dict
        
    except Exception as e:
        logger.error(f"Error querying authors for institution {institution_id}: {e}")
        return {}
    
def setup_mongo_connection():
    """
    Connessione al cluster MongoDB
    """
    try:
        client = MongoClient(MONGO_URI)
        db = client[DATABASE_NAME]
        collection = db[OA_AUTH_COLLECTION_NAME]
        
        # Test connessione
        client.admin.command('ping')
        logger.info("MongoDB connection successful!")
        
        return collection
    except Exception as e:
        logger.error(f"MongoDB connection failed: {e}")
        return None

def get_mongo_collection():
    client = MongoClient(MONGO_URI)
    db = client[DATABASE_NAME]
    collection = db[OA_AUTH_COLLECTION_NAME]
    return collection

def load_professor_stack_forid(file_path, id_ateneo):
    """
    Carica il CSV dei professori e restituisce una lista per un specifico ateneo
    
    Args:
        file_path (str): Percorso del file CSV
        id_ateneo: ID dell'ateneo da filtrare
    
    Returns:
        list: Lista di dizionari con i dati dei professori
    """
    try:
        logger.debug(f"Loading professors for ateneo {id_ateneo}")
        df = pd.read_csv(file_path)
        
        # Debug info sui tipi di dato
        logger.debug(f"Tipo id_ateneo: {type(id_ateneo)}")
        if not df.empty:
            logger.debug(f"Tipo id_oa nel CSV: {type(df['id_oa'].iloc[0])}")
            logger.debug(f"Primi 3 valori id_oa: {df['id_oa'].head(3).tolist()}")
        
        # Converte in lista di dizionari
        professor_stack = []
        for _, row in df.iterrows():
            # Controllo più robusto per il matching degli ID
            if pd.notna(row["id_oa"]) and str(row["id_oa"]) == str(id_ateneo):
                professor_stack.append({
                    "id": row["id"],
                    "fascia": row["Fascia"],
                    "nome_completo": row["Cognome e Nome"],
                    "ateneo": row["Ateneo"],
                    "id_oa_ateneo": row["id_oa"]
                })
        
        logger.info(f"Loaded {len(professor_stack)} professors for ateneo {id_ateneo}")
        return professor_stack
        
    except Exception as e:
        logger.error(f"Error loading CSV for ateneo {id_ateneo}: {e}")
        return []

def get_university_list():
    """Legge il file con la lista di università per ottenere gli id OpenAlex da usare nella query mongo di ricerca di autori"""
    try:
        df = pd.read_csv(RUBRICA_CSV)
        university_list = df['id_oa'].dropna().unique().tolist()
        logger.info(f"Found {len(university_list)} universities in MIUR data")
        logger.debug(f"First 5 university IDs: {university_list[:5]}")
        return university_list
    except Exception as e:
        logger.error(f"Error loading university list: {e}")
        return []

def log_preprocessing_stats(professors, authors_dict, ateneo_id):
    """Log dettagliato delle statistiche di preprocessing"""
    logger.info(f"=== PREPROCESSING STATS - ATENEO {ateneo_id} ===")
    logger.info(f"Professori: {len(professors)}")
    logger.info(f"Autori OpenAlex: {len(authors_dict)}")
    
    if professors:
        # Esempio di professore
        prof_example = professors[0]
        logger.debug(f"Esempio professore: {prof_example.get('nome_completo', 'N/A')}")
        if 'nome_completo_normalized' in prof_example:
            logger.debug(f"  Normalizzato: {prof_example['nome_completo_normalized']}")
            logger.debug(f"  Token: {prof_example.get('nome_completo_tokens', [])}")
    
    if authors_dict:
        # Esempio di autore
        author_example = list(authors_dict.values())[0]
        logger.debug(f"Esempio autore: {author_example.get('display_name', 'N/A')}")
        if 'display_name_normalized' in author_example:
            logger.debug(f"  Normalizzato: {author_example['display_name_normalized']}")
            logger.debug(f"  Token: {author_example.get('display_name_tokens', [])}")

def log_matching_stats(all_matches, final_matches, ateneo_id):
    """Log dettagliato delle statistiche di matching"""
    logger.info(f"=== MATCHING STATS - ATENEO {ateneo_id} ===")
    logger.info(f"Tutti i match candidati: {len(all_matches)}")
    
    if all_matches:
        scores = [match[0] for match in all_matches]
        logger.info(f"Score range: {min(scores):.1f} - {max(scores):.1f}")
        logger.info(f"Score medio: {sum(scores)/len(scores):.1f}")
        
        # Distribuzione per fasce di score
        high_scores = len([s for s in scores if s >= 80])
        medium_scores = len([s for s in scores if 60 <= s < 80])
        low_scores = len([s for s in scores if s < 60])
        
        logger.info(f"Score >=80: {high_scores}, 60-79: {medium_scores}, <60: {low_scores}")
        
        # Top 3 match
        logger.debug("Top 3 match candidati:")
        for i, (score, prof, author_id, match_type) in enumerate(all_matches[:3]):
            logger.debug(f"  {i+1}. Score: {score:.1f} | {prof.get('nome_completo', 'N/A')} -> {author_id}")
    
    logger.info(f"Match finali (soglia {RESOLVE_THRESHOLD}): {len(final_matches)}")
    
    if final_matches:
        final_scores = [match['score'] for match in final_matches]
        logger.info(f"Score finali range: {min(final_scores):.1f} - {max(final_scores):.1f}")

def main_professor_matcher():
    """Funzione principale ottimizzata con preprocessing e logging dettagliato"""
    
    logger.info("=== INIZIO ELABORAZIONE ===")
    
    """
    # Setup connessione MongoDB
    if not setup_mongo_connection():
        logger.error("Impossibile connettersi a MongoDB. Script terminato.")
        return
    """
    # Carica lista atenei
    atenei_id = get_university_list()
    if not atenei_id:
        logger.error("Nessun ateneo trovato. Script terminato.")
        return
    
    logger.info(f"Elaborazione di {len(atenei_id)} atenei")
    
    # Apri il file CSV in modalità append per scrivere man mano
    with open(OUTPUT_FILE, 'w', newline='', encoding='utf-8') as csvfile:
        fieldnames = [
            'ateneo_id', 
            'score', 
            'nome_completo_rubrica',
            'display_name_openalex', 
            'author_id_openalex', 
            'orcid'
        ]
        
        csv_writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        csv_writer.writeheader()

        # Statistiche globali
        total_oa_authors = 0
        total_miur_profs = 0
        total_matches = 0
        processed_atenei = 0
        
        for ateneo_id in tqdm(atenei_id[:2], desc='Progresso Atenei'):  # Rimuovi [:2] per elaborare tutti
            logger.info(f"\n{'='*50}")
            logger.info(f"ELABORANDO ATENEO: {ateneo_id}")
            logger.info(f"{'='*50}")
            
            try:
                # 1. CARICAMENTO DATI
                logger.info("1. Caricamento dati...")
                professor_stack = load_professor_stack_forid(RUBRICA_CSV, ateneo_id)
                oa_authors = get_authors_by_institution_id(ateneo_id, get_mongo_collection())

                total_oa_authors += len(oa_authors)
                total_miur_profs += len(professor_stack)
                
                if not professor_stack or not oa_authors:
                    logger.warning(f"Saltato ateneo {ateneo_id} - dati mancanti (prof: {len(professor_stack)}, autori: {len(oa_authors)})")
                    continue
                
                # 2. PREPROCESSING
                logger.info("2. Preprocessing dati...")
                preprocess_professor_data(professor_stack)
                preprocess_authors_data(oa_authors)
                
                # Log statistiche preprocessing
                log_preprocessing_stats(professor_stack, oa_authors, ateneo_id)
                
                # 3. MATCHING
                logger.info("3. Esecuzione matching...")
                all_matches = find_best_matches_optimized(professor_stack, oa_authors, SCORE_APPEND_THRESHOLD)
                final_matches = resolve_matches(all_matches, oa_authors, RESOLVE_THRESHOLD)

                # Log statistiche matching
                log_matching_stats(all_matches, final_matches, ateneo_id)
                
                # 4. SALVATAGGIO RISULTATI
                logger.info("4. Salvataggio risultati...")
                for match in final_matches:
                    # Trova il professore originale per ottenere nome completo
                    prof_data = None
                    for prof in professor_stack:
                        if prof.get('nome_completo') == match['nome_completo_rubrica']:
                            prof_data = prof
                            break
                    
                    nome_completo_rubrica = prof_data.get('nome_completo', '') if prof_data else ''
                    
                    row = {
                        'ateneo_id': ateneo_id,
                        'score': f"{match['score']:.2f}",
                        'nome_completo_rubrica': nome_completo_rubrica,
                        'display_name_openalex': match['display_name'],
                        'author_id_openalex': match['author_id'],
                        'orcid': match['orcid'] or ''
                    }
                    
                    csv_writer.writerow(row)
                    total_matches += 1
                
                # Flush per assicurarsi che i dati vengano scritti subito
                csvfile.flush()
                
                logger.info(f"✅ Completato ateneo {ateneo_id} - {len(final_matches)} match salvati")
                processed_atenei += 1

                # 5. CLEANUP
                logger.info("5. Cleanup memoria...")
                cleanup_preprocessed_data(professor_stack, oa_authors)
                
                # Cleanup esplicito dei dati di ateneo per performance
                del professor_stack
                del oa_authors  
                del all_matches
                del final_matches

                # Garbage collection forzato
                gc.collect()
                
            except Exception as e:
                logger.error(f"❌ Errore nell'elaborazione dell'ateneo {ateneo_id}: {e}")
                continue
        
        # STATISTICHE FINALI
        logger.info(f"\n{'='*60}")
        logger.info(f"🎉 ELABORAZIONE COMPLETATA!")
        logger.info(f"{'='*60}")
        logger.info(f"📊 Statistiche finali:")
        logger.info(f"   Atenei elaborati: {processed_atenei}/{len(atenei_id)}")
        logger.info(f"   Professori MIUR totali: {total_miur_profs}")
        logger.info(f"   Autori OpenAlex totali: {total_oa_authors}")
        logger.info(f"   Match totali trovati: {total_matches}")
        
        if total_miur_profs > 0:
            match_percentage = (total_matches / total_miur_profs) * 100
            logger.info(f"   Percentuale professori MIUR matchati: {match_percentage:.2f}%")
        
        logger.info(f"   File di output: {OUTPUT_FILE}")
        logger.info(f"{'='*60}")

if __name__ == "__main__":
    main_professor_matcher()