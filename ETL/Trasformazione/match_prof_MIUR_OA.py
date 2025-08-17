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


from matching_functionsV3 import (
    calculate_name_score, 
    find_best_matches, 
    resolve_matches,
    parse_professor_name,
    parse_author_name
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

#FUNCTIONS

def leggi_dati_rubrica():

    file_prof = RUBRICA_CSV

    prof_miur = pd.read_csv(file_prof, sep=',')

    prof_data = prof_miur[['id', 'Cognome e Nome', 'ror', 'id_oa']]

    print(prof_data.head(5))

def get_authors_by_institution_id(institution_id, collection):
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
                
            alternatives = doc.get("display_name_alternatives", [])
            if not isinstance(alternatives, list):
                alternatives = []
            
            authors_dict[author_id] = {
                "id": author_id,
                "orcid": doc.get("orcid"),
                "display_name": doc.get("display_name", ""),
                "display_name_alternatives": alternatives
            }
        
        print(f"Found {len(authors_dict)} authors for institution {institution_id}")
        return authors_dict
        
    except Exception as e:
        print(f"Error: {e}")
        return {}
    
def setup_mongo_connection():
    """
    Connessione al cluster MongoDB
    """
    try:
        client = MongoClient(MONGO_URI)  # Modifica con i tuoi parametri
        db = client[DATABASE_NAME]
        collection = db[OA_AUTH_COLLECTION_NAME]
        
        # Test connessione
        client.admin.command('ping')
        print("MongoDB connection successful!")
        
        return collection
    except Exception as e:
        print(f"MongoDB connection failed: {e}")
        return None

def get_mongo_collection():
    client = MongoClient(MONGO_URI)  # I tuoi parametri
    db = client.unisurf  # Nome del tuo database
    collection = db[OA_AUTH_COLLECTION_NAME]  # Nome della collezione
    return collection

def load_professor_stack_forid(file_path, id_ateneo):
   """
   Carica il CSV dei professori e restituisce una lista (stack)
   
   Args:
       file_path (str): Percorso del file CSV
   
   Returns:
       list: Lista di dizionari con i dati dei professori
   """
   try:
       df = pd.read_csv(file_path)
       
       # Converte in lista di dizionari
       professor_stack = []
       for _, row in df.iterrows():
           if row["id_oa"] == id_ateneo:
            professor_stack.append({
                "id": row["id"],
                "fascia": row["Fascia"],
                "cognome": row["Cognome e Nome"],  # O row["Cognome"] se separato
                "nome_completo": row["Cognome e Nome"],
                "ateneo": row["Ateneo"],
                "id_oa_ateneo": row["id_oa"]
           })
       
       print(f"Loaded {len(professor_stack)} professors")
       return professor_stack
       
   except Exception as e:
       print(f"Error loading CSV: {e}")
       return []

def get_university_list():
   df = pd.read_csv(RUBRICA_CSV)
   return df['id_oa'].dropna().unique().tolist()

def matching_algorithm(prof, oa_authors):

    name = prof.get('cognome')



    print(f"Eseguo Match di {name}")

def main_professor_matcher():

    setup_mongo_connection()  # inizializza la connessione al DB Mongo
    atenei_id = get_university_list()  # legge la lista di atenei dal CSV Miur
    
    # Prepara il file CSV di output
    output_filename = OUTPUT_FILE
    
    # Apri il file CSV in modalità append per scrivere man mano
    with open(output_filename, 'w', newline='', encoding='utf-8') as csvfile:
        fieldnames = [
            'ateneo_id', 
            'score', 
            'cognome_rubrica', 
            'nome_completo_rubrica',
            'display_name_openalex', 
            'author_id_openalex', 
            'orcid'
        ]
        
        csv_writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        csv_writer.writeheader()
        
        total_matches = 0
        
        for ateneo_id in tqdm(atenei_id, desc='Progresso Atenei'):
            print(f"Elaborando Ateneo: {ateneo_id}")
            
            try:
                # Carica dati per questo ateneo
                professor_stack = load_professor_stack_forid(RUBRICA_CSV, ateneo_id)
                oa_authors = get_authors_by_institution_id(ateneo_id, get_mongo_collection())
                
                print(f"  Professori trovati: {len(professor_stack)}")
                print(f"  Autori OpenAlex trovati: {len(oa_authors)}")
                
                if not professor_stack or not oa_authors:
                    print(f"  Saltato - dati mancanti per ateneo {ateneo_id}")
                    continue
                
                # Esegui matching
                all_matches = find_best_matches(professor_stack, oa_authors)
                final_matches = resolve_matches(all_matches, oa_authors, threshold=75.0)
                
                print(f"  Match trovati: {len(final_matches)}")
                
                # Scrivi i risultati nel CSV
                for match in final_matches:
                    # Trova il professore originale per ottenere nome completo
                    prof_data = None
                    for prof in professor_stack:
                        if prof.get('cognome') == match['cognome_rubrica']:
                            prof_data = prof
                            break
                    
                    nome_completo_rubrica = prof_data.get('nome_completo', '') if prof_data else ''
                    
                    row = {
                        'ateneo_id': ateneo_id,
                        'score': f"{match['score']:.2f}",
                        'cognome_rubrica': match['cognome_rubrica'],
                        'nome_completo_rubrica': nome_completo_rubrica,
                        'display_name_openalex': match['display_name'],
                        'author_id_openalex': match['author_id'],
                        'orcid': match['orcid'] or ''
                    }
                    
                    csv_writer.writerow(row)
                    total_matches += 1
                
                # Flush per assicurarsi che i dati vengano scritti subito
                csvfile.flush()
                
                print(f"Completato ateneo {ateneo_id} - {len(final_matches)} match salvati")
                
            except Exception as e:
                print(f"Errore nell'elaborazione dell'ateneo {ateneo_id}: {e}")
                continue
        
        print(f"\n🎉 ELABORAZIONE COMPLETATA!")
        print(f"📊 Statistiche finali:")
        print(f"   Atenei elaborati: {len(atenei_id)}")
        print(f"   Match totali trovati: {total_matches}")
        print(f"   File di output: {output_filename}")

# USAGE 

main_professor_matcher()

# Analisi sui nomi

# Esempio di utilizzo:
#analysis = analyze_names_in_dictionary(oa_authors)
#print_analysis_report(analysis)
