import pandas as pd
from rapidfuzz.distance import JaroWinkler
from rapidfuzz import fuzz
from tqdm import tqdm
import csv
from datetime import datetime

def normalize_institution_name(name):
    """Normalizza il nome dell'istituzione per il confronto"""
    if not name:
        return ""
    
    # Rimuove caratteri speciali comuni
    import re
    name = re.sub(r'[‐‑–—]', '-', name)
    name = re.sub(r'[''`´]', "'", name)
    
    # Rimuove spazi multipli e normalizza
    name = re.sub(r'\s+', ' ', name.strip())
    
    return name

def remove_stopwords_from_institution(name):
    """Rimuove stop words dai nomi delle istituzioni"""
    if not name:
        return ""
    
    # Stop words da rimuovere (case insensitive)
    stopwords = [
        'università', 'degli', 'studi', 'di', 'della', 'del', 'dello', 'delle',
        'university', 'of', 'studies'
    ]
    
    # Normalizza e dividi in token
    name = normalize_institution_name(name)
    tokens = name.split()
    
    # Rimuovi stop words mantenendo case originale per il resto
    filtered_tokens = []
    for token in tokens:
        if token.lower() not in stopwords:
            filtered_tokens.append(token)
    
    return ' '.join(filtered_tokens)

def calculate_institution_score(miur_clean, oa_clean):
    """
    Calcola score di matching tra nomi già puliti
    """
    if not miur_clean or not oa_clean:
        return 0.0
    
    # Se dopo aver rimosso stop words rimane poco, score basso
    if len(miur_clean.strip()) < 2 or len(oa_clean.strip()) < 2:
        return 0.0
    
    # Normalizza case
    miur_upper = miur_clean.upper()
    oa_upper = oa_clean.upper()
    
    # Usa sia Jaro-Winkler che token_set per nomi di istituzioni
    jaro_score = JaroWinkler.similarity(miur_upper, oa_upper) * 100
    token_score = fuzz.token_set_ratio(miur_upper, oa_upper)
    
    # Prendi il punteggio migliore
    final_score = max(jaro_score, token_score)
    
    return final_score

def greedy_institution_matching(miur_stack, oa_stack, threshold=75.0):
    """
    Greedy matching: ogni università MIUR trova il suo migliore match e lo prende
    """
    final_matches = []
    used_openalex = set()  # Track delle istituzioni OpenAlex già assegnate
    
    print(f"🎯 Greedy matching con soglia {threshold}")
    
    for miur_idx in tqdm(range(len(miur_stack)), desc="Matching università"):
        miur_row = miur_stack[miur_idx]
        miur_name = miur_row['NomeOperativo']
        miur_clean = miur_row['nome_pulito']
        
        best_score = 0
        best_match_idx = None
        best_match_type = None
        best_oa_name = None
        
        # Cerca il migliore tra quelli disponibili
        for oa_idx in range(len(oa_stack)):
            if oa_idx in used_openalex:
                continue  # Già assegnata, salta
            
            oa_row = oa_stack[oa_idx]
            
            # Score con display_name
            oa_clean = oa_row['display_name_pulito']
            score = calculate_institution_score(miur_clean, oa_clean)
            
            if score > best_score:
                best_score = score
                best_match_idx = oa_idx
                best_match_type = 'display_name'
                best_oa_name = oa_row['display_name']
            
            # Score con alternatives se score non soddisfacente
            if best_score < threshold and 'alternatives_pulite' in oa_row:
                for alt_idx, alt_clean in enumerate(oa_row['alternatives_pulite']):
                    if alt_clean:
                        alt_score = calculate_institution_score(miur_clean, alt_clean)
                        if alt_score > best_score:
                            best_score = alt_score
                            best_match_idx = oa_idx
                            best_match_type = 'alternative'
                            best_oa_name = oa_row['display_name_alternatives'][alt_idx]
        
        # Se trovato un match sopra soglia, assegnalo
        if best_match_idx is not None and best_score >= threshold:
            match_result = {
                'score': best_score,
                'nome_operativo_miur': miur_name,
                'nome_esteso_miur': miur_row['NomeEsteso'],
                'display_name_oa': best_oa_name,
                'id_openalex': oa_stack[best_match_idx]['id'],
                'match_type': best_match_type
            }
            
            final_matches.append(match_result)
            used_openalex.add(best_match_idx)  # Marca come utilizzata
    
    return final_matches

def prepare_institution_stacks(miur_data, oa_data):
    """
    Prepara gli stack pre-computando i nomi puliti per velocità
    """
    print("🧹 Pre-elaborazione nomi (rimozione stop words)...")
    
    # Stack MIUR
    miur_stack = []
    for _, row in tqdm(miur_data.iterrows(), total=len(miur_data), desc="Preparando MIUR"):
        miur_row = {
            'NomeOperativo': row['NomeOperativo'],
            'NomeEsteso': row['NomeEsteso'],
            'nome_pulito': remove_stopwords_from_institution(row['NomeOperativo'])
        }
        miur_stack.append(miur_row)
    
    # Stack OpenAlex
    oa_stack = []
    for _, row in tqdm(oa_data.iterrows(), total=len(oa_data), desc="Preparando OpenAlex"):
        oa_row = {
            'id': row['id'],
            'display_name': row['display_name'],
            'display_name_pulito': remove_stopwords_from_institution(row['display_name']),
            'display_name_alternatives': row.get('display_name_alternatives', [])
        }
        
        # Pre-pulisci anche le alternative
        alternatives = row.get('display_name_alternatives', [])
        if isinstance(alternatives, list) and alternatives:
            oa_row['alternatives_pulite'] = [
                remove_stopwords_from_institution(alt) for alt in alternatives
            ]
        else:
            oa_row['alternatives_pulite'] = []
        
        oa_stack.append(oa_row)
    
    return miur_stack, oa_stack

def main_greedy_institution_matcher():
    """Funzione principale per il matching greedy delle istituzioni"""
    
    print("🏛️  GREEDY MATCHING ISTITUZIONI MIUR-OPENALEX")
    print("=" * 60)
    
    # Carica dati MIUR
    print("📚 Caricamento dati MIUR...")
    file_miur = "data/tabelle/AteneiEnriched.csv"
    atenei_miur = pd.read_csv(file_miur, sep=',')
    miur_data = atenei_miur[['NomeEsteso', 'NomeOperativo']]
    print(f"   Istituzioni MIUR trovate: {len(miur_data)}")
    
    # Carica dati OpenAlex
    print("🌐 Caricamento dati OpenAlex...")
    file_oa = "data/raw_data/openalex/institutions_it.jsonl"
    istituzioni_oa = pd.read_json(file_oa, lines=True)
    print(f"   Istituzioni OpenAlex trovate: {len(istituzioni_oa)}")
    
    # Prepara stack con nomi pre-puliti
    miur_stack, oa_stack = prepare_institution_stacks(miur_data, istituzioni_oa)
    
    # Mostra esempi di pulizia
    print(f"\n🧹 Esempi pulizia stop words:")
    for i in range(min(3, len(miur_stack))):
        original = miur_stack[i]['NomeOperativo']
        cleaned = miur_stack[i]['nome_pulito']
        print(f"   MIUR: '{original}' → '{cleaned}'")
    
    if len(oa_stack) > 0:
        original = oa_stack[0]['display_name']
        cleaned = oa_stack[0]['display_name_pulito']
        print(f"   OpenAlex: '{original}' → '{cleaned}'")
    
    print(f"\n⚙️  Stop words: università, degli, studi, di, della, del, dello, delle, university, of, studies")
    
    # Esegui greedy matching
    print(f"\n🎯 Avvio greedy matching (soglia: 75.0)...")
    print("   Logica: ogni università MIUR trova il suo miglior match disponibile")
    
    final_matches = greedy_institution_matching(miur_stack, oa_stack, threshold=75.0)
    
    # Salva risultati
    output_filename = f"greedy_institution_matches_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    
    print(f"\n💾 Salvataggio risultati in {output_filename}...")
    
    with open(output_filename, 'w', newline='', encoding='utf-8') as csvfile:
        fieldnames = [
            'score',
            'nome_operativo_miur', 
            'nome_esteso_miur',
            'display_name_oa',
            'id_openalex',
            'match_type'
        ]
        
        csv_writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        csv_writer.writeheader()
        
        for match in final_matches:
            row = {
                'score': f"{match['score']:.2f}",
                'nome_operativo_miur': match['nome_operativo_miur'],
                'nome_esteso_miur': match['nome_esteso_miur'],
                'display_name_oa': match['display_name_oa'],
                'id_openalex': match['id_openalex'],
                'match_type': match['match_type']
            }
            csv_writer.writerow(row)
    
    # Statistiche finali
    print(f"\n🎉 ELABORAZIONE COMPLETATA!")
    print(f"📊 Statistiche:")
    print(f"   Istituzioni MIUR totali: {len(miur_stack)}")
    print(f"   Istituzioni OpenAlex totali: {len(oa_stack)}")
    print(f"   Match trovati: {len(final_matches)}")
    print(f"   Percentuale match: {len(final_matches)/len(miur_stack)*100:.1f}%")
    print(f"   OpenAlex utilizzate: {len(final_matches)}/{len(oa_stack)} ({len(final_matches)/len(oa_stack)*100:.1f}%)")
    print(f"   File output: {output_filename}")
    
    # Mostra primi risultati
    if final_matches:
        print(f"\n📋 Primi 5 match:")
        for i, match in enumerate(final_matches[:5]):
            print(f"   {i+1}. Score: {match['score']:.1f} | {match['nome_operativo_miur']} → {match['display_name_oa']}")

if __name__ == "__main__":
    main_greedy_institution_matcher()