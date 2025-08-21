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

def calculate_institution_score(miur_name, oa_name):
    """
    Calcola score di matching tra nome MIUR e nome OpenAlex
    """
    if not miur_name or not oa_name:
        return 0.0
    
    # Rimuove stop words e normalizza
    miur_clean = remove_stopwords_from_institution(miur_name).upper()
    oa_clean = remove_stopwords_from_institution(oa_name).upper()
    """
    # Se dopo aver rimosso stop words rimane poco, usa nomi originali
    if len(miur_clean.strip()) < 3 or len(oa_clean.strip()) < 3:
        miur_clean = normalize_institution_name(miur_name).upper()
        oa_clean = normalize_institution_name(oa_name).upper()
    """
    # Usa sia Jaro-Winkler che token_set per nomi di istituzioni
    jaro_score = JaroWinkler.similarity(miur_clean, oa_clean) * 100
    token_score = fuzz.token_set_ratio(miur_clean, oa_clean)
    
    # Prendi il punteggio migliore (le istituzioni hanno spesso ordini diversi)
    final_score = max(jaro_score, token_score)
    
    return final_score

def find_best_institution_matches(miur_data, oa_data, threshold=75.0):
    """
    Trova i migliori match tra istituzioni MIUR e OpenAlex con matching 1-to-1
    """
    all_matches = []
    
    for _, miur_row in tqdm(miur_data.iterrows(), total=len(miur_data), desc="Cercando match"):
        miur_name = miur_row['NomeOperativo']  # Usa NomeOperativo invece di NomeEsteso
        
        for _, oa_row in oa_data.iterrows():
            oa_id = oa_row['id']
            
            # Score con display_name
            display_name = oa_row.get('display_name', '')
            score = calculate_institution_score(miur_name, display_name)
            
            if score > 0:
                all_matches.append((score, miur_row, oa_row, 'display_name'))
            
            # Score con alternatives
            alternatives = oa_row.get('display_name_alternatives', [])
            if isinstance(alternatives, list):
                for alt_name in alternatives:
                    if alt_name:
                        alt_score = calculate_institution_score(miur_name, alt_name)
                        if alt_score > score:  # Prendi solo se migliore del display_name
                            # Rimuovi il match precedente con score più basso
                            all_matches = [m for m in all_matches 
                                         if not (m[1]['NomeOperativo'] == miur_name and m[2]['id'] == oa_id)]
                            all_matches.append((alt_score, miur_row, oa_row, 'alternative'))
    
    # Ordina per score decrescente
    all_matches.sort(reverse=True, key=lambda x: x[0])
    
    return all_matches

def resolve_institution_matches(all_matches, threshold=75.0):
    """
    Risolve i match evitando conflitti (1-to-1 mapping)
    """
    final_matches = []
    used_miur = set()
    used_openalex = set()
    
    for score, miur_row, oa_row, match_type in tqdm(all_matches, desc="Risolvendo match"):
        if score < threshold:
            break  # Tutti i successivi avranno score ancora più basso
        
        miur_name = miur_row['NomeOperativo']
        oa_id = oa_row['id']
        
        # Verifica se già usati
        if miur_name not in used_miur and oa_id not in used_openalex:
            match_result = {
                'score': score,
                'nome_operativo_miur': miur_name,
                'nome_esteso_miur': miur_row['NomeEsteso'],
                'display_name_oa': oa_row.get('display_name', ''),
                'id_openalex': oa_id,
                'match_type': match_type
            }
            
            final_matches.append(match_result)
            used_miur.add(miur_name)
            used_openalex.add(oa_id)
    
    return final_matches

def main_institution_matcher():
    """Funzione principale per il matching delle istituzioni"""
    
    print("🏛️  MATCHING ISTITUZIONI MIUR-OPENALEX")
    print("=" * 50)
    
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
    
    # Mostra esempi
    print("\n📋 Esempi nomi da matchare:")
    print("MIUR (primi 3 - NomeOperativo):")
    for i, name in enumerate(miur_data['NomeOperativo'].head(3)):
        print(f"   {i+1}. {name}")
    
    print("OpenAlex (primi 3):")
    for i, name in enumerate(istituzioni_oa['display_name'].head(3)):
        print(f"   {i+1}. {name}")
    
    # Mostra esempio di pulizia stop words
    if len(miur_data) > 0:
        example_name = miur_data['NomeOperativo'].iloc[0]
        cleaned_name = remove_stopwords_from_institution(example_name)
        print(f"\n🧹 Esempio pulizia stop words:")
        print(f"   Originale: '{example_name}'")
        print(f"   Pulito: '{cleaned_name}'")
    
    print(f"\n⚙️  Stop words rimosse: università, degli, studi, di, della, del, dello, delle, university, of, studies")
    
    # Esegui matching
    print(f"\n🔍 Avvio matching (soglia: 75.0)...")
    all_matches = find_best_institution_matches(miur_data, istituzioni_oa, threshold=75.0)
    print(f"   Match totali trovati: {len(all_matches)}")
    
    # Risolvi conflitti con mapping 1-to-1
    print("🎯 Risoluzione conflitti (1-to-1 mapping)...")
    final_matches = resolve_institution_matches(all_matches, threshold=75.0)
    
    # Salva risultati
    output_filename = f"institution_matches_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    
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
    print(f"   Istituzioni MIUR totali: {len(miur_data)}")
    print(f"   Istituzioni OpenAlex totali: {len(istituzioni_oa)}")
    print(f"   Match trovati: {len(final_matches)}")
    print(f"   Percentuale match: {len(final_matches)/len(miur_data)*100:.1f}%")
    print(f"   File output: {output_filename}")
    
    # Mostra primi risultati
    if final_matches:
        print(f"\n📋 Primi 5 match:")
        for i, match in enumerate(final_matches[:5]):
            print(f"   {i+1}. Score: {match['score']:.1f} | {match['nome_operativo_miur']} -> {match['display_name_oa']}")

if __name__ == "__main__":
    main_institution_matcher()