from rapidfuzz.distance import JaroWinkler
import re
import csv
from rapidfuzz import fuzz
from rapidfuzz.fuzz import token_set_ratio
from tqdm import tqdm
import unicodedata
import logging

logger = logging.getLogger(__name__)

# SETUP VARIABILI
SCORE_APPEND_THRESHOLD = 40  # score minimo per essere aggiunti alla lista di score
THRESHOLD_RESOLVE = 75.0     # Abbassata da 88.0

# ========== PREPROCESSING FUNCTIONS ==========

def normalize_name(name):
    """
    Normalizza un nome rimuovendo caratteri speciali, accenti e spazi extra
    ATTENZIONE: Non modifica maiuscole/minuscole per preservare la logica di parsing
    """
    if not name:
        return ""
    
    # Normalizza caratteri Unicode (rimuove accenti)
    name = unicodedata.normalize('NFD', name)
    name = ''.join(char for char in name if unicodedata.category(char) != 'Mn')
    
    # Rimuove caratteri speciali comuni
    name = re.sub(r'[‐‑–—]', '-', name)  # Normalizza trattini
    name = re.sub(r'[''`´]', "'", name)  # Normalizza apostrofi
    name = re.sub(r'[,;]', ' ', name)    # Sostituisce virgole/punti e virgola con spazi
    
    # Rimuove spazi multipli ma PRESERVA maiuscole/minuscole
    name = re.sub(r'\s+', ' ', name.strip())
    
    return name

def preprocess_professor_data(professors):
    """
    Preprocessa i dati dei professori aggiungendo campi normalizzati e tokenizzati
    
    Args:
        professors (list): Lista di dizionari professori (modificata in-place)
    """
    logger.debug(f"Preprocessing {len(professors)} professors")
    
    for prof in professors:
        nome_completo = prof.get('nome_completo', '')
        if nome_completo:
            # Normalizza ma PRESERVA il nome originale per il parsing
            normalized = normalize_name(nome_completo)
            
            prof['nome_completo_normalized'] = normalized
            prof['nome_completo_tokens'] = normalized.split()
            prof['nome_completo_original'] = nome_completo  # Mantieni originale per parsing
        else:
            prof['nome_completo_normalized'] = ''
            prof['nome_completo_tokens'] = []
            prof['nome_completo_original'] = ''
    
    logger.debug(f"Professor preprocessing completed")

def preprocess_authors_data(authors_dict):
    """
    Preprocessa i dati degli autori aggiungendo campi normalizzati e tokenizzati
    
    Args:
        authors_dict (dict): Dizionario autori {id: author_data} (modificato in-place)
    """
    logger.debug(f"Preprocessing {len(authors_dict)} authors")
    
    for author_id, author_data in authors_dict.items():
        display_name = author_data.get('display_name', '')
        if display_name:
            normalized = normalize_name(display_name)
            
            author_data['display_name_normalized'] = normalized
            author_data['display_name_tokens'] = normalized.split()
        else:
            author_data['display_name_normalized'] = ''
            author_data['display_name_tokens'] = []
    
    logger.debug(f"Author preprocessing completed")

def cleanup_preprocessed_data(professors, authors_dict):
    """
    Rimuove i campi di preprocessing per liberare memoria
    
    Args:
        professors (list): Lista professori
        authors_dict (dict): Dizionario autori
    """
    logger.debug("Cleaning up preprocessed data")
    
    # Cleanup professori
    for prof in professors:
        prof.pop('nome_completo_normalized', None)
        prof.pop('nome_completo_tokens', None)
        prof.pop('nome_completo_original', None)
    
    # Cleanup autori
    for author_data in authors_dict.values():
        author_data.pop('display_name_normalized', None)
        author_data.pop('display_name_tokens', None)

# ========== PARSING FUNCTIONS ==========

def parse_professor_name(prof_tokens, original_name):
    """
    Parsing del nome professore dalla rubrica usando il nome ORIGINALE
    
    Regola: Il cognome è scritto in maiuscolo e può essere più token,
    il resto sono parte del nome.
    
    Args:
        prof_tokens (list): Lista di token del nome normalizzato (per backup)
        original_name (str): Nome originale non normalizzato (per logica maiuscolo)
    
    Returns:
        tuple: (nome, cognome)
    """
    # Usa il nome originale per preservare maiuscole
    original_tokens = original_name.split() if original_name else prof_tokens
    
    if len(original_tokens) < 2:
        return "", ""
    
    # Trova dove inizia il cognome (primi token in maiuscolo)
    cognome_tokens = []
    nome_tokens = []
        
    for token in original_tokens:
        # Se il token è tutto maiuscolo, è parte del cognome
        if token.isupper():
            cognome_tokens.append(token)
        else:
            nome_tokens.append(token)
    
    prof_nome = " ".join(nome_tokens)
    prof_cognome = " ".join(cognome_tokens)
    
    # Fallback: se non troviamo pattern maiuscolo, 
    # considera primo token cognome, resto nome
    if not prof_cognome:
        prof_cognome = original_tokens[0]
        prof_nome = " ".join(original_tokens[1:])
        
        logger.debug(f"Fallback parsing for: {original_name} -> {prof_cognome}, {prof_nome}")
    
    return prof_nome, prof_cognome

def parse_author_name(author_tokens):
    """
    Parsing del nome autore da OpenAlex
    
    Regole:
    - Caso più comune: "N. Cognome" -> primo token è iniziale, resto è cognome
    - Caso alternativo: "Nome Cognome" -> primo token nome, resto cognome
    - Se più token e primo è iniziale: iniziale + cognome
    - Se più token e primo non è iniziale: primo nome, resto cognome
    
    Args:
        author_tokens (list): Lista di token del nome dell'autore
    
    Returns:
        tuple: (nome_o_iniziale, cognome) oppure ("", lista_tokens) se non parsabile
    """
    if not author_tokens:
        return "", ""
    
    # Caso frequente
    if len(author_tokens) == 2 and is_initial(author_tokens[0]):
        return extract_initial(author_tokens[0]), author_tokens[1]
    elif len(author_tokens) == 2:  # caso Nome Cognome
        return author_tokens[0], author_tokens[1]
    elif len(author_tokens) > 6:
        return "", ""
    else:  # mette tutto a nome, poi si farà scoring token set
        return "", author_tokens
    
def is_initial(name):
    """
    Controlla se una stringa è un'iniziale (es. 'G.', 'G', 'M.T.')
    
    Args:
        name (str): Stringa da verificare
        
    Returns:
        bool: True se è un'iniziale
    """
    name = name.strip()
    # Pattern: una o più lettere seguite opzionalmente da punti
    pattern = r'^[A-Za-z]\.?([A-Za-z]\.?)*$'
    return bool(re.match(pattern, name)) and len(name.replace('.', '')) <= 3

def extract_initial(name):
    """Estrae la prima lettera da un'iniziale"""
    return re.sub(r'[^A-Za-z]', '', name)[0] if name else ''

def check_common_abbreviations(full_name, abbrev_name):
    """Controlla abbreviazioni comuni italiane"""
    abbreviations = {
        'giuseppe': ['peppe', 'beppe', 'pino'],
        'giovanni': ['gianni', 'nino'],
        'francesco': ['franco', 'checco'],
        'alessandro': ['sandro', 'alex'],
        'antonio': ['toni', 'tonino'],
        'maria': ['mary'],
    }
    
    # Normalizza e converti in maiuscolo per confronto
    full_lower = full_name.lower() if full_name else ''
    abbrev_lower = abbrev_name.lower() if abbrev_name else ''
    
    if full_lower in abbreviations:
        return abbrev_lower in abbreviations[full_lower]
    
    return False

# ========== SCORING FUNCTIONS (OTTIMIZZATE) ==========

def calculate_name_score_optimized(prof_data, author_data):
    """
    Versione ottimizzata di calculate_name_score che usa dati preprocessati
    
    Args:
        prof_data (dict): Dati professore con campi preprocessati
        author_data (dict): Dati autore con campi preprocessati
    
    Returns:
        float: Score da 0 a 100
    """
    # Usa dati già normalizzati (nessuna chiamata a normalize_name!)
    prof_clean = prof_data.get('nome_completo_normalized', '')
    author_clean = author_data.get('display_name_normalized', '')
    
    if not prof_clean or not author_clean:
        return 0.0
    
    # Usa token già calcolati
    prof_tokens = prof_data.get('nome_completo_tokens', [])
    author_tokens = author_data.get('display_name_tokens', [])
    
    if len(prof_tokens) < 2 or len(author_tokens) < 2:
        # Fallback su Jaro-Winkler semplice se struttura non standard
        return JaroWinkler.similarity(prof_clean, author_clean) * 100
    
    # Usa le funzioni di parsing (ora con nome originale preservato)
    original_name = prof_data.get('nome_completo_original', prof_clean)
    prof_nome, prof_cognome = parse_professor_name(prof_tokens, original_name)
    author_nome, author_cognome = parse_author_name(author_tokens)
    
    # Caso speciale: se author_cognome è una lista (parsing fallito)
    if isinstance(author_cognome, list):
        return calculate_token_set_score_optimized(prof_tokens, author_tokens)
    
    # Score cognome (più importante, deve essere molto simile)
    # Ora usa già dati normalizzati!
    prof_cognome_norm = prof_cognome.upper() if prof_cognome else ''
    author_cognome_norm = author_cognome.upper() if author_cognome else ''
    cognome_score = JaroWinkler.similarity(prof_cognome_norm, author_cognome_norm) * 100
    
    # Score nome (gestisce iniziali e abbreviazioni)
    nome_score = calculate_first_name_score_optimized(prof_nome, author_nome)
    
    # Score combinato pesato (cognome 70%, nome 30%)
    final_score = (cognome_score * 0.7) + (nome_score * 0.3)
    
    # Bonus se match perfetto di entrambi
    if cognome_score > 95 and nome_score > 95:
        final_score = min(100, final_score + 5)
    
    # Penalità se cognome troppo diverso (anche se nome matcha)
    if cognome_score < 60:
        final_score *= 0.5
    
    return final_score

def calculate_token_set_score_optimized(prof_tokens, author_tokens):
    """
    Versione ottimizzata che usa token già normalizzati
    """
    # Token già normalizzati e tokenizzati!
    prof_tokens_clean = [token.upper() for token in prof_tokens if token.isalpha()]
    author_tokens_clean = [token.upper() for token in author_tokens if token.isalpha()]
    
    # Usa token_set_ratio che gestisce meglio ordini diversi e token parziali
    token_set_score = fuzz.token_set_ratio(" ".join(prof_tokens_clean), " ".join(author_tokens_clean))
    
    # Controlla anche se ci sono iniziali che matchano
    initial_bonus = 0
    for prof_token in prof_tokens:
        for author_token in author_tokens:
            if is_initial(author_token):
                initial = extract_initial(author_token).upper()
                if prof_token.upper().startswith(initial):
                    initial_bonus += 10
            elif is_initial(prof_token):
                initial = extract_initial(prof_token).upper()
                if author_token.upper().startswith(initial):
                    initial_bonus += 10
    
    # Score finale con bonus per iniziali
    final_score = min(100, token_set_score + initial_bonus)
    
    return final_score

def calculate_first_name_score_optimized(prof_nome, author_first):
    """
    Versione ottimizzata di calculate_first_name_score (già normalizzati)
    """
    prof_nome = prof_nome.strip() if prof_nome else ''
    author_first = author_first.strip() if author_first else ''
    
    # Caso 1: Iniziale nel nome autore (es. "Giuseppe" vs "G." o "G")
    if is_initial(author_first):
        initial = extract_initial(author_first)
        if prof_nome and prof_nome[0].upper() == initial.upper():
            return 95.0  # Score alto per match iniziale
        else:
            return 10.0  # Score basso se iniziale non matcha
    
    # Caso 2: Iniziale nel nome professore (meno comune)
    if is_initial(prof_nome):
        initial = extract_initial(prof_nome)
        if author_first and author_first[0].upper() == initial.upper():
            return 95.0
        else:
            return 10.0
    
    # Caso 3: Entrambi nomi completi - già normalizzati!
    prof_nome_norm = prof_nome.upper()
    author_first_norm = author_first.upper()
    jaro_score = JaroWinkler.similarity(prof_nome_norm, author_first_norm) * 100
    
    # Bonus per abbreviazioni comuni - riabilitato dato che normalize_name è più veloce
    if check_common_abbreviations(prof_nome, author_first):
        jaro_score = min(100, jaro_score + 10)
    
    return jaro_score

# ========== MATCHING FUNCTIONS (OTTIMIZZATE) ==========

def find_best_matches_optimized(professors, authors_dict, score_threshold):
    """
    Versione ottimizzata di find_best_matches che usa dati preprocessati
    
    Args:
        professors (list): Lista di dizionari professori (con dati preprocessati)
        authors_dict (dict): Dizionario autori {id: author_data} (con dati preprocessati)
        score_threshold (float): Soglia minima per considerare un match
    
    Returns:
        list: Lista di tuple (score, prof, author_id, match_type) ordinata per score
    """
    logger.info(f"Finding matches between {len(professors)} professors and {len(authors_dict)} authors")
    logger.debug(f"Score threshold: {score_threshold}")
    
    all_matches = []
    
    for prof in tqdm(professors, desc='Professors', leave=False):
        prof_name = prof.get('nome_completo', '')
        
        for author_id, author_data in authors_dict.items():
            # Score con display_name usando funzione ottimizzata
            score = calculate_name_score_optimized(prof, author_data)
            
            if score > score_threshold:
                all_matches.append((score, prof, author_id, 'display_name'))
            
            # Nota: alternatives disabilitate per performance
            # Se necessario, possono essere riabilitate qui
    
    # Ordina per score decrescente
    all_matches.sort(reverse=True, key=lambda x: x[0])
    
    logger.info(f"Found {len(all_matches)} candidate matches above threshold {score_threshold}")
    
    if all_matches:
        scores = [match[0] for match in all_matches]
        logger.debug(f"Score range in candidates: {min(scores):.1f} - {max(scores):.1f}")
    
    return all_matches

def resolve_matches(all_matches, authors_dict, threshold_resolve):
    """
    Risolve i match evitando conflitti (1-to-1 mapping)
    
    Args:
        all_matches (list): Lista di match ordinata per score
        authors_dict (dict): Dizionario autori per recuperare i dati
        threshold_resolve (float): Score minimo per considerare un match valido
    
    Returns:
        list: Lista di match finali con campi richiesti
    """
    logger.debug(f"Resolving matches with threshold {threshold_resolve}")
    
    final_matches = []
    used_professors = set()
    used_authors = set()
    
    for score, prof, author_id, match_type in all_matches:
        if score < threshold_resolve:
            break  # Tutti i successivi avranno score ancora più basso
        
        prof_id = prof.get('id')
        
        # Verifica se già usati
        if prof_id not in used_professors and author_id not in used_authors:
            # Recupera dati autore
            author_data = authors_dict[author_id]
            
            # Salva i campi richiesti
            match_result = {
                'score': score,
                'nome_completo_rubrica': prof.get('nome_completo'),
                'display_name': author_data.get('display_name'),
                'author_id': author_id,
                'orcid': author_data.get('orcid')
            }
            
            final_matches.append(match_result)
            used_professors.add(prof_id)
            used_authors.add(author_id)
    
    logger.debug(f"Resolved to {len(final_matches)} final matches")
    return final_matches

# ========== UTILITY FUNCTIONS ==========

def test_matching_performance(professors, authors_dict, num_samples=10):
    """
    Funzione di test per verificare le performance del matching
    
    Args:
        professors (list): Lista professori
        authors_dict (dict): Dizionario autori
        num_samples (int): Numero di campioni da testare
    """
    import time
    
    logger.info(f"Testing matching performance with {num_samples} samples")
    
    # Test preprocessing
    start_time = time.time()
    
    # Simula preprocessing
    test_profs = professors[:num_samples] if len(professors) >= num_samples else professors
    test_authors = dict(list(authors_dict.items())[:num_samples]) if len(authors_dict) >= num_samples else authors_dict
    
    preprocess_professor_data(test_profs)
    preprocess_authors_data(test_authors)
    
    preprocessing_time = time.time() - start_time
    
    # Test matching
    start_time = time.time()
    
    all_matches = find_best_matches_optimized(test_profs, test_authors, SCORE_APPEND_THRESHOLD)
    final_matches = resolve_matches(all_matches, test_authors, THRESHOLD_RESOLVE)
    
    matching_time = time.time() - start_time
    
    logger.info(f"Performance test results:")
    logger.info(f"  Preprocessing time: {preprocessing_time:.3f}s")
    logger.info(f"  Matching time: {matching_time:.3f}s")
    logger.info(f"  Total time: {preprocessing_time + matching_time:.3f}s")
    logger.info(f"  Matches found: {len(final_matches)}")
    
    # Cleanup
    cleanup_preprocessed_data(test_profs, test_authors)
    
    return preprocessing_time, matching_time, len(final_matches)

# ========== DEBUG FUNCTIONS ==========

def debug_name_parsing(name, is_professor=True):
    """
    Funzione di debug per verificare il parsing dei nomi
    
    Args:
        name (str): Nome da parsare
        is_professor (bool): True se è un professore, False se è un autore
    """
    normalized = normalize_name(name)
    tokens = normalized.split()
    
    print(f"\n=== DEBUG PARSING: {name} ===")
    print(f"Originale: '{name}'")
    print(f"Normalizzato: '{normalized}'")
    print(f"Token: {tokens}")
    
    if is_professor:
        nome, cognome = parse_professor_name(tokens, name)
        print(f"Parsing professore -> Nome: '{nome}', Cognome: '{cognome}'")
    else:
        nome, cognome = parse_author_name(tokens)
        print(f"Parsing autore -> Nome: '{nome}', Cognome: '{cognome}'")
    
    return normalized, tokens

def debug_name_score(prof_name, author_name):
    """
    Funzione di debug per verificare il calcolo dello score
    """
    print(f"\n=== DEBUG SCORE ===")
    print(f"Professore: '{prof_name}'")
    print(f"Autore: '{author_name}'")
    
    # Simula preprocessing
    prof_data = {
        'nome_completo': prof_name,
        'nome_completo_normalized': normalize_name(prof_name),
        'nome_completo_original': prof_name
    }
    prof_data['nome_completo_tokens'] = prof_data['nome_completo_normalized'].split()
    
    author_data = {
        'display_name': author_name,
        'display_name_normalized': normalize_name(author_name)
    }
    author_data['display_name_tokens'] = author_data['display_name_normalized'].split()
    
    score = calculate_name_score_optimized(prof_data, author_data)
    print(f"Score finale: {score:.2f}")
    
    return score

# Esempio di utilizzo per testing:
# if __name__ == "__main__":
#     # Test parsing
#     debug_name_parsing("Giuseppe VERDI", is_professor=True)
#     debug_name_parsing("G. Verdi", is_professor=False)
#     
#     # Test score
#     debug_name_score("Giuseppe VERDI", "G. Verdi")