from rapidfuzz.distance import JaroWinkler
import re
import csv
from rapidfuzz import fuzz
from rapidfuzz import fuzz
from rapidfuzz.fuzz import token_set_ratio
from tqdm import tqdm


# NOTA: Per versioni recenti di RapidFuzz (>= 2.0)
# JaroWinkler è stato spostato da rapidfuzz.fuzz a rapidfuzz.distance
# Se usi una versione più vecchia, usa: from rapidfuzz.fuzz import jaro_winkler

# Parsing 
def parse_professor_name(prof_tokens):
    """
    Parsing del nome professore dalla rubrica
    
    Regola: Il cognome è scritto in maiuscolo e può essere più token,
    il resto sono parte del nome.
    
    Args:
        prof_tokens (list): Lista di token del nome completo
    
    Returns:
        tuple: (nome, cognome)
    """

    if len(prof_tokens) < 2:
        return "", ""
    
    # Trova dove inizia il cognome (primi token in maiuscolo)
    cognome_tokens = []
    nome_tokens = []
        
    for token in prof_tokens:
        # Se il token è tutto maiuscolo, è parte del cognome
        if token.isupper():
            cognome_tokens.append(token)
        else:
            nome_tokens.append(token)
    
    prof_nome = " ".join(nome_tokens)
    prof_cognome = " ".join(cognome_tokens)
    
    # Fallback: se non troviamo pattern maiuscolo, 
    # considera primo token nome, resto cognome
    if not prof_cognome:
        prof_cognome = prof_tokens[0]
        prof_nome = " ".join(prof_tokens[1:])
        """
        with open("anomalie_prof.csv", 'a',  newline="") as f:
            csv_writer = csv.writer(f)
            csv_writer.writerow((prof_cognome, prof_nome))
        """
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

    #caso frequente
    if is_initial(author_tokens[0]) and len(author_tokens) == 2:
        return extract_initial(author_tokens[0]), author_tokens[1]
    elif len(author_tokens) == 2:
        return author_tokens[0], author_tokens[1]
    elif len(author_tokens) > 6:
        return "",""
    else: #mette tutto a nome, poi si farà scoring token set
        return "", author_tokens
    
def is_initial(name):
    """
    Controlla se una stringa è un'iniziale (es. 'G.', 'G', 'M.T.')
    
    Args:
        name (str): Stringa da verificare
        
    Returns:
        bool: True se è un'iniziale
    """
    import re
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
        'francesco': ['franco']
    }
    
    # Normalizza e converti in maiuscolo per confronto
    full_lower = normalize_name(full_name).lower()
    abbrev_lower = normalize_name(abbrev_name).lower()
    
    if full_lower in abbreviations:
        return abbrev_lower in abbreviations[full_lower]
    
    return False

def normalize_name(name):
    """Normalizza un nome rimuovendo caratteri speciali e spazi extra"""
    if not name:
        return ""
    
    # Rimuove caratteri speciali comuni
    name = re.sub(r'[‐‑–—]', '-', name)  # Normalizza trattini
    name = re.sub(r'[''`´]', "'", name)  # Normalizza apostrofi
    
    # Rimuove spazi multipli
    name = re.sub(r'\s+', ' ', name.strip())
    
    return name

# Calcolo punteggi
def calculate_name_score(prof_name, author_name):
    """
    Calcola lo score di matching tra nome professore e nome autore
    usando algoritmi ottimizzati per nomi accademici e le funzioni di parsing definite
    
    Args:
        prof_name (str): Nome del professore (es. "Giuseppe VERDI")
        author_name (str): Nome dell'autore (es. "G. Verdi")
    
    Returns:
        float: Score da 0 a 100
    """
    if not prof_name or not author_name:
        return 0.0
    
    # Normalizza i nomi
    prof_clean = normalize_name(prof_name)
    author_clean = normalize_name(author_name)
    
    # Tokenizza
    prof_tokens = prof_clean.split()
    author_tokens = author_clean.split()
    
    if len(prof_tokens) < 2 or len(author_tokens) < 2:
        # Fallback su Jaro-Winkler semplice se struttura non standard
        return JaroWinkler.similarity(prof_clean, author_clean) * 100
    
    # Usa le funzioni di parsing definite
    prof_nome, prof_cognome = parse_professor_name(prof_tokens)
    author_nome, author_cognome = parse_author_name(author_tokens)
    
    # Caso speciale: se author_cognome è una lista (parsing fallito)
    if isinstance(author_cognome, list):
        return calculate_token_set_score(prof_tokens, author_tokens)
    
    # Score cognome (più importante, deve essere molto simile)
    # Normalizza per confronto (maiuscolo e strip)
    prof_cognome_norm = normalize_name(prof_cognome).upper()
    author_cognome_norm = normalize_name(author_cognome).upper()
    cognome_score = JaroWinkler.similarity(prof_cognome_norm, author_cognome_norm) * 100
    
    # Score nome (gestisce iniziali e abbreviazioni)
    nome_score = calculate_first_name_score(prof_nome, author_nome)
    
    # Score combinato pesato (cognome 70%, nome 30%)
    final_score = (cognome_score * 0.7) + (nome_score * 0.3)
    
    # Bonus se match perfetto di entrambi
    if cognome_score > 95 and nome_score > 95:
        final_score = min(100, final_score + 5)
    
    # Penalità se cognome troppo diverso (anche se nome matcha)
    if cognome_score < 60:
        final_score *= 0.5
    
    return final_score

def calculate_token_set_score(prof_tokens, author_tokens):
    """
    Calcola score usando token set quando il parsing nome/cognome fallisce
    
    Args:
        prof_tokens (list): Token del nome professore
        author_tokens (list): Token del nome autore
    
    Returns:
        float: Score da 0 a 100
    """
    # Normalizza i token per il confronto (maiuscolo e solo alfabetici)
    prof_tokens_clean = [normalize_name(token).upper() for token in prof_tokens if token.isalpha()]
    author_tokens_clean = [normalize_name(token).upper() for token in author_tokens if token.isalpha()]
    
    # Usa token_set_ratio che gestisce meglio ordini diversi e token parziali
    token_set_score = fuzz.token_set_ratio(" ".join(prof_tokens_clean), " ".join(author_tokens_clean))
    
    # Controlla anche se ci sono iniziali che matchano
    initial_bonus = 0
    for prof_token in prof_tokens:
        for author_token in author_tokens:
            if is_initial(author_token):
                initial = extract_initial(author_token).upper()
                if normalize_name(prof_token).upper().startswith(initial):
                    initial_bonus += 10
            elif is_initial(prof_token):
                initial = extract_initial(prof_token).upper()
                if normalize_name(author_token).upper().startswith(initial):
                    initial_bonus += 10
    
    # Score finale con bonus per iniziali
    final_score = min(100, token_set_score + initial_bonus)
    
    return final_score

def calculate_first_name_score(prof_nome, author_first):
    """
    Calcola score specifico per il nome, gestendo iniziali e abbreviazioni
    """
    prof_nome = prof_nome.strip()
    author_first = author_first.strip()
    
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
    
    # Caso 3: Entrambi nomi completi - normalizza per confronto
    prof_nome_norm = normalize_name(prof_nome).upper()
    author_first_norm = normalize_name(author_first).upper()
    jaro_score = JaroWinkler.similarity(prof_nome_norm, author_first_norm) * 100
    
    # Bonus per abbreviazioni comuni
    if check_common_abbreviations(prof_nome, author_first):
        jaro_score = min(100, jaro_score + 10)
    
    return jaro_score


def find_best_matches(professors, authors_dict):
    """
    Trova i migliori match per tutti i professori
    
    Args:
        professors (list): Lista di dizionari professori
        authors_dict (dict): Dizionario autori {id: author_data}
    
    Returns:
        list: Lista di tuple (score, prof, author_id) ordinata per score
    """
    all_matches = []
    
    for prof in tqdm(professors, desc='Professors', leave=False):
        prof_name = prof.get('nome_completo', '')
        
        for author_id, author_data in authors_dict.items():
            # Score con display_name
            display_name = author_data.get('display_name', '')
            score = calculate_name_score(prof_name, display_name)
            
            if score > 0:
                all_matches.append((score, prof, author_id, 'display_name'))
            """
            # Score con alternatives
            alternatives = author_data.get('display_name_alternatives', [])
            for alt_name in alternatives:
                if alt_name:
                    alt_score = calculate_name_score(prof_name, alt_name)
                    if alt_score > score:  # Prendi solo se migliore del display_name
                        # Rimuovi il match precedente con score più basso
                        all_matches = [m for m in all_matches 
                                     if not (m[1] == prof and m[2] == author_id)]
                        all_matches.append((alt_score, prof, author_id, 'alternative'))
            """
    # Ordina per score decrescente
    all_matches.sort(reverse=True, key=lambda x: x[0])
    
    return all_matches

def resolve_matches(all_matches, authors_dict, threshold=75.0):
    """
    Risolve i match evitando conflitti (1-to-1 mapping)
    
    Args:
        all_matches (list): Lista di match ordinata per score
        authors_dict (dict): Dizionario autori per recuperare i dati
        threshold (float): Score minimo per considerare un match valido
    
    Returns:
        list: Lista di match finali con campi richiesti
    """
    final_matches = []
    used_professors = set()
    used_authors = set()
    
    for score, prof, author_id, match_type in all_matches:
        if score < threshold:
            break  # Tutti i successivi avranno score ancora più basso
        
        prof_id = prof.get('id')
        
        # Verifica se già usati
        if prof_id not in used_professors and author_id not in used_authors:
            # Recupera dati autore
            author_data = authors_dict[author_id]
            
            # Salva i campi richiesti
            match_result = {
                'score': score,
                'cognome_rubrica': prof.get('cognome'),  # cognome dalla rubrica
                'display_name': author_data.get('display_name'),  # display_name autore
                'author_id': author_id,  # id autore
                'orcid': author_data.get('orcid')  # orcid autore
            }
            
            final_matches.append(match_result)
            used_professors.add(prof_id)
            used_authors.add(author_id)
    
    return final_matches

# Esempio di utilizzo aggiornato:
# all_matches = find_best_matches(professors, authors_dict)
# final_matches = resolve_matches(all_matches, authors_dict, threshold=75.0)
# 
# print(f"Found {len(final_matches)} matches")
# for match in final_matches:
#     print(f"Score: {match['score']:.1f} | {match['cognome_rubrica']} -> {match['display_name']} | ID: {match['author_id']} | ORCID: {match['orcid']}")