# debug_matching.py
from matching_functionsV3 import (
    calculate_name_score, 
    find_best_matches, 
    resolve_matches,
    parse_professor_name,
    parse_author_name
)

def debug_matching_process(professors, authors_dict, threshold=75.0):
    """
    Debug completo del processo di matching
    """
    print("=== DEBUG MATCHING PROCESS ===\n")
    
    # 1. Verifica input
    print(f"📊 DATI INPUT:")
    print(f"   Professori: {len(professors)}")
    print(f"   Autori: {len(authors_dict)}")
    print(f"   Soglia: {threshold}")
    print()
    
    # 2. Mostra primi esempi
    print("📝 ESEMPI DATI:")
    if professors:
        print("   Primo professore:")
        first_prof = professors[0]
        for key, value in first_prof.items():
            print(f"     {key}: '{value}'")
    
    if authors_dict:
        first_author_id = list(authors_dict.keys())[0]
        first_author = authors_dict[first_author_id]
        print(f"   Primo autore (ID: {first_author_id}):")
        for key, value in first_author.items():
            print(f"     {key}: '{value}'")
    print()
    
    # 3. Test parsing per primi esempi
    if professors:
        prof_name = professors[0].get('nome_completo', '')
        if prof_name:
            tokens = prof_name.split()
            nome, cognome = parse_professor_name(tokens)
            print(f"🔍 TEST PARSING PROFESSORE:")
            print(f"   Input: '{prof_name}'")
            print(f"   Tokens: {tokens}")
            print(f"   Nome: '{nome}', Cognome: '{cognome}'")
            print()
    
    if authors_dict:
        first_author = list(authors_dict.values())[0]
        display_name = first_author.get('display_name', '')
        if display_name:
            tokens = display_name.split()
            nome, cognome = parse_author_name(tokens)
            print(f"🔍 TEST PARSING AUTORE:")
            print(f"   Input: '{display_name}'")
            print(f"   Tokens: {tokens}")
            print(f"   Nome: '{nome}', Cognome: '{cognome}'")
            print()
    
    # 4. Test calcolo score per prima coppia
    if professors and authors_dict:
        prof_name = professors[0].get('nome_completo', '')
        first_author = list(authors_dict.values())[0]
        author_name = first_author.get('display_name', '')
        
        if prof_name and author_name:
            score = calculate_name_score(prof_name, author_name)
            print(f"💯 TEST SCORE:")
            print(f"   Professore: '{prof_name}'")
            print(f"   Autore: '{author_name}'")
            print(f"   Score: {score:.2f}")
            print()
    
    # 5. Trova tutti i match con soglia bassa per debug
    print("🔎 RICERCA MATCH (soglia debug: 0):")
    all_matches = find_best_matches(professors, authors_dict)
    
    print(f"   Match totali trovati: {len(all_matches)}")
    
    # Mostra primi 10 match
    print("   Primi 10 match:")
    for i, (score, prof, author_id, match_type) in enumerate(all_matches[:10]):
        prof_name = prof.get('nome_completo', 'N/A')
        author_data = authors_dict[author_id]
        author_name = author_data.get('display_name', 'N/A')
        print(f"     {i+1}. Score: {score:.2f} | '{prof_name}' -> '{author_name}' ({match_type})")
    
    if len(all_matches) > 10:
        print(f"     ... e altri {len(all_matches) - 10} match")
    print()
    
    # 6. Analizza distribuzione score
    if all_matches:
        scores = [match[0] for match in all_matches]
        print(f"📈 DISTRIBUZIONE SCORE:")
        print(f"   Score massimo: {max(scores):.2f}")
        print(f"   Score minimo: {min(scores):.2f}")
        print(f"   Score medio: {sum(scores)/len(scores):.2f}")
        
        # Conta match sopra varie soglie
        for thresh in [50, 60, 70, 75, 80, 90]:
            count = sum(1 for s in scores if s >= thresh)
            print(f"   Match >= {thresh}: {count}")
        print()
    
    # 7. Risolvi match con soglia originale
    final_matches = resolve_matches(all_matches, authors_dict, threshold)
    
    print(f"✅ MATCH FINALI (soglia {threshold}):")
    print(f"   Match risolti: {len(final_matches)}")
    
    for i, match in enumerate(final_matches):
        print(f"     {i+1}. Score: {match['score']:.2f} | {match['cognome_rubrica']} -> {match['display_name']}")
    
    return final_matches

# Test con dati di esempio
def test_with_sample_data():
    """Test con dati campione per verificare che le funzioni funzionino"""
    
    professors_sample = [
        {
            'id': 1,
            'nome_completo': 'Giuseppe VERDI',
            'cognome': 'VERDI'
        },
        {
            'id': 2, 
            'nome_completo': 'Maria ROSSI',
            'cognome': 'ROSSI'
        },
        {
            'id': 3,
            'nome_completo': 'Giuseppe ROSSI',
            'cognome': 'ROSSI'
        }
    ]
    
    authors_sample = {
        'A123': {
            'display_name': 'G. Verdi',
            'display_name_alternatives': ['Giuseppe Verdi'],
            'orcid': '0000-0000-0000-0001'
        },
        'A456': {
            'display_name': 'M. Rossi',
            'display_name_alternatives': [],
            'orcid': '0000-0000-0000-0002'
        },
        'A453': {
            'display_name': 'Giuseppe Rossi'
        }
    }
    
    print("🧪 TEST CON DATI CAMPIONE:")
    print("=" * 50)
    debug_matching_process(professors_sample, authors_sample, threshold=75.0)

if __name__ == "__main__":
    # Prima testa con dati campione
    test_with_sample_data()
    
    print("\n" + "=" * 80)
    print("AGGIUNGI I TUOI DATI REALI QUI SOTTO:")
    print("=" * 80)
    
    # Qui aggiungi i tuoi dati reali
    # your_professors = [...]  # I tuoi dati professori
    # your_authors = {...}     # I tuoi dati autori
    # debug_matching_process(your_professors, your_authors)