# main_script.py
from matching_functionsV2 import (
    calculate_name_score, 
    find_best_matches, 
    resolve_matches,
    parse_professor_name,
    parse_author_name
)

# Esempio di utilizzo
def main():
    # Dati di esempio
    professors = [
        {
            'id': 1,
            'nome_completo': 'Giuseppe VERDI',
            'cognome': 'VERDI'
        },
        {
            'id': 2, 
            'nome_completo': 'Maria ROSSI',
            'cognome': 'ROSSI'
        }
    ]
    
    authors_dict = {
        'A123': {
            'display_name': 'G. Verdi',
            'display_name_alternatives': ['Giuseppe Verdi'],
            'orcid': '0000-0000-0000-0001'
        },
        'A456': {
            'display_name': 'M. Rossi',
            'display_name_alternatives': [],
            'orcid': '0000-0000-0000-0002'
        }
    }
    
    # Trova i match
    all_matches = find_best_matches(professors, authors_dict)
    final_matches = resolve_matches(all_matches, authors_dict, threshold=75.0)
    
    # Stampa risultati
    print(f"Trovati {len(final_matches)} match:")
    for match in final_matches:
        print(f"Score: {match['score']:.1f} | {match['cognome_rubrica']} -> {match['display_name']} | ORCID: {match['orcid']}")

if __name__ == "__main__":
    main()