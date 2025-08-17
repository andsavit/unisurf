import re
import unicodedata
from collections import Counter

def analyze_names_in_dictionary(authors_dict):
    """
    Analizza i nomi nel dizionario degli autori per identificare pattern,
    lunghezza dei token, caratteri speciali, alfabeti non latini, etc.
    """
    
    analysis = {
        'total_authors': len(authors_dict),
        'display_names': {'tokens': [], 'chars': [], 'scripts': []},
        'alternatives': {'tokens': [], 'chars': [], 'scripts': []},
        'strange_chars': [],
        'non_latin_names': [],
        'empty_names': 0,
        'very_long_names': [],
        'stats': {}
    }
    
    # Pattern per identificare caratteri non-ASCII
    non_ascii_pattern = re.compile(r'[^\x00-\x7F]')
    
    for author_id, author_data in authors_dict.items():
        
        # Analizza display_name
        display_name = author_data.get('display_name', '')
        if display_name:
            tokens = display_name.split()
            analysis['display_names']['tokens'].append(len(tokens))
            analysis['display_names']['chars'].append(len(display_name))
            
            # Identifica script/alfabeto
            script = identify_script(display_name)
            analysis['display_names']['scripts'].append(script)
            
            # Caratteri strani
            if non_ascii_pattern.search(display_name):
                analysis['strange_chars'].append({
                    'author_id': author_id,
                    'name': display_name,
                    'type': 'display_name'
                })
            
            # Nomi non latini
            if script != 'Latin':
                analysis['non_latin_names'].append({
                    'author_id': author_id,
                    'name': display_name,
                    'script': script,
                    'type': 'display_name'
                })
            
            # Nomi molto lunghi (>50 caratteri)
            if len(display_name) > 50:
                analysis['very_long_names'].append({
                    'author_id': author_id,
                    'name': display_name,
                    'length': len(display_name),
                    'type': 'display_name'
                })
        else:
            analysis['empty_names'] += 1
        
        # Analizza display_name_alternatives
        alternatives = author_data.get('display_name_alternatives', [])
        for alt_name in alternatives:
            if alt_name:
                tokens = alt_name.split()
                analysis['alternatives']['tokens'].append(len(tokens))
                analysis['alternatives']['chars'].append(len(alt_name))
                
                script = identify_script(alt_name)
                analysis['alternatives']['scripts'].append(script)
                
                if non_ascii_pattern.search(alt_name):
                    analysis['strange_chars'].append({
                        'author_id': author_id,
                        'name': alt_name,
                        'type': 'alternative'
                    })
                
                if script != 'Latin':
                    analysis['non_latin_names'].append({
                        'author_id': author_id,
                        'name': alt_name,
                        'script': script,
                        'type': 'alternative'
                    })
                
                if len(alt_name) > 50:
                    analysis['very_long_names'].append({
                        'author_id': author_id,
                        'name': alt_name,
                        'length': len(alt_name),
                        'type': 'alternative'
                    })
    
    # Calcola statistiche
    analysis['stats'] = calculate_stats(analysis)
    
    return analysis

def identify_script(text):
    """Identifica il sistema di scrittura predominante nel testo"""
    if not text:
        return 'Unknown'
    
    script_counts = Counter()
    
    for char in text:
        if char.isalpha():
            try:
                script = unicodedata.name(char).split()[0]
                script_counts[script] += 1
            except ValueError:
                script_counts['Unknown'] += 1
    
    if not script_counts:
        return 'No_Letters'
    
    most_common_script = script_counts.most_common(1)[0][0]
    
    # Raggruppa script simili
    if most_common_script in ['LATIN', 'LATIN-1']:
        return 'Latin'
    elif most_common_script in ['CYRILLIC']:
        return 'Cyrillic'
    elif most_common_script in ['GREEK']:
        return 'Greek'
    elif most_common_script in ['ARABIC']:
        return 'Arabic'
    elif most_common_script in ['CJK']:
        return 'Chinese/Japanese/Korean'
    else:
        return most_common_script

def calculate_stats(analysis):
    """Calcola statistiche riassuntive"""
    stats = {}
    
    # Statistiche sui token
    if analysis['display_names']['tokens']:
        stats['display_name_tokens'] = {
            'min': min(analysis['display_names']['tokens']),
            'max': max(analysis['display_names']['tokens']),
            'avg': sum(analysis['display_names']['tokens']) / len(analysis['display_names']['tokens']),
            'distribution': Counter(analysis['display_names']['tokens'])
        }
    
    if analysis['alternatives']['tokens']:
        stats['alternatives_tokens'] = {
            'min': min(analysis['alternatives']['tokens']),
            'max': max(analysis['alternatives']['tokens']),
            'avg': sum(analysis['alternatives']['tokens']) / len(analysis['alternatives']['tokens']),
            'distribution': Counter(analysis['alternatives']['tokens'])
        }
    
    # Statistiche sui caratteri
    if analysis['display_names']['chars']:
        stats['display_name_chars'] = {
            'min': min(analysis['display_names']['chars']),
            'max': max(analysis['display_names']['chars']),
            'avg': sum(analysis['display_names']['chars']) / len(analysis['display_names']['chars'])
        }
    
    # Distribuzione degli script
    stats['script_distribution'] = {
        'display_names': Counter(analysis['display_names']['scripts']),
        'alternatives': Counter(analysis['alternatives']['scripts'])
    }
    
    return stats

def print_analysis_report(analysis):
    """Stampa un report leggibile dell'analisi"""
    
    print("="*60)
    print("ANALISI NOMI AUTORI")
    print("="*60)
    
    print(f"\n📊 STATISTICHE GENERALI:")
    print(f"   Totale autori: {analysis['total_authors']}")
    print(f"   Nomi vuoti: {analysis['empty_names']}")
    print(f"   Nomi con caratteri non-ASCII: {len(analysis['strange_chars'])}")
    print(f"   Nomi non-latini: {len(analysis['non_latin_names'])}")
    print(f"   Nomi molto lunghi (>50 char): {len(analysis['very_long_names'])}")
    
    stats = analysis['stats']
    
    # Statistiche token display_names
    if 'display_name_tokens' in stats:
        token_stats = stats['display_name_tokens']
        print(f"\n🏷️  TOKEN DISPLAY_NAMES:")
        print(f"   Min token: {token_stats['min']}")
        print(f"   Max token: {token_stats['max']}")
        print(f"   Media token: {token_stats['avg']:.1f}")
        print(f"   Distribuzione più comune:")
        for tokens, count in token_stats['distribution'].most_common(5):
            print(f"      {tokens} token: {count} nomi")
    
    # Distribuzione script
    print(f"\n🌍 DISTRIBUZIONE ALFABETI:")
    print("   Display names:")
    for script, count in stats['script_distribution']['display_names'].most_common():
        print(f"      {script}: {count}")
    
    # Esempi nomi strani
    if analysis['non_latin_names']:
        print(f"\n🔤 ESEMPI NOMI NON-LATINI:")
        for i, item in enumerate(analysis['non_latin_names'][:5]):
            print(f"   {i+1}. {item['name']} (Script: {item['script']})")
    
    # Esempi nomi lunghi
    if analysis['very_long_names']:
        print(f"\n📏 ESEMPI NOMI MOLTO LUNGHI:")
        for i, item in enumerate(analysis['very_long_names'][:5]):
            print(f"   {i+1}. {item['name']} ({item['length']} caratteri)")

# Esempio di utilizzo:
#analysis = analyze_names_in_dictionary(oa_authors)
#print_analysis_report(analysis)