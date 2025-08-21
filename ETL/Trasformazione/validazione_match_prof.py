import pandas as pd
import random
from datetime import datetime

def create_validation_sample(input_csv_path, output_csv_path=None):
    """
    Crea un campione stratificato per validazione manuale dei match
    
    Args:
        input_csv_path (str): Percorso del file CSV con i risultati del matching
        output_csv_path (str): Percorso del file di output (opzionale)
    
    Returns:
        str: Nome del file di output creato
    """
    
    # Leggi i risultati del matching
    print("📂 Caricamento risultati matching...")
    df = pd.read_csv(input_csv_path)
    
    print(f"📊 Dataset totale: {len(df)} match trovati")
    print(f"Score range: {df['score'].min():.1f} - {df['score'].max():.1f}")
    
    # Converti score a numerico se necessario
    df['score'] = pd.to_numeric(df['score'], errors='coerce')
    
    # Distribuzione attuale degli score
    print("\n📈 Distribuzione score attuali:")
    print(f"Score = 100: {len(df[df['score'] == 100])} match")
    print(f"Score 95-99: {len(df[(df['score'] >= 95) & (df['score'] < 100)])} match")
    print(f"Score 85-94: {len(df[(df['score'] >= 85) & (df['score'] < 95)])} match")
    print(f"Score 75-84: {len(df[(df['score'] >= 75) & (df['score'] < 85)])} match")
    
    # Definisci le fasce di campionamento
    sampling_config = [
        {"min_score": 100, "max_score": 100, "samples": 30, "label": "Score 100 (SOSPETTI)"},
        {"min_score": 95, "max_score": 99.99, "samples": 20, "label": "Score 95-99"},
        {"min_score": 85, "max_score": 94.99, "samples": 15, "label": "Score 85-94"},
        {"min_score": 75, "max_score": 84.99, "samples": 10, "label": "Score 75-84"}
    ]
    
    validation_samples = []
    
    print("\n🎯 Estrazione campioni per validazione:")
    
    for config in sampling_config:
        # Filtra per fascia di score
        mask = (df['score'] >= config['min_score']) & (df['score'] <= config['max_score'])
        subset = df[mask].copy()
        
        available = len(subset)
        requested = config['samples']
        actual_samples = min(available, requested)
        
        print(f"  {config['label']}: {actual_samples}/{requested} campioni (disponibili: {available})")
        
        if actual_samples > 0:
            # Campionamento random stratificato
            if actual_samples < available:
                sampled = subset.sample(n=actual_samples, random_state=42)
            else:
                sampled = subset
            
            # Aggiungi info sulla fascia
            sampled = sampled.copy()
            sampled['score_band'] = config['label']
            validation_samples.append(sampled)
    
    # Combina tutti i campioni
    if not validation_samples:
        print("❌ Nessun campione estratto!")
        return None
    
    validation_df = pd.concat(validation_samples, ignore_index=True)
    
    # Mescola l'ordine per evitare bias durante la validazione
    validation_df = validation_df.sample(frac=1, random_state=42).reset_index(drop=True)
    
    # Crea CSV leggero per validazione manuale
    validation_csv = pd.DataFrame({
        'id': range(1, len(validation_df) + 1),
        'score': validation_df['score'].round(1),
        'prof_name': validation_df['nome_completo_rubrica'],
        'author_name': validation_df['display_name_openalex'],
        'score_band': validation_df['score_band'],
        'result': ''  # Colonna vuota da compilare manualmente
    })
    
    # Nome file di output
    if output_csv_path is None:
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        output_csv_path = f"validation_sample_{timestamp}.csv"
    
    # Salva CSV
    validation_csv.to_csv(output_csv_path, index=False, encoding='utf-8')
    
    print(f"\n✅ Campione di validazione creato: {output_csv_path}")
    print(f"📝 Totale campioni da validare: {len(validation_csv)}")
    print("\n📋 ISTRUZIONI PER LA VALIDAZIONE:")
    print("1. Apri il file CSV in Excel/LibreOffice")
    print("2. Per ogni riga, confronta 'prof_name' con 'author_name'")
    print("3. Nella colonna 'result' scrivi:")
    print("   - C = Correct (stesso professore)")
    print("   - W = Wrong (professori diversi)")
    print("   - U = Uncertain (non sicuro)")
    print("4. Salva il file quando completato")
    
    return output_csv_path

def analyze_validation_results(validation_csv_path):
    """
    Analizza i risultati della validazione manuale
    
    Args:
        validation_csv_path (str): Percorso del CSV compilato manualmente
    """
    
    print("📊 Analizzando risultati validazione...")
    df = pd.read_csv(validation_csv_path, sep=';')
    
    # Filtra solo righe compilate
    completed = df[df['result'].notna() & (df['result'] != '')].copy()
    
    if len(completed) == 0:
        print("❌ Nessun risultato di validazione trovato!")
        return
    
    print(f"✅ Validazioni completate: {len(completed)}/{len(df)}")
    
    # Analisi generale
    total_correct = len(completed[completed['result'].str.upper() == 'C'])
    total_wrong = len(completed[completed['result'].str.upper() == 'W'])
    total_uncertain = len(completed[completed['result'].str.upper() == 'U'])
    
    accuracy = (total_correct / len(completed)) * 100 if len(completed) > 0 else 0
    
    print(f"\n📈 RISULTATI GENERALI:")
    print(f"✅ Correct: {total_correct} ({total_correct/len(completed)*100:.1f}%)")
    print(f"❌ Wrong: {total_wrong} ({total_wrong/len(completed)*100:.1f}%)")
    print(f"❓ Uncertain: {total_uncertain} ({total_uncertain/len(completed)*100:.1f}%)")
    print(f"🎯 Accuratezza stimata: {accuracy:.1f}%")
    
    # Analisi per fascia di score
    print(f"\n📊 ACCURATEZZA PER FASCIA DI SCORE:")
    
    for band in completed['score_band'].unique():
        band_data = completed[completed['score_band'] == band]
        band_correct = len(band_data[band_data['result'].str.upper() == 'C'])
        band_total = len(band_data)
        band_accuracy = (band_correct / band_total) * 100 if band_total > 0 else 0
        
        print(f"  {band}: {band_correct}/{band_total} = {band_accuracy:.1f}% accuratezza")
    
    # Raccomandazioni threshold
    print(f"\n💡 RACCOMANDAZIONI:")
    
    # Calcola accuratezza per score >= soglia
    for threshold in [100, 95, 90, 85, 80]:
        high_score = completed[completed['score'] >= threshold]
        if len(high_score) > 0:
            high_correct = len(high_score[high_score['result'].str.upper() == 'C'])
            high_accuracy = (high_correct / len(high_score)) * 100
            print(f"  Score >= {threshold}: {high_accuracy:.1f}% accuratezza ({high_correct}/{len(high_score)} campioni)")
    
    return {
        'total_samples': len(completed),
        'accuracy': accuracy,
        'correct': total_correct,
        'wrong': total_wrong,
        'uncertain': total_uncertain
    }

# ESEMPIO DI UTILIZZO:

if __name__ == "__main__":
    
    # FASE 1: Crea campione per validazione
    input_file = "/Users/andrea/Documents/AIDA/Progetti/unisurf/data/tabelle_ponte/professor_author_matches_20250820_181744.csv"  # Il tuo file risultati
    
    print("🚀 CREAZIONE CAMPIONE DI VALIDAZIONE")
    print("=" * 50)
    
    validation_file = create_validation_sample(
        input_csv_path=input_file,
        output_csv_path="validation_sample.csv"
    )
    
    print(f"\n🎯 Ora valida manualmente il file: {validation_file}")
    print("Quando hai finito, esegui analyze_validation_results() per vedere i risultati")
    
    # FASE 2: Analizza risultati (da eseguire dopo validazione manuale)
    # Decommentare quando hai completato la validazione:
    
    #print("\n🔍 ANALISI RISULTATI VALIDAZIONE")
    #print("=" * 50)
    #results = analyze_validation_results("validation_sample_COMPILATO.csv")