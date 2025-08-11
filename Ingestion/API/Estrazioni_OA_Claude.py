import os
import json
import pandas as pd
import requests
import time
from datetime import datetime
from tqdm import tqdm

def scarica_works_OA():
    input_file = "ETL/TabellePonte/ponte_OA_MIUR_test"  # CSV sep=';'
    output_file = "data/raw_data/openalex/oa_works_test.jsonl"
    metadata_file = "data/raw_data/openalex/extraction_metadata.json"
    
    # Assicura che la cartella di output esista
    out_dir = os.path.dirname(output_file) or "."
    os.makedirs(out_dir, exist_ok=True)
    
    # Carica il DataFrame
    df = pd.read_csv(input_file, sep=';')
    
    # Inizializza i metadati dell'estrazione
    extraction_metadata = {
        "timestamp": datetime.now().isoformat(),
        "total_universities": len(df),
        "universities": [],
        "total_works_extracted": 0,
        "errors": []
    }
    
    print(f"Inizio estrazione per {len(df)} università...")
    
    # Apri il file di output una sola volta
    with open(output_file, "w", encoding="utf-8") as out_f:
        # Progress bar per le università
        for idx, row in tqdm(df.iterrows(), total=len(df), desc="Università"):
            ateneo = row.NomeEsteso
            ror = row.ror
            inst_id = row.id_oa_inst
            
            print(f"\n[{idx+1}/{len(df)}] {ateneo}")
            print(f"ID OpenAlex: {inst_id}")
            
            cursor = "*"
            tot_scritti = 0
            total_count = None
            pagina = 0
            errori = []
            
            # Progress bar per le pagine (sarà aggiornata quando conosciamo il totale)
            pbar_pages = None
            
            while True:
                try:
                    pagina += 1
                    response = requests.get(
                        "https://api.openalex.org/works",
                        params={
                            "filter": f"authorships.institutions.id:{inst_id}",
                            "cursor": cursor,
                            "per_page": 200  # max consentito
                        },
                        timeout=30
                    )
                    
                    # Verifica status code
                    if response.status_code != 200:
                        error_msg = f"HTTP {response.status_code}: {response.text[:200]}"
                        print(f"  ❌ {error_msg}")
                        errori.append(error_msg)
                        break
                    
                    # Parse JSON
                    try:
                        data = response.json()
                    except Exception as e:
                        error_msg = f"Errore parsing JSON: {e}"
                        print(f"  ❌ {error_msg}")
                        errori.append(error_msg)
                        break
                    
                    meta = data.get("meta", {}) or {}
                    results = data.get("results", []) or []
                    
                    # Alla prima pagina, ottieni il conteggio totale
                    if total_count is None:
                        total_count = meta.get("count", 0)
                        if total_count > 0:
                            pagine_totali = (total_count + 199) // 200  # ceiling division
                            pbar_pages = tqdm(total=pagine_totali, desc="  Pagine", leave=False)
                            print(f"  📊 Totale works da scaricare: {total_count:,}")
                        else:
                            print("  ℹ️  Nessun work trovato per questa università")
                            break
                    
                    # Scrivi i risultati nel file
                    for work in results:
                        # Aggiungi metadati dell'università al work
                        work['_extraction_metadata'] = {
                            'university': ateneo,
                            'ror_id': ror,
                            'openalex_institution_id': inst_id,
                            'extraction_timestamp': datetime.now().isoformat()
                        }
                        out_f.write(json.dumps(work, ensure_ascii=False) + "\n")
                        tot_scritti += 1
                    
                    # Aggiorna progress bar
                    if pbar_pages:
                        pbar_pages.update(1)
                        pbar_pages.set_postfix({
                            'works': f"{tot_scritti:,}/{total_count:,}",
                            'pagina': pagina
                        })
                    
                    # Flush periodico per assicurarsi che i dati vengano scritti
                    if pagina % 10 == 0:
                        out_f.flush()
                        os.fsync(out_f.fileno())
                    
                    # Controlla se ci sono altre pagine
                    next_cursor = meta.get("next_cursor")
                    if not next_cursor or len(results) == 0:
                        break
                    
                    cursor = next_cursor
                    
                    # Pausa per rispettare rate limit (OpenAlex consiglia max 10 req/sec)
                    time.sleep(0.1)
                    
                except requests.exceptions.RequestException as e:
                    error_msg = f"Errore di rete pagina {pagina}: {e}"
                    print(f"  ❌ {error_msg}")
                    errori.append(error_msg)
                    # Riprova una volta dopo una pausa più lunga
                    if pagina == 1 or len(errori) <= 3:
                        print("  🔄 Riprovo tra 5 secondi...")
                        time.sleep(5)
                        continue
                    else:
                        print("  ❌ Troppi errori, salto questa università")
                        break
                except Exception as e:
                    error_msg = f"Errore imprevisto pagina {pagina}: {e}"
                    print(f"  ❌ {error_msg}")
                    errori.append(error_msg)
                    break
            
            # Chiudi progress bar delle pagine
            if pbar_pages:
                pbar_pages.close()
            
            # Flush finale per questa università
            out_f.flush()
            os.fsync(out_f.fileno())
            
            # Salva metadati per questa università
            uni_metadata = {
                "name": ateneo,
                "ror_id": ror,
                "openalex_institution_id": inst_id,
                "total_works_found": total_count or 0,
                "total_works_extracted": tot_scritti,
                "pages_processed": pagina,
                "extraction_complete": tot_scritti == (total_count or 0),
                "errors": errori
            }
            extraction_metadata["universities"].append(uni_metadata)
            extraction_metadata["total_works_extracted"] += tot_scritti
            
            # Stampa riassunto per questa università
            if total_count is not None and total_count > 0:
                completeness = (tot_scritti / total_count) * 100
                status = "✅" if completeness >= 99.9 else "⚠️"
                print(f"  {status} Completato: {tot_scritti:,}/{total_count:,} works ({completeness:.1f}%)")
            else:
                print(f"  ℹ️  Completato: {tot_scritti:,} works")
            
            if errori:
                print(f"  ⚠️  {len(errori)} errori riscontrati")
                extraction_metadata["errors"].extend([f"{ateneo}: {err}" for err in errori])
    
    # Salva i metadati dell'estrazione
    extraction_metadata["completion_timestamp"] = datetime.now().isoformat()
    
    with open(metadata_file, "w", encoding="utf-8") as meta_f:
        json.dump(extraction_metadata, meta_f, indent=2, ensure_ascii=False)
    
    # Stampa riassunto finale
    print(f"\n{'='*60}")
    print(f"🎉 ESTRAZIONE COMPLETATA!")
    print(f"📁 File output: {output_file}")
    print(f"📋 Metadati: {metadata_file}")
    print(f"🏛️  Università processate: {len(extraction_metadata['universities'])}")
    print(f"📄 Totale works estratti: {extraction_metadata['total_works_extracted']:,}")
    if extraction_metadata["errors"]:
        print(f"⚠️  Errori totali: {len(extraction_metadata['errors'])}")
    print(f"{'='*60}")
    
    return extraction_metadata

# Esempio di come usare la funzione e analizzare i metadati
def analizza_metadati(metadata_file="data/raw_data/openalex/extraction_metadata.json"):
    """Funzione per analizzare i metadati dell'estrazione"""
    
    with open(metadata_file, "r", encoding="utf-8") as f:
        metadata = json.load(f)
    
    print("ANALISI ESTRAZIONE:")
    print("-" * 40)
    
    # Statistiche generali
    print(f"Data estrazione: {metadata['timestamp']}")
    print(f"Università totali: {metadata['total_universities']}")
    print(f"Works totali estratti: {metadata['total_works_extracted']:,}")
    
    # Top università per numero di works
    universities = metadata['universities']
    universities_sorted = sorted(universities, key=lambda x: x['total_works_extracted'], reverse=True)
    
    print(f"\nTop 10 università per numero di works:")
    for i, uni in enumerate(universities_sorted[:10], 1):
        completeness = ""
        if uni['total_works_found'] > 0:
            pct = (uni['total_works_extracted'] / uni['total_works_found']) * 100
            completeness = f" ({pct:.1f}% completo)"
        print(f"{i:2d}. {uni['name']}: {uni['total_works_extracted']:,}{completeness}")
    
    # Università con estrazioni incomplete
    incomplete = [uni for uni in universities if not uni['extraction_complete']]
    if incomplete:
        print(f"\n⚠️  Università con estrazioni incomplete: {len(incomplete)}")
        for uni in incomplete:
            print(f"   - {uni['name']}: {uni['total_works_extracted']}/{uni['total_works_found']}")
    
    # Errori
    if metadata['errors']:
        print(f"\n❌ Errori riscontrati: {len(metadata['errors'])}")
        for error in metadata['errors'][:5]:  # mostra solo i primi 5
            print(f"   - {error}")
        if len(metadata['errors']) > 5:
            print(f"   ... e altri {len(metadata['errors']) - 5} errori")

if __name__ == "__main__":
    # Esegui l'estrazione
    metadata = scarica_works_OA()
    
    # Analizza i risultati
    print("\n")
    analizza_metadati()