import requests
import json
from pathlib import Path
import pandas as pd
import os

def scarica_authors_OA():

    input_file = "ETL/TabellePonte/ponte_OA_MIUR_test"
    output_file = "oa_authors_test.jsonl"

    with open(output_file, "w", encoding="utf-8") as out_f:
        df = pd.read_csv(input_file, sep=';')

        for row in df.itertuples(index=False):
            ateneo = row.NomeEsteso
            ror = row.ror
            id = row.id_oa

            print(f"[{ateneo}] Inizio estrazione...")

            cursor = "*"
            tot_scritti = 0

            while True:
                try:
                    response = requests.get(
                        f"https://api.openalex.org/authors",
                        params={
                            "filter": f"affiliations.institution.id:{id}",
                            "cursor": cursor,
                            "per_page": 200
                        },
                        timeout=30
                    )
                except Exception as e:
                    print(f"  Errore di rete per {ateneo}: {e}. Termino.")
                    break

                if response.status_code != 200:
                    print(f"  HTTP {response.status_code} per {ateneo}: {response.text[:200]}. Termino.")
                    break

                data = response.json()

                print(f"Trovati {data['meta']['count']} Autori per {ateneo}")

                for autore in data.get("results", []):
                        out_f.write(json.dumps(autore, ensure_ascii=False) + "\n")
                        tot_scritti += 1
                        cursor = data['meta']['next_cursor']
                    
                if not cursor:
                    break

            # Controlla se ci sono altre pagine

            
            print(f"[{ateneo}] Scritti {tot_scritti} autori su file {output_file}")

def scarica_works_OA():
    input_file = "ETL/TabellePonte/ponte_OA_MIUR_test"   # CSV sep=';'
    output_file = "data/raw_data/openalex/oa_works_test.jsonl"

    # assicura che la cartella di output esista (anche se salvi in cwd va bene)
    out_dir = os.path.dirname(output_file) or "."
    os.makedirs(out_dir, exist_ok=True)

    df = pd.read_csv(input_file, sep=';')

    with open(output_file, "w", encoding="utf-8") as out_f:
        for row in df.itertuples(index=False):
            ateneo = row.NomeEsteso
            ror = row.ror
            inst_id = row.id_oa_inst  # evita di chiamarla "id" (ombra built-in)

            print(f"[{ateneo}] Inizio estrazione…")

            cursor = "*"
            tot_scritti = 0
            total_count = None

            test = 0
            while True and test < 5:
                try:
                    response = requests.get(
                        "https://api.openalex.org/works",
                        params={
                            "filter": f"authorships.institutions.id:{inst_id}",
                            "cursor": cursor,
                            "per_page": 200  # max consentito
                        },
                        timeout=30
                    )
                except Exception as e:
                    print(f"  Errore di rete per {ateneo}: {e}. Termino.")
                    break

                if response.status_code != 200:
                    print(f"  HTTP {response.status_code} per {ateneo}: {response.text[:200]}. Termino.")
                    break

                try:
                    data = response.json()
                except Exception as e:
                    print(f"  Errore nel parsing JSON per {ateneo}: {e}.")
                    break

                meta = data.get("meta", {}) or {}
                results = data.get("results", []) or []

                if total_count is None:
                    total_count = meta.get("count")

                # scrivi ogni autore come una riga JSON
                for autore in results[0:5]:
                    out_f.write(json.dumps(autore, ensure_ascii=False) + "\n")
                    tot_scritti += 1

                next_cursor = meta.get("next_cursor")

                # fine pagine
                if not next_cursor or len(results) == 0:
                    break

                cursor = next_cursor
                #time.sleep(0.05)  # piccola pausa per rispetto rate limit
                test += 1

            if total_count is not None:
                print(f"[{ateneo}] Trovati {total_count} paper. Scritti {tot_scritti}.")
            else:
                print(f"[{ateneo}] Scritti {tot_scritti}.")

    print(f"Estrazione completata. Output → {output_file}")

#scarica_authors_OA()
scarica_works_OA()