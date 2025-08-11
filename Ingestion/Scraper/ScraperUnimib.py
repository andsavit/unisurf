import requests
from bs4 import BeautifulSoup
from collections import deque
from dataclasses import dataclass, field, asdict
from typing import Optional, Dict
import os
import json
from typing import List
import pandas as pd
import csv

@dataclass
class Insegnamento:
    
    titolo: str
    codice_corso: Optional[str] = None
    cfu: Optional[float] = None
    periodo: Optional[str] = None
    ateneo: Optional[str] = None
    area: Optional[str] = None
    corso_di_laurea: Optional[str] = None
    anno_accademico: Optional[str] = None
    anno_corso: Optional[str] = None
    tipo_att: Optional[str] = None
    ore: Optional[int] = None
    tipologia_cds: Optional[str] = None
    lingua: Optional[str] = None
    staff: Dict[str, str] = field(default_factory=dict)

def inizio_navigazione():

    "Parte dalla pagina che ha il menu delle aree, ne salva i link"

    BASE_URL = "https://www.unimib.it"

    START_URL = "https://www.unimib.it/studiare/corsi-laurea-iscrizioni/area-economico-statistica-laurea-triennale"

    headers = {"User-Agent": "Mozilla/5.0"}

    # Scarica la pagina iniziale
    try:
        response = requests.get(START_URL, headers=headers)
        soup = BeautifulSoup(response.text, "html.parser")
    except:
        print(response.status_code)

    print(response)

    # Trova la <ul class="menu">
    menu = soup.find("li", class_="nav-item menu-item--expanded menu-item--active-trail")

    # Estrai tutti i link delle voci di menu
    links = []
    titles = []
    links.append(START_URL) #inserisce il link di partenza, diverso in studiare/ vs didattica/

    for li in menu.find_all("li", class_="nav-item"):
        a = li.find("a")
        if a and a.get("href"):
            href = a["href"]
            #print(href)
            # Completa l’URL relativo
            full_url = BASE_URL + href
            links.append(full_url)
            text = a.get_text(strip=True)
            titles.append(text)
            #print(f"{text} → {full_url}")

    #chiamata della funzione di accesso ai box dei corsi
    print(f"Trovate {len(links)} Aree")
    for link in links:
        trova_cdl_area(link)
            

    """
    # Salva su file (CSV o TXT)
    with open("aree_didattiche_unimib.csv", "w", encoding="utf-8") as f:
        for text, url in links:
            f.write(f"{text};{url}\n")
    """


def trova_cdl_area(link):
    """
    Entrata nella pagina dell'area con la lista dei CDL, gli estrae, salvando nome e url su file, restituisce gli url
    """
    seen = set()

    try:
        response = requests.get(link)
        soup = BeautifulSoup(response.text, "html.parser")
    except:
        pass

    if response.status_code != 200:
        print(f"ATTENZIONE: Problema {response.status_code} con il link: {link}")

    BASE_URL = "www.unimib.it"

    cdl_boxes = soup.find_all("div", class_="cdl-anteprima__box")

    tuples = []

    for box in cdl_boxes:
        a = box.find('a')
        if a and a.get("href"):
            href = a["href"]
            full_url = BASE_URL + href
            text = a.get_text(strip=True)
            tuples.append((text, full_url))
            print(f"{text} -> {full_url}")
    
    with open("cdl_unimib.csv", "a", encoding="utf-8") as f:
        for text, url in tuples:
            if url not in seen:
                f.write(f"{text};{url}\n")
                seen.add(url)
        print(f"Salvati {len(tuples)} corsi.")
    
    link_cdl = [l[1] for l in tuples]
    print(link_cdl)

    return link_cdl

def trova_index(link):
    """Da pagina del CDL restituisce IL link agli insegnamenti per A.A."""
    stringa_anno = "A.A. 2024-2025"

    try:
        response = requests.get(link)
        soup = BeautifulSoup(response.text, "html.parser")
    except:
        print(f"Problema con il link {link}. Response: {response.status_code}")


    if response.status_code != 200:
        print(f"ATTENZIONE: problema {response.status_code} con il link: {link}")


    link_insegnamenti = ""
    index_link = ""

    #trova il bottone "insegnamenti"
    for a in soup.find_all('a'):
        if a.get("href") and a.text == "Insegnamenti":
                index_link = a["href"]
                print(index_link)

    #pagina avanti

    if index_link != "":
        try:
            response = requests.get(index_link)
            soup = BeautifulSoup(response.text, "html.parser")
        except:
            print(f"Problema con la pagina degli insegnamenti di {link}")
    else:
        print(f"Bottone Insgnementi non trovato per {link}")

    #print(f"Seconda chiamata: {response}")

    #trova l'A.A. specificato nella variabile ed estrai il link alla lista di insegnamenti
    for a in soup.find_all('a', class_="info px-3 transition-hover-bg d-block"):
        if a.get("href") and a.get("title") == stringa_anno:
            link_insegnamenti =  a["href"]
            #print(link_insegnamenti)  
             
    print(".")

    if link_insegnamenti != "":
        return link_insegnamenti
    else:
        return None

def iteratore_pagine_cdl(file = "/Users/andrea/Documents/AIDA/Progetti/unisurf/cdl_unimib.csv"):
    "Legge i file con i link dei CDL e arriva ad estrazione per tutti"
    urls = []
    pagine_cdl_aa = []

    with open(file, newline='', encoding="utf-8") as csvfile:
        reader = csv.reader(csvfile, delimiter=";")
        for row in reader:
            if len(row) >= 2:
                urls.append(row[1])  # seconda colonna

    #print(urls[1])

    print(f"-----Letti {len(urls)} URL di pagine dei CDL------")

    for url in urls:
        print(url)
        pagine_cdl_aa.append(trova_index("https://" + url))

    """
    print(pagine_cdl_aa)
    
    for el in pagine_cdl_aa:
        estrai_info(el)
    """

def estrai_info(start_link):
    """Parte dalla pagina dell'AA e arriva ad estrazione """
    response = requests.get(start_link)
    soup = BeautifulSoup(response.text, "html.parser")

    print(f"Chiamata menu anni: {response}")

    anni = soup.find_all('a', class_="info px-3 transition-hover-bg d-block")

    print(len(anni))

    links = []

    for anno in anni:
        anno_corso = anno.get("title")
        link = anno.get("href")

        #print((anno_corso, link))

        response = requests.get(link)
        soup = BeautifulSoup(response.text, "html.parser")

        print(f"Chiamata lista corsi: {response}")

        insegnamenti = soup.find_all('a', class_= "d-block w-100")

        for insegnamento in insegnamenti:
            link_syllabus = insegnamento.get('href')

            estrai_info_syllabus(link_syllabus)
            """
            response = requests.get(link)
            soup = BeautifulSoup(response.text, "html.parser")

            titolo = soup.find('div', class_='card-title course-fullname text-truncate')

            rows = soup.find_all('div', class_="row no-gutters w-100")

            for row in rows:
                cols = row.find_all("div")

                if cols[0].get_text().strip() == "Settore disciplinare":
                    settore_disc = cols[1].get_text()
        """
            
def estrai_info_syllabus(link):
    """Estrae le info vere e proprie e le salva come istanza di un oggetto."""
    response = requests.get(link)
    soup = BeautifulSoup(response.text, "html.parser")

    nome_corso = soup.find('div', class_='card-title course-fullname text-truncate').get_text()

    rows = soup.find_all('div', class_="row no-gutters w-100")

    for row in rows:
        cols = row.find_all("div")

        if cols[0].get_text() == "Settore disciplinare":
            settore_disc = cols[1].get_text()
        elif cols[0].get_text() == "CFU":
            cfu = cols[1].get_text()
        elif cols[0].get_text() == "Periodo":
            periodo = cols[1].get_text()
        elif cols[0].get_text() == "Tipo di attività":
            tipo_att = cols[1].get_text()
        elif cols[0].get_text() == "Ore":
            ore = cols[1].get_text()
        elif cols[0].get_text() == "Tipologia CdS":
            tipo_cds = cols[1].get_text()
        elif cols[0].get_text() == "Lingua":
            lingua = cols[1].get_text()

    staff = estrai_staff(soup)

    breadcrumb = soup.find("ol", class_="breadcrumb category-nav")
    crumbs = [li.get_text(strip=True) for li in breadcrumb.find_all("li", class_="breadcrumb-item")] if breadcrumb else []

    insegnamento = Insegnamento(
        titolo = nome_corso,
        cfu = cfu, 
        periodo = periodo,
        ateneo = "https://ror.org/01ynf4891",
        area = crumbs[0],
        tipologia_cds = crumbs[1],
        corso_di_laurea = crumbs[2],
        anno_accademico = crumbs[4],
        anno_corso = crumbs[5],
        tipo_att = tipo_att,
        ore = ore,
        lingua = lingua,
        staff = staff
    )

    scrivi_insegnamento_senza_duplicati(insegnamento) #chiama la funzione che scrive su json

    #print(insegnamento)

def estrai_staff(soup):
    staff = []

    ul = soup.find('ul', class_="summary-content teachers")
    if not ul:
        return staff

    current_role = None

    for el in ul.children:
        if el.name == "h4" and "contact-role" in el.get("class", []):
            current_role = el.get_text(strip=True)

        elif el.name == "li" and "contact" in el.get("class", []):
            nome_tag = el.find("div", class_="contact-name")
            nome = nome_tag.get_text(strip=True) if nome_tag else "N/D"
            email = el.get("id", "").replace("contact-", "")

            staff.append({
                "nome": nome,
                "email": email,
                "ruolo": current_role or "N/D"
            })

    print(staff)

    return staff 


    #print(titolo, settore_disc)

def scrivi_insegnamento_senza_duplicati(ins: Insegnamento, filepath: str = "insegnamenti_bicocca.json"):

    nuovo = asdict(ins)

    # 1. Carica il file se esiste
    if os.path.exists(filepath):
        with open(filepath, "r", encoding="utf-8") as f:
            try:
                dati: List[dict] = json.load(f)
            except json.JSONDecodeError:
                dati = []
    else:
        dati = []

    # 2. Controlla se già presente 
    già_presente = any(
        d["titolo"] == nuovo["titolo"] and d["corso_di_laurea"] == nuovo["corso_di_laurea"]
        for d in dati
    )

    if not già_presente:
        dati.append(nuovo)

        # 3. Scrive tutto di nuovo (sovrascrive il file)
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(dati, f, ensure_ascii=False, indent=2)
        print(f"Aggiunto: {nuovo['titolo']}")
    else:
        print(f"⚠️ Già presente: {nuovo['titolo']}")

"""
Struttura Sito:

-https://www.unimib.it/studiare/corsi-laurea-iscrizioni/area-economico-statistica-laurea-triennale
	-area X, triennale
		-corso di laurea
			-pagina del corso
				-lista anni accademici
					-anno di corso per anno accademico
						-lista insegnamenti
							-syllabus (estrazione info)
"""

#link = trova_index("https://www.unimib.it/triennale/statistica-gestione-informazioni")

#estrai_info(link)

#estrai_info_syllabus("https://elearning.unimib.it/course/info.php?id=55391") #estrazione e salvataggio informazioni insegnamento

#start_link = "https://elearning.unimib.it/course/index.php?categoryid=11101"
#estrai_info(start_link)

#trova_cdl("https://www.unimib.it/studiare/corsi-laurea-iscrizioni/area-economico-statistica-laurea-triennale")

#1

#inizio_navigazione()

iteratore_pagine_cdl()