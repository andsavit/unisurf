
import json
import csv
import os
import logging
import time
from dataclasses import dataclass, asdict, field
from typing import Optional, Dict, List, Tuple, Set
from urllib.parse import urljoin, urlparse
import requests
from bs4 import BeautifulSoup
from datetime import datetime
import traceback


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
    staff: List[Dict[str, str]] = field(default_factory=list)


@dataclass
class ScrapingStats:
    """Statistiche delle operazioni di scraping"""
    aree_trovate: int = 0
    cdl_trovati: int = 0
    insegnamenti_estratti: int = 0
    errori_http: int = 0
    errori_parsing: int = 0
    link_falliti: List[str] = field(default_factory=list)
    tempo_inizio: Optional[datetime] = None
    tempo_fine: Optional[datetime] = None
    
    def durata_totale(self) -> str:
        if self.tempo_inizio and self.tempo_fine:
            delta = self.tempo_fine - self.tempo_inizio
            return str(delta).split('.')[0]  # Rimuove i microsecondi
        return "N/A"


class UniBicoccaScraper:
    """Scraper principale per l'Università Bicocca"""
    
    def __init__(self, config_file: str = "scraper_config.json"):
        self.config = self._load_config(config_file)
        self.stats = ScrapingStats()
        self.session = requests.Session()
        self.session.headers.update(self.config["headers"])
        
        # Setup logging
        self._setup_logging()
        
        # Set per evitare duplicati
        self.urls_visitati: Set[str] = set()
        self.insegnamenti_salvati: Set[Tuple[str, str]] = set()  # (titolo, corso_di_laurea)
        
        self.logger.info("🚀 Scraper inizializzato")
    
    def _load_config(self, config_file: str) -> Dict:
        """Carica configurazione da file JSON o usa default"""
        default_config = {
            "base_url": "https://www.unimib.it",
            "start_url": "https://www.unimib.it/studiare/corsi-laurea-iscrizioni/area-economico-statistica-laurea-triennale",
            "anno_accademico": "A.A. 2024-2025",
            "headers": {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
            },
            "timeout": 10,
            "retry_attempts": 3,
            "retry_delay": 2,
            "output_files": {
                "cdl": "cdl_unimib.csv",
                "insegnamenti": "insegnamenti_bicocca.json",
                "errori": "errori_scraping.json",
                "statistiche": "statistiche_scraping.json"
            }
        }
        
        if os.path.exists(config_file):
            try:
                with open(config_file, 'r', encoding='utf-8') as f:
                    user_config = json.load(f)
                    default_config.update(user_config)
            except Exception as e:
                print(f"⚠️ Errore nel caricamento config: {e}. Uso configurazione default.")
        
        return default_config
    
    def _setup_logging(self):
        """Configura il sistema di logging"""
        log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        
        # Logger principale
        self.logger = logging.getLogger('UniBicoccaScraper')
        self.logger.setLevel(logging.INFO)
        
        # Handler per file
        file_handler = logging.FileHandler('scraping.log', encoding='utf-8')
        file_handler.setLevel(logging.DEBUG)
        file_handler.setFormatter(logging.Formatter(log_format))
        
        # Handler per console
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        console_handler.setFormatter(logging.Formatter('%(levelname)s - %(message)s'))
        
        self.logger.addHandler(file_handler)
        self.logger.addHandler(console_handler)
    
    def _safe_request(self, url: str, context: str = "") -> Optional[BeautifulSoup]:
        """Esegue una richiesta HTTP con gestione errori e retry"""
        for tentativo in range(self.config["retry_attempts"]):
            try:
                self.logger.debug(f"🌐 Richiesta a: {url} (tentativo {tentativo + 1})")
                
                response = self.session.get(
                    url, 
                    timeout=self.config["timeout"]
                )
                response.raise_for_status()
                
                soup = BeautifulSoup(response.text, "html.parser")
                self.logger.debug(f"✅ Successo per: {url}")
                return soup
                
            except requests.exceptions.RequestException as e:
                self.logger.warning(f"❌ Errore HTTP per {url}: {e}")
                self.stats.errori_http += 1
                
                if tentativo < self.config["retry_attempts"] - 1:
                    time.sleep(self.config["retry_delay"])
                else:
                    self.logger.error(f"💥 Fallimento definitivo per {url} dopo {self.config['retry_attempts']} tentativi")
                    self.stats.link_falliti.append(f"{url} - {str(e)} - {context}")
            
            except Exception as e:
                self.logger.error(f"💥 Errore parsing per {url}: {e}")
                self.stats.errori_parsing += 1
                self.stats.link_falliti.append(f"{url} - Errore parsing: {str(e)} - {context}")
                break
        
        return None
    
    def estrai_aree_didattiche(self) -> List[str]:
        """Estrae i link delle aree didattiche dalla pagina principale"""
        self.logger.info("📚 Inizio estrazione aree didattiche")
        
        soup = self._safe_request(self.config["start_url"], "pagina principale aree")
        if not soup:
            return []
        
        # Trova il menu delle aree
        menu = soup.find("li", class_="nav-item menu-item--expanded menu-item--active-trail")
        if not menu:
            self.logger.error("❌ Menu aree non trovato nella pagina principale")
            return []
        
        links = [self.config["start_url"]]  # Include la pagina di partenza
        
        for li in menu.find_all("li", class_="nav-item"):
            a = li.find("a")
            if a and a.get("href"):
                full_url = urljoin(self.config["base_url"], a["href"])
                links.append(full_url)
                area_nome = a.get_text(strip=True)
                self.logger.info(f"🎯 Area trovata: {area_nome}")
        
        self.stats.aree_trovate = len(links)
        self.logger.info(f"✅ Trovate {len(links)} aree didattiche")
        return links
    
    def estrai_cdl_da_area(self, area_url: str) -> List[Tuple[str, str]]:
        """Estrae i corsi di laurea da una specifica area"""
        self.logger.info(f"🎓 Estrazione CDL da: {area_url}")
        
        soup = self._safe_request(area_url, f"area CDL - {area_url}")
        if not soup:
            return []
        
        cdl_boxes = soup.find_all("div", class_="cdl-anteprima__box")
        cdl_trovati = []
        
        for box in cdl_boxes:
            a = box.find('a')
            if a and a.get("href"):
                cdl_nome = a.get_text(strip=True)
                cdl_url = urljoin(self.config["base_url"], a["href"])
                cdl_trovati.append((cdl_nome, cdl_url))
                self.logger.debug(f"📋 CDL: {cdl_nome}")
        
        self.logger.info(f"✅ Trovati {len(cdl_trovati)} CDL nell'area")
        return cdl_trovati
    
    def salva_cdl_su_file(self, tutti_cdl: List[Tuple[str, str]]):
        """Salva l'elenco dei CDL su file CSV"""
        filepath = self.config["output_files"]["cdl"]
        
        try:
            with open(filepath, "w", encoding="utf-8", newline='') as f:
                writer = csv.writer(f, delimiter=";")
                writer.writerow(["Nome CDL", "URL"])  # Header
                
                for nome, url in tutti_cdl:
                    if url not in self.urls_visitati:
                        writer.writerow([nome, url])
                        self.urls_visitati.add(url)
            
            self.logger.info(f"💾 Salvati {len(tutti_cdl)} CDL in {filepath}")
            
        except Exception as e:
            self.logger.error(f"❌ Errore nel salvataggio CDL: {e}")
    
    def trova_link_insegnamenti(self, cdl_url: str) -> Optional[str]:
        """Trova il link agli insegnamenti per l'A.A. specificato"""
        self.logger.debug(f"🔍 Ricerca insegnamenti per: {cdl_url}")
        
        # Prima pagina: trova il bottone "Insegnamenti"
        soup = self._safe_request(cdl_url, f"pagina CDL - {cdl_url}")
        if not soup:
            return None
        
        link_insegnamenti = None
        for a in soup.find_all('a'):
            if a.get("href") and a.text.strip() == "Insegnamenti":
                link_insegnamenti = a["href"]
                break
        
        if not link_insegnamenti:
            self.logger.warning(f"⚠️ Bottone 'Insegnamenti' non trovato per {cdl_url}")
            return None
        
        # Seconda pagina: trova l'A.A. specificato
        soup = self._safe_request(link_insegnamenti, f"pagina insegnamenti - {link_insegnamenti}")
        if not soup:
            return None
        
        anno_target = self.config["anno_accademico"]
        for a in soup.find_all('a', class_="info px-3 transition-hover-bg d-block"):
            if a.get("href") and a.get("title") == anno_target:
                return a["href"]
        
        self.logger.warning(f"⚠️ Anno accademico {anno_target} non trovato per {cdl_url}")
        return None
    
    def estrai_insegnamenti_da_cdl(self, link_aa: str, nome_cdl: str):
        """Estrae tutti gli insegnamenti da un CDL per l'A.A. specificato"""
        self.logger.info(f"📖 Estrazione insegnamenti da: {nome_cdl}")
        
        soup = self._safe_request(link_aa, f"anni di corso - {nome_cdl}")
        if not soup:
            return
        
        anni_corso = soup.find_all('a', class_="info px-3 transition-hover-bg d-block")
        
        for anno in anni_corso:
            anno_corso = anno.get("title", "N/A")
            link_anno = anno.get("href")
            
            if not link_anno:
                continue
            
            self.logger.debug(f"📅 Processando {anno_corso}")
            
            soup_anno = self._safe_request(link_anno, f"insegnamenti {anno_corso} - {nome_cdl}")
            if not soup_anno:
                continue
            
            insegnamenti = soup_anno.find_all('a', class_="d-block w-100")
            
            for insegnamento in insegnamenti:
                link_syllabus = insegnamento.get('href')
                if link_syllabus:
                    self.estrai_info_syllabus(link_syllabus, nome_cdl, anno_corso)
    
    def estrai_info_syllabus(self, link_syllabus: str, nome_cdl: str, anno_corso: str):
        """Estrae le informazioni dettagliate di un singolo insegnamento"""
        soup = self._safe_request(link_syllabus, f"syllabus - {link_syllabus}")
        if not soup:
            return
        
        try:
            # Estrazione titolo
            titolo_elem = soup.find('div', class_='card-title course-fullname text-truncate')
            if not titolo_elem:
                self.logger.warning(f"⚠️ Titolo non trovato per {link_syllabus}")
                return
            
            titolo = titolo_elem.get_text(strip=True)
            
            # Controlla duplicati
            chiave_insegnamento = (titolo, nome_cdl)
            if chiave_insegnamento in self.insegnamenti_salvati:
                self.logger.debug(f"⏭️ Già presente: {titolo}")
                return
            
            # Estrazione altri campi
            info = self._estrai_info_dettagli(soup)
            staff = self._estrai_staff(soup)
            breadcrumb_info = self._estrai_breadcrumb(soup)
            
            # Creazione oggetto Insegnamento
            insegnamento = Insegnamento(
                titolo=titolo,
                cfu=info.get('cfu'),
                periodo=info.get('periodo'),
                ateneo="https://ror.org/01ynf4891",
                area=breadcrumb_info.get('area'),
                tipologia_cds=breadcrumb_info.get('tipologia_cds'),
                corso_di_laurea=nome_cdl,
                anno_accademico=breadcrumb_info.get('anno_accademico'),
                anno_corso=anno_corso,
                tipo_att=info.get('tipo_att'),
                ore=info.get('ore'),
                lingua=info.get('lingua'),
                staff=staff
            )
            
            # Salvataggio
            self._salva_insegnamento(insegnamento)
            self.insegnamenti_salvati.add(chiave_insegnamento)
            self.stats.insegnamenti_estratti += 1
            
            self.logger.info(f"✅ Estratto: {titolo}")
            
        except Exception as e:
            self.logger.error(f"❌ Errore nell'estrazione da {link_syllabus}: {e}")
            self.logger.debug(traceback.format_exc())
    
    def _estrai_info_dettagli(self, soup: BeautifulSoup) -> Dict:
        """Estrae i dettagli dell'insegnamento dalle righe della pagina"""
        info = {}
        rows = soup.find_all('div', class_="row no-gutters w-100")
        
        mapping = {
            "CFU": "cfu",
            "Periodo": "periodo", 
            "Tipo di attività": "tipo_att",
            "Ore": "ore",
            "Tipologia CdS": "tipologia_cds",
            "Lingua": "lingua"
        }
        
        for row in rows:
            cols = row.find_all("div")
            if len(cols) >= 2:
                chiave = cols[0].get_text(strip=True)
                valore = cols[1].get_text(strip=True)
                
                if chiave in mapping:
                    # Conversioni specifiche
                    if chiave == "CFU":
                        try:
                            valore = float(valore)
                        except:
                            pass
                    elif chiave == "Ore":
                        try:
                            valore = int(valore)
                        except:
                            pass
                    
                    info[mapping[chiave]] = valore
        
        return info
    
    def _estrai_staff(self, soup: BeautifulSoup) -> List[Dict[str, str]]:
        """Estrae informazioni del personale docente"""
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
        
        return staff
    
    def _estrai_breadcrumb(self, soup: BeautifulSoup) -> Dict:
        """Estrae informazioni dal breadcrumb"""
        info = {}
        
        breadcrumb = soup.find("ol", class_="breadcrumb category-nav")
        if breadcrumb:
            crumbs = [li.get_text(strip=True) for li in breadcrumb.find_all("li", class_="breadcrumb-item")]
            
            if len(crumbs) >= 1:
                info['area'] = crumbs[0]
            if len(crumbs) >= 2:
                info['tipologia_cds'] = crumbs[1]
            if len(crumbs) >= 5:
                info['anno_accademico'] = crumbs[4]
        
        return info
    
    def _salva_insegnamento(self, insegnamento: Insegnamento):
        """Salva un insegnamento nel file JSON"""
        filepath = self.config["output_files"]["insegnamenti"]
        
        try:
            # Carica dati esistenti
            if os.path.exists(filepath):
                with open(filepath, "r", encoding="utf-8") as f:
                    try:
                        dati = json.load(f)
                    except json.JSONDecodeError:
                        dati = []
            else:
                dati = []
            
            # Aggiungi nuovo insegnamento
            dati.append(asdict(insegnamento))
            
            # Salva tutto
            with open(filepath, "w", encoding="utf-8") as f:
                json.dump(dati, f, ensure_ascii=False, indent=2)
                
        except Exception as e:
            self.logger.error(f"❌ Errore nel salvataggio insegnamento: {e}")
    
    def salva_errori_e_statistiche(self):
        """Salva gli errori e le statistiche finali"""
        # Salva errori
        if self.stats.link_falliti:
            try:
                with open(self.config["output_files"]["errori"], "w", encoding="utf-8") as f:
                    json.dump({
                        "timestamp": datetime.now().isoformat(),
                        "errori": self.stats.link_falliti
                    }, f, ensure_ascii=False, indent=2)
                
                self.logger.info(f"💾 Salvati {len(self.stats.link_falliti)} errori")
            except Exception as e:
                self.logger.error(f"❌ Errore nel salvataggio errori: {e}")
        
        # Salva statistiche
        try:
            stats_dict = asdict(self.stats)
            with open(self.config["output_files"]["statistiche"], "w", encoding="utf-8") as f:
                json.dump(stats_dict, f, ensure_ascii=False, indent=2, default=str)
            
            self.logger.info("💾 Statistiche salvate")
        except Exception as e:
            self.logger.error(f"❌ Errore nel salvataggio statistiche: {e}")
    
    def esegui_scraping_completo(self):
        """Esegue l'intero processo di scraping"""
        self.stats.tempo_inizio = datetime.now()
        self.logger.info("🚀 INIZIO SCRAPING COMPLETO")
        
        try:
            # 1. Estrai aree didattiche
            aree_urls = self.estrai_aree_didattiche()
            if not aree_urls:
                self.logger.error("❌ Nessuna area didattica trovata. Interrompo.")
                return
            
            # 2. Estrai tutti i CDL
            tutti_cdl = []
            for area_url in aree_urls:
                cdl_area = self.estrai_cdl_da_area(area_url)
                tutti_cdl.extend(cdl_area)
            
            self.stats.cdl_trovati = len(tutti_cdl)
            self.salva_cdl_su_file(tutti_cdl)
            
            # 3. Processa ogni CDL
            for i, (nome_cdl, url_cdl) in enumerate(tutti_cdl, 1):
                self.logger.info(f"🎓 [{i}/{len(tutti_cdl)}] Processando: {nome_cdl}")
                
                link_aa = self.trova_link_insegnamenti(url_cdl)
                if link_aa:
                    self.estrai_insegnamenti_da_cdl(link_aa, nome_cdl)
                else:
                    self.logger.warning(f"⚠️ Link insegnamenti non trovato per: {nome_cdl}")
        
        except KeyboardInterrupt:
            self.logger.warning("⏹️ Scraping interrotto dall'utente")
        except Exception as e:
            self.logger.error(f"💥 Errore durante lo scraping: {e}")
            self.logger.debug(traceback.format_exc())
        
        finally:
            self.stats.tempo_fine = datetime.now()
            self._stampa_riepilogo_finale()
            self.salva_errori_e_statistiche()
    
    def _stampa_riepilogo_finale(self):
        """Stampa un riepilogo finale delle operazioni"""
        self.logger.info("=" * 60)
        self.logger.info("📊 RIEPILOGO FINALE")
        self.logger.info("=" * 60)
        self.logger.info(f"🎯 Aree didattiche trovate: {self.stats.aree_trovate}")
        self.logger.info(f"🎓 CDL trovati: {self.stats.cdl_trovati}")
        self.logger.info(f"📖 Insegnamenti estratti: {self.stats.insegnamenti_estratti}")
        self.logger.info(f"❌ Errori HTTP: {self.stats.errori_http}")
        self.logger.info(f"⚠️ Errori parsing: {self.stats.errori_parsing}")
        self.logger.info(f"💥 Link falliti: {len(self.stats.link_falliti)}")
        self.logger.info(f"⏱️ Durata totale: {self.stats.durata_totale()}")
        self.logger.info("=" * 60)


def main():
    """Funzione principale"""
    scraper = UniBicoccaScraper()
    scraper.esegui_scraping_completo()


if __name__ == "__main__":
    main()