import requests
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import time
import csv
import re
import os
import hashlib
import glob
from calendar import monthrange

def clean_text(text):
    """Limpia el texto de caracteres problemáticos"""
    if text:
        text = re.sub(r'\s+', ' ', text)
        text = text.replace('\n', ' ').replace('\r', ' ')
        return text.strip()
    return ""

def generate_article_id(title, journal, date):
    """Genera un ID único para el artículo basado en título, revista y fecha"""
    content = f"{title}_{journal}_{date}"
    return hashlib.md5(content.encode()).hexdigest()

def get_date_range():
    """Calcula el rango de fechas para quincenas completas del MES ACTUAL"""
    today = datetime.now()
    current_year = today.year
    current_month = today.month
    
    # Obtener el último día del mes actual
    _, last_day = monthrange(current_year, current_month)
    
    if today.day >= 15:
        # Segunda quincena: días 16 hasta fin de mes
        start_date = datetime(current_year, current_month, 16)
        end_date = datetime(current_year, current_month, last_day)
        period_name = f"quincena_2_{current_year}-{current_month:02d}"
    else:
        # Primera quincena: días 1-15
        start_date = datetime(current_year, current_month, 1)
        end_date = datetime(current_year, current_month, 15)
        period_name = f"quincena_1_{current_year}-{current_month:02d}"
    
    print(f"📅 Período REAL: {start_date.strftime('%Y-%m-%d')} a {end_date.strftime('%Y-%m-%d')}")
    return start_date.strftime("%Y/%m/%d"), end_date.strftime("%Y/%m/%d"), period_name

def get_next_csv_number(csv_dir='data'):
    """Encuentra el siguiente número para articulos_X.csv"""
    if not os.path.exists(csv_dir):
        return 1
    
    pattern = os.path.join(csv_dir, 'articulos_*.csv')
    csv_files = glob.glob(pattern)
    
    numbers = []
    for file in csv_files:
        filename = os.path.basename(file)
        match = re.match(r'articulos_(\d+)\.csv', filename)
        if match:
            numbers.append(int(match.group(1)))
    
    return max(numbers) + 1 if numbers else 1

def get_articles(start_date, end_date):
    """Obtiene artículos de PubMed con headers mejorados"""
    base_url = "https://pubmed.ncbi.nlm.nih.gov/"
    
    search_term = f'("International endodontic journal"[Journal] OR "Journal of endodontics"[Journal]) AND ("{start_date}"[Date - Entry] : "{end_date}"[Date - Entry])'
    
    print(f"🔍 Búsqueda: {search_term}")
    
    params = {
        'term': search_term,
        'sort': 'date',
        'size': 50
    }
    
    # Headers más realistas
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.9',
        'Accept-Encoding': 'gzip, deflate, br',
        'DNT': '1',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1',
        'Sec-Fetch-Dest': 'document',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-Site': 'none',
        'Cache-Control': 'max-age=0',
    }
    
    articulos = []
    
    with requests.Session() as session:
        session.headers.update(headers)
        
        try:
            print("🌐 Realizando solicitud a PubMed...")
            response = session.get(base_url, params=params, timeout=30)
            print(f"📡 Status Code: {response.status_code}")
            
            if response.status_code != 200:
                print(f"❌ Error HTTP {response.status_code}")
                # Intentar una búsqueda más simple para diagnóstico
                print("🔧 Probando búsqueda simplificada...")
                test_params = {'term': 'endodontic', 'sort': 'date'}
                test_response = session.get(base_url, params=test_params, timeout=30)
                print(f"📡 Status Code (búsqueda test): {test_response.status_code}")
                return articulos
                
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Verificar si hay bloqueo o captcha
            if "captcha" in response.text.lower() or "access denied" in response.text.lower():
                print("🚫 Posible bloqueo por CAPTCHA o acceso denegado")
                return articulos
            
            # Buscar artículos
            articles = soup.find_all('article', class_='full-docsum')
            print(f"📖 Encontrados {len(articles)} artículos")
            
            if not articles:
                no_results = soup.find('div', class_='no-results-message')
                if no_results:
                    print("📭 No se encontraron resultados")
                return articulos
            
            for i, art in enumerate(articles):
                try:
                    print(f"  📝 Procesando artículo {i+1}/{len(articles)}...")
                    
                    # Extraer título
                    title_tag = art.find('a', class_='docsum-title')
                    if not title_tag:
                        continue
                        
                    title = clean_text(title_tag.text.strip())
                    link = "https://pubmed.ncbi.nlm.nih.gov" + title_tag['href']
                    
                    # Extraer revista y fecha
                    journal_info = art.find('span', class_='docsum-journal-citation')
                    revista = "Desconocida"
                    fecha = "Desconocida"
                    
                    if journal_info:
                        journal_text = journal_info.text.strip()
                        parts = journal_text.split('. ')
                        revista = clean_text(parts[0]) if parts else "Desconocida"
                        fecha = clean_text(parts[1]) if len(parts) > 1 else "Desconocida"
                    
                    # Generar ID único
                    article_id = generate_article_id(title, revista, fecha)
                    
                    # Obtener abstract
                    time.sleep(2)  # Espera más larga para evitar bloqueos
                    abstract = "No abstract available"
                    
                    try:
                        art_response = session.get(link, timeout=30)
                        if art_response.status_code == 200:
                            art_soup = BeautifulSoup(art_response.text, 'html.parser')
                            abstract_section = art_soup.find('div', class_='abstract-content')
                            if abstract_section:
                                abstract = clean_text(abstract_section.text.strip())
                    except Exception as e:
                        print(f"    ⚠️ Error obteniendo abstract: {e}")
                    
                    articulos.append({
                        'id': article_id,
                        'title': title,
                        'journal': revista,
                        'date': fecha,
                        'abstract': abstract,
                        'scraped_date': datetime.now().strftime("%Y-%m-%d")
                    })
                    
                    print(f"    ✅ {title[:50]}...")
                    
                except Exception as e:
                    print(f"    ❌ Error: {e}")
                    continue
                    
        except Exception as e:
            print(f"❌ Error general: {e}")
    
    return articulos

# ... (el resto de las funciones se mantienen igual: load_existing_articles, migrate_old_format, save_to_master)

# Ejecutar y guardar resultados
if __name__ == "__main__":
    # Crear carpetas si no existen
    data_dir = 'data'
    maestro_dir = 'articulos_maestro'
    os.makedirs(data_dir, exist_ok=True)
    os.makedirs(maestro_dir, exist_ok=True)
    
    print(f"🕐 Fecha actual del sistema: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Obtener el rango de fechas (quincenas completas)
    start_date, end_date, period_name = get_date_range()
    
    print(f"🔍 Buscando artículos desde {start_date} hasta {end_date}")
    
    resultados = get_articles(start_date, end_date)
    
    print(f"📊 Total de artículos encontrados: {len(resultados)}")
    
    # 1. Crear archivo numerado articulos_X.csv en carpeta data/
    next_number = get_next_csv_number(data_dir)
    numbered_filename = os.path.join(data_dir, f"articulos_{next_number}.csv")
    
    with open(numbered_filename, 'w', newline='', encoding='utf-8-sig') as f:
        fieldnames = ['id', 'title', 'journal', 'date', 'abstract', 'scraped_date']
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        if resultados:
            writer.writerows(resultados)
            print(f"✅ Archivo numerado guardado: {numbered_filename} con {len(resultados)} artículos")
        else:
            print("⚠️ Archivo numerado creado vacío")
    
    # 2. Agregar al archivo maestro en carpeta articulos_maestro/
    master_file = os.path.join(maestro_dir, 'articulos.csv')
    new_count = save_to_master(resultados, master_file)
    
    print(f"📈 Resumen final:")
    print(f"   - Artículos encontrados: {len(resultados)}")
    print(f"   - Nuevos en maestro: {new_count}")
    print(f"   - Archivo numerado: {numbered_filename}")
    print(f"   - Archivo maestro: {master_file}")
