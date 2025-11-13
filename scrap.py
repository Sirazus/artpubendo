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
        # Remover múltiples espacios y saltos de línea
        text = re.sub(r'\s+', ' ', text)
        # Remover caracteres problemáticos para CSV
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
    
    print(f"📅 Período: {start_date.strftime('%Y-%m-%d')} a {end_date.strftime('%Y-%m-%d')}")
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
    """Obtiene artículos de PubMed - VERSIÓN SIMPLIFICADA COMO EL ORIGINAL"""
    
    # URL base como en el script original
    base_url = "https://pubmed.ncbi.nlm.nih.gov/"
    
    # Construir término de búsqueda como en el original
    search_term = f'("International endodontic journal"[Journal] OR "Journal of endodontics"[Journal]) AND ("{start_date}"[Date - Entry] : "{end_date}"[Date - Entry])'
    
    print(f"🔍 Búsqueda: {search_term}")
    
    # Parámetros SIMPLES como en el original
    params = {
        'term': search_term,
        'sort': 'date'
    }
    
    # Headers SIMPLES como en el original
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }
    
    articulos = []
    
    with requests.Session() as session:
        # Paginación - igual que el original
        page = 1
        while True:
            params['page'] = page
            response = session.get(base_url, params=params, headers=headers)
            soup = BeautifulSoup(response.text, 'html.parser')
            
            articles = soup.find_all('article', class_='full-docsum')
            if not articles:
                break
                
            for art in articles:
                try:
                    # Extraer título y enlace - IGUAL QUE EL ORIGINAL
                    title_tag = art.find('a', class_='docsum-title')
                    title = clean_text(title_tag.text.strip())
                    link = "https://pubmed.ncbi.nlm.nih.gov" + title_tag['href']
                    
                    # Extraer revista y fecha - IGUAL QUE EL ORIGINAL
                    journal_info = art.find('span', class_='docsum-journal-citation').text.strip()
                    parts = journal_info.split('.')
                    revista = clean_text(parts[0])
                    fecha = clean_text(parts[1].strip().split(';')[0] if len(parts) > 1 else '')
                    
                    # Generar ID único (NUEVO - para evitar duplicados)
                    article_id = generate_article_id(title, revista, fecha)
                    
                    # Obtener abstract - IGUAL QUE EL ORIGINAL
                    time.sleep(1)  # Espera entre requests
                    art_response = session.get(link, headers=headers)
                    art_soup = BeautifulSoup(art_response.text, 'html.parser')
                    
                    abstract_section = art_soup.find('div', class_='abstract-content')
                    abstract = clean_text(abstract_section.text.strip()) if abstract_section else "No abstract available"
                    
                    articulos.append({
                        'id': article_id,  # NUEVO campo
                        'title': title,
                        'journal': revista,
                        'date': fecha,
                        'abstract': abstract,
                        'scraped_date': datetime.now().strftime("%Y-%m-%d")  # NUEVO campo
                    })
                    
                    print(f"Procesado: {title[:50]}...")
                    
                except Exception as e:
                    print(f"Error procesando artículo: {str(e)}")
                    continue
            
            # Verificar siguiente página - IGUAL QUE EL ORIGINAL
            next_btn = soup.find('button', class_='next-page-btn')
            if not next_btn or 'disabled' in next_btn.get('class', []):
                break
                
            page += 1
            print(f"Página {page} procesada")
    
    return articulos

def load_existing_articles(master_file='articulos_maestro/articulos.csv'):
    """Carga los artículos existentes del archivo maestro"""
    existing_articles = {}
    if os.path.exists(master_file):
        with open(master_file, 'r', encoding='utf-8-sig') as f:
            reader = csv.DictReader(f)
            for row in reader:
                existing_articles[row['id']] = row
    return existing_articles

def save_to_master(articles, master_file='articulos_maestro/articulos.csv'):
    """Guarda artículos en el archivo maestro, evitando duplicados"""
    existing_articles = load_existing_articles(master_file)
    
    # Filtrar artículos nuevos
    new_articles = [article for article in articles if article['id'] not in existing_articles]
    
    if not new_articles:
        print("No hay artículos nuevos para agregar al archivo maestro")
        return 0
    
    # Combinar existentes con nuevos
    all_articles = list(existing_articles.values()) + new_articles
    
    # Guardar todo
    with open(master_file, 'w', newline='', encoding='utf-8-sig') as f:
        fieldnames = ['id', 'title', 'journal', 'date', 'abstract', 'scraped_date']
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(all_articles)
    
    return len(new_articles)

# Ejecutar y guardar resultados
if __name__ == "__main__":
    # Crear carpetas si no existen
    data_dir = 'data'
    maestro_dir = 'articulos_maestro'
    os.makedirs(data_dir, exist_ok=True)
    os.makedirs(maestro_dir, exist_ok=True)
    
    # Obtener el rango de fechas (quincenas completas)
    start_date, end_date, period_name = get_date_range()
    
    print(f"Buscando artículos desde {start_date} hasta {end_date}")
    
    resultados = get_articles(start_date, end_date)
    
    # 1. Crear archivo numerado articulos_X.csv en carpeta data/
    next_number = get_next_csv_number(data_dir)
    numbered_filename = os.path.join(data_dir, f"articulos_{next_number}.csv")
    
    # Guardar archivo del período - como el original pero con encoding mejorado
    with open(numbered_filename, 'w', newline='', encoding='utf-8-sig') as f:
        fieldnames = ['id', 'title', 'journal', 'date', 'abstract', 'scraped_date']
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(resultados)
    
    print(f"✓ Archivo del período guardado: {numbered_filename}")
    
    # 2. Agregar al archivo maestro en carpeta articulos_maestro/
    master_file = os.path.join(maestro_dir, 'articulos.csv')
    new_count = save_to_master(resultados, master_file)
    
    print(f"✓ Se encontraron {len(resultados)} artículos en este período")
    print(f"✓ Se agregaron {new_count} artículos nuevos al archivo maestro")
    print(f"✓ Archivos guardados:")
    print(f"   - {numbered_filename} (período actual)")
    print(f"   - {master_file} (maestro acumulativo)")
