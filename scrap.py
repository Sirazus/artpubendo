import requests
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import time
import csv
import re
import os
import hashlib
import glob

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
    """Calcula el rango de fechas para quincenas completas (1-15 y 16-fin de mes)"""
    today = datetime.now()
    
    if today.day >= 15:
        # Segunda quincena: desde día 16 hasta último día del mes
        start_date = today.replace(day=16)
        # Calcular último día del mes
        next_month = today.replace(day=28) + timedelta(days=4)
        end_date = next_month - timedelta(days=next_month.day)
        period_name = f"quincena_2_{today.strftime('%Y-%m')}"
    else:
        # Primera quincena: desde día 1 hasta día 15
        start_date = today.replace(day=1)
        end_date = today.replace(day=15)
        period_name = f"quincena_1_{today.strftime('%Y-%m')}"
    
    print(f"📅 Período: {start_date.strftime('%Y-%m-%d')} a {end_date.strftime('%Y-%m-%d')}")
    return start_date.strftime("%Y/%m/%d"), end_date.strftime("%Y/%m/%d"), period_name

def get_next_csv_number(csv_dir='data'):
    """Encuentra el siguiente número para articulos_X.csv"""
    if not os.path.exists(csv_dir):
        return 1
    
    # Buscar todos los archivos articulos_X.csv
    pattern = os.path.join(csv_dir, 'articulos_*.csv')
    csv_files = glob.glob(pattern)
    
    numbers = []
    for file in csv_files:
        # Extraer el número del nombre del archivo
        filename = os.path.basename(file)
        match = re.match(r'articulos_(\d+)\.csv', filename)
        if match:
            numbers.append(int(match.group(1)))
    
    if not numbers:
        return 1
    
    return max(numbers) + 1

def get_articles(start_date, end_date):
    # Construir URL con fechas dinámicas - FORMATO CORREGIDO
    base_url = "https://pubmed.ncbi.nlm.nih.gov/"
    
    # Término de búsqueda más flexible
    search_term = f'("International endodontic journal"[Journal] OR "Journal of endodontics"[Journal]) AND ({start_date}[Date - Entry] : {end_date}[Date - Entry])'
    
    print(f"🔍 Búsqueda: {search_term}")
    
    params = {
        'term': search_term,
        'sort': 'date',
        'size': 100  # Aumentar número de resultados por página
    }
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }
    
    articulos = []
    
    with requests.Session() as session:
        # Paginación
        page = 1
        while True:
            params['page'] = page
            print(f"📄 Procesando página {page}...")
            
            try:
                response = session.get(base_url, params=params, headers=headers, timeout=30)
                response.raise_for_status()
            except Exception as e:
                print(f"❌ Error en la solicitud: {e}")
                break
                
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Debug: mostrar contenido de la página
            if page == 1:
                results_info = soup.find('div', class_='results-amount')
                if results_info:
                    print(f"📊 Info resultados: {results_info.text.strip()}")
            
            articles = soup.find_all('article', class_='full-docsum')
            print(f"📖 Encontrados {len(articles)} artículos en la página {page}")
            
            if not articles:
                print("⏹️ No hay más artículos")
                break
                
            for art in articles:
                try:
                    # Extraer título y enlace
                    title_tag = art.find('a', class_='docsum-title')
                    if not title_tag:
                        print("⚠️ No se encontró título")
                        continue
                        
                    title = clean_text(title_tag.text.strip())
                    link = "https://pubmed.ncbi.nlm.nih.gov" + title_tag['href']
                    
                    # Extraer revista y fecha
                    journal_info = art.find('span', class_='docsum-journal-citation')
                    if journal_info:
                        journal_text = journal_info.text.strip()
                        parts = journal_text.split('.')
                        revista = clean_text(parts[0])
                        fecha = clean_text(parts[1].strip().split(';')[0] if len(parts) > 1 else '')
                    else:
                        revista = "Desconocida"
                        fecha = "Desconocida"
                    
                    # Generar ID único
                    article_id = generate_article_id(title, revista, fecha)
                    
                    # Obtener abstract
                    time.sleep(1)  # Espera entre requests
                    try:
                        art_response = session.get(link, headers=headers, timeout=30)
                        art_soup = BeautifulSoup(art_response.text, 'html.parser')
                        
                        abstract_section = art_soup.find('div', class_='abstract-content')
                        abstract = clean_text(abstract_section.text.strip()) if abstract_section else "No abstract available"
                    except Exception as e:
                        print(f"⚠️ Error obteniendo abstract: {e}")
                        abstract = "Error al obtener abstract"
                    
                    articulos.append({
                        'id': article_id,
                        'title': title,
                        'journal': revista,
                        'date': fecha,
                        'abstract': abstract,
                        'scraped_date': datetime.now().strftime("%Y-%m-%d")
                    })
                    
                    print(f"✅ Procesado: {title[:50]}...")
                    
                except Exception as e:
                    print(f"❌ Error procesando artículo: {str(e)}")
                    continue
            
            # Verificar siguiente página
            next_btn = soup.find('button', class_='next-page-btn')
            if not next_btn or 'disabled' in next_btn.get('class', []):
                print("⏹️ No hay más páginas")
                break
                
            page += 1
            if page > 10:  # Límite de seguridad
                print("⚠️ Límite de páginas alcanzado")
                break
    
    return articulos

def load_existing_articles(master_file='articulos_maestro/articulos.csv'):
    """Carga los artículos existentes del archivo maestro"""
    existing_articles = {}
    if os.path.exists(master_file):
        with open(master_file, 'r', encoding='utf-8-sig') as f:
            reader = csv.DictReader(f)
            for row in reader:
                # Manejar archivos antiguos sin 'id'
                if 'id' not in row:
                    # Generar ID para artículos antiguos
                    row['id'] = generate_article_id(
                        row.get('title', ''),
                        row.get('journal', ''),
                        row.get('date', '')
                    )
                existing_articles[row['id']] = row
    return existing_articles

def migrate_old_format(master_file='articulos_maestro/articulos.csv'):
    """Migra archivos antiguos al nuevo formato con ID"""
    if not os.path.exists(master_file):
        return
    
    with open(master_file, 'r', encoding='utf-8-sig') as f:
        content = f.read()
    
    # Verificar si es formato antiguo (sin 'id')
    if 'id' not in content.split('\n')[0]:
        print("🔄 Migrando archivo antiguo al nuevo formato...")
        
        # Leer datos antiguos
        articles = []
        with open(master_file, 'r', encoding='utf-8-sig') as f:
            reader = csv.DictReader(f)
            for row in reader:
                # Agregar ID y fecha de scraping
                row['id'] = generate_article_id(
                    row.get('title', ''),
                    row.get('journal', ''),
                    row.get('date', '')
                )
                row['scraped_date'] = datetime.now().strftime("%Y-%m-%d")
                articles.append(row)
        
        # Guardar en nuevo formato
        with open(master_file, 'w', newline='', encoding='utf-8-sig') as f:
            fieldnames = ['id', 'title', 'journal', 'date', 'abstract', 'scraped_date']
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(articles)
        
        print("✅ Migración completada")

def save_to_master(articles, master_file='articulos_maestro/articulos.csv'):
    """Guarda artículos en el archivo maestro, evitando duplicados"""
    # Primero migrar si es necesario
    migrate_old_format(master_file)
    
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
    print(f"   - Artículos encontrados en este período: {len(resultados)}")
    print(f"   - Artículos nuevos agregados al maestro: {new_count}")
    print(f"   - Archivo numerado: {numbered_filename}")
    print(f"   - Archivo maestro: {master_file}")
