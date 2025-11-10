import requests
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import time
import csv
import re
import os
import hashlib

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
    """Calcula el rango de fechas para el día 1 o 15 del mes actual"""
    today = datetime.now()
    
    if today.day >= 15:
        # Si estamos después del día 15, buscar desde día 15 hasta hoy
        start_date = today.replace(day=15)
        period_name = f"{start_date.strftime('%Y-%m-%d')}_to_{today.strftime('%Y-%m-%d')}"
    else:
        # Si estamos antes del día 15, buscar desde día 1 hasta hoy
        start_date = today.replace(day=1)
        period_name = f"{start_date.strftime('%Y-%m-%d')}_to_{today.strftime('%Y-%m-%d')}"
    
    return start_date.strftime("%Y/%m/%d"), today.strftime("%Y/%m/%d"), period_name

def get_articles(start_date, end_date):
    # Construir URL con fechas dinámicas
    base_url = "https://pubmed.ncbi.nlm.nih.gov/"
    params = {
        'term': f'("International endodontic journal"[Journal] OR "Journal of endodontics"[Journal]) AND ("{start_date}"[Date - Entry] : "{end_date}"[Date - Entry])',
        'sort': 'date'
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
            response = session.get(base_url, params=params, headers=headers)
            soup = BeautifulSoup(response.text, 'html.parser')
            
            articles = soup.find_all('article', class_='full-docsum')
            if not articles:
                break
                
            for art in articles:
                try:
                    # Extraer título y enlace
                    title_tag = art.find('a', class_='docsum-title')
                    title = clean_text(title_tag.text.strip())
                    link = "https://pubmed.ncbi.nlm.nih.gov" + title_tag['href']
                    
                    # Extraer revista y fecha
                    journal_info = art.find('span', class_='docsum-journal-citation').text.strip()
                    parts = journal_info.split('.')
                    revista = clean_text(parts[0])
                    fecha = clean_text(parts[1].strip().split(';')[0] if len(parts) > 1 else '')
                    
                    # Generar ID único
                    article_id = generate_article_id(title, revista, fecha)
                    
                    # Obtener abstract
                    time.sleep(1)  # Espera entre requests
                    art_response = session.get(link, headers=headers)
                    art_soup = BeautifulSoup(art_response.text, 'html.parser')
                    
                    abstract_section = art_soup.find('div', class_='abstract-content')
                    abstract = clean_text(abstract_section.text.strip()) if abstract_section else "No abstract available"
                    
                    articulos.append({
                        'id': article_id,
                        'title': title,
                        'journal': revista,
                        'date': fecha,
                        'abstract': abstract,
                        'scraped_date': datetime.now().strftime("%Y-%m-%d")
                    })
                    
                    print(f"Procesado: {title[:50]}...")
                    
                except Exception as e:
                    print(f"Error procesando artículo: {str(e)}")
                    continue
            
            # Verificar siguiente página
            next_btn = soup.find('button', class_='next-page-btn')
            if not next_btn or 'disabled' in next_btn.get('class', []):
                break
                
            page += 1
            print(f"Página {page} procesada")
    
    return articulos

def load_existing_articles(master_file='articulos.csv'):
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

def migrate_old_format(master_file='articulos.csv'):
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

def save_to_master(articles, master_file='articulos.csv'):
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
    # Obtener el rango de fechas según el día del mes
    start_date, end_date, period_name = get_date_range()
    
    print(f"Buscando artículos desde {start_date} hasta {end_date}")
    
    resultados = get_articles(start_date, end_date)
    
    # 1. Crear archivo específico del período
    period_filename = f"articulos_{period_name}.csv"
    with open(period_filename, 'w', newline='', encoding='utf-8-sig') as f:
        fieldnames = ['id', 'title', 'journal', 'date', 'abstract', 'scraped_date']
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(resultados)
    
    print(f"✓ Archivo del período guardado: {period_filename}")
    
    # 2. Agregar al archivo maestro
    new_count = save_to_master(resultados)
    
    print(f"✓ Se encontraron {len(resultados)} artículos en este período")
    print(f"✓ Se agregaron {new_count} artículos nuevos al archivo maestro")
    print(f"✓ Archivos guardados: {period_filename} y articulos.csv")
