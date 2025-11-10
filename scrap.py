import requests
from bs4 import BeautifulSoup
from datetime import datetime
import time
import csv
import re

def clean_text(text):
    """Limpia el texto de caracteres problemáticos"""
    if text:
        # Remover múltiples espacios y saltos de línea
        text = re.sub(r'\s+', ' ', text)
        # Remover caracteres problemáticos para CSV
        text = text.replace('\n', ' ').replace('\r', ' ')
        return text.strip()
    return ""

def get_articles():
    # Definir fecha fija de inicio (1 de septiembre del año actual)
    año_actual = datetime.now().year
    fecha_inicio = f"{año_actual}/09/01"
    hoy = datetime.now().strftime("%Y/%m/%d")
    
    # Construir URL con fechas dinámicas
    base_url = "https://pubmed.ncbi.nlm.nih.gov/"
    params = {
        'term': f'("International endodontic journal"[Journal] OR "Journal of endodontics"[Journal]) AND ("{fecha_inicio}"[Date - Entry] : "{hoy}"[Date - Entry])',
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
                    
                    # Obtener abstract
                    time.sleep(1)  # Espera entre requests
                    art_response = session.get(link, headers=headers)
                    art_soup = BeautifulSoup(art_response.text, 'html.parser')
                    
                    abstract_section = art_soup.find('div', class_='abstract-content')
                    abstract = clean_text(abstract_section.text.strip()) if abstract_section else "No abstract available"
                    
                    articulos.append({
                        'title': title,
                        'journal': revista,
                        'date': fecha,
                        'abstract': abstract
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

# Ejecutar y guardar resultados
if __name__ == "__main__":
    resultados = get_articles()
    
    # Guardar en CSV con encoding mejorado
    with open('articulos.csv', 'w', newline='', encoding='utf-8-sig') as f:
        writer = csv.DictWriter(f, fieldnames=['title', 'journal', 'date', 'abstract'])
        writer.writeheader()
        writer.writerows(resultados)
    
    print(f"Se encontraron {len(resultados)} artículos. Guardados en articulos.csv")
