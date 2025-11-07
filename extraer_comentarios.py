import pandas as pd
from apify_client import ApifyClient
import time
import re
import logging
import html
import unicodedata
import os
import random
from pathlib import Path

# Configurar logging más limpio
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)

# --- PARÁMETROS DE CONFIGURACIÓN ---
APIFY_TOKEN = os.environ.get("APIFY_TOKEN")
SOLO_PRIMER_POST = False

# LISTA DE URLs A PROCESAR
URLS_A_PROCESAR = [
    "https://www.facebook.com/reel/793063333529226",
    "https://www.instagram.com/p/DPpXWpHjMX2/",
    "https://www.instagram.com/p/DPpVC6UjLlp/",
    "https://www.instagram.com/p/DPzKNF0DIqm/",
]

# INFORMACIÓN DE CAMPAÑA
CAMPAIGN_INFO = {
    'campaign_name': 'CAMPAÑA_MANUAL_MULTIPLE',
    'campaign_id': 'MANUAL_002',
    'campaign_mes': 'Septiembre 2025',
    'campaign_marca': 'TU_MARCA',
    'campaign_referencia': 'REF_MANUAL',
    'campaign_objetivo': 'Análisis de Comentarios'
}

class SocialMediaScraper:
    def __init__(self, apify_token):
        self.client = ApifyClient(apify_token)

    def detect_platform(self, url):
        if pd.isna(url) or not url: return None
        url = str(url).lower()
        if any(d in url for d in ['facebook.com', 'fb.com']): return 'facebook'
        if 'instagram.com' in url: return 'instagram'
        if 'tiktok.com' in url: return 'tiktok'
        return None

    def clean_url(self, url):
        return str(url).split('?')[0] if '?' in str(url) else str(url)

    def fix_encoding(self, text):
        if pd.isna(text) or text == '': return ''
        try:
            text = str(text)
            text = html.unescape(text)
            text = unicodedata.normalize('NFKD', text)
            return text.strip()
        except Exception as e:
            logger.warning(f"Could not fix encoding: {e}")
            return str(text)

    def _wait_for_run_finish(self, run):
        logger.info("Scraper initiated, waiting for results...")
        max_wait_time = 300
        start_time = time.time()
        while True:
            run_status = self.client.run(run["id"]).get()
            if run_status["status"] in ["SUCCEEDED", "FAILED", "TIMED-OUT"]:
                return run_status
            if time.time() - start_time > max_wait_time:
                logger.error("Timeout reached while waiting for scraper.")
                return None
            time.sleep(10)

    def scrape_facebook_comments(self, url, max_comments=500, campaign_info=None, post_number=1):
        try:
            logger.info(f"Processing Facebook Post {post_number}: {url}")
            run_input = {"startUrls": [{"url": self.clean_url(url)}], "maxComments": max_comments}
            run = self.client.actor("apify/facebook-comments-scraper").call(run_input=run_input)
            run_status = self._wait_for_run_finish(run)
            if not run_status or run_status["status"] != "SUCCEEDED":
                logger.error(f"Facebook extraction failed. Status: {run_status.get('status', 'UNKNOWN')}")
                return []
            items = self.client.dataset(run["defaultDatasetId"]).list_items().items
            logger.info(f"Extraction complete: {len(items)} items found.")
            return self._process_facebook_results(items, url, post_number, campaign_info)
        except Exception as e:
            logger.error(f"Fatal error in scrape_facebook_comments: {e}")
            return []

    def scrape_instagram_comments(self, url, max_comments=500, campaign_info=None, post_number=1):
        try:
            logger.info(f"Processing Instagram Post {post_number}: {url}")
            run_input = {"directUrls": [url], "resultsType": "comments", "resultsLimit": max_comments}
            run = self.client.actor("apify/instagram-scraper").call(run_input=run_input)
            run_status = self._wait_for_run_finish(run)
            if not run_status or run_status["status"] != "SUCCEEDED":
                logger.error(f"Instagram extraction failed. Status: {run_status.get('status', 'UNKNOWN')}")
                return []
            items = self.client.dataset(run["defaultDatasetId"]).list_items().items
            logger.info(f"Extraction complete: {len(items)} items found.")
            return self._process_instagram_results(items, url, post_number, campaign_info)
        except Exception as e:
            logger.error(f"Fatal error in scrape_instagram_comments: {e}")
            return []

    def scrape_tiktok_comments(self, url, max_comments=500, campaign_info=None, post_number=1):
        try:
            logger.info(f"Processing TikTok Post {post_number}: {url}")
            run_input = {"postURLs": [self.clean_url(url)], "maxCommentsPerPost": max_comments}
            run = self.client.actor("clockworks/tiktok-comments-scraper").call(run_input=run_input)
            run_status = self._wait_for_run_finish(run)
            if not run_status or run_status["status"] != "SUCCEEDED":
                logger.error(f"TikTok extraction failed. Status: {run_status.get('status', 'UNKNOWN')}")
                return []
            items = self.client.dataset(run["defaultDatasetId"]).list_items().items
            logger.info(f"Extraction complete: {len(items)} comments found.")
            return self._process_tiktok_results(items, url, post_number, campaign_info)
        except Exception as e:
            logger.error(f"Fatal error in scrape_tiktok_comments: {e}")
            return []

    def _process_facebook_results(self, items, url, post_number, campaign_info):
        processed = []
        possible_date_fields = ['createdTime', 'timestamp', 'publishedTime', 'date', 'createdAt', 'publishedAt']
        for comment in items:
            created_time = None
            for field in possible_date_fields:
                if field in comment and comment[field]:
                    created_time = comment[field]
                    break
            comment_data = {
                **campaign_info,
                'post_url': url,
                'post_number': post_number,
                'platform': 'Facebook',
                'author_name': self.fix_encoding(comment.get('authorName')),
                'author_url': comment.get('authorUrl'),
                'comment_text': self.fix_encoding(comment.get('text')),
                'created_time': created_time,
                'likes_count': comment.get('likesCount', 0),
                'replies_count': comment.get('repliesCount', 0),
                'is_reply': False,
                'parent_comment_id': None,
                'created_time_raw': str(comment)
            }
            processed.append(comment_data)
        logger.info(f"Processed {len(processed)} Facebook comments.")
        return processed

    def _process_instagram_results(self, items, url, post_number, campaign_info):
        processed = []
        possible_date_fields = ['timestamp', 'createdTime', 'publishedAt', 'date', 'createdAt', 'taken_at']
        for item in items:
            comments_list = item.get('comments', [item]) if item.get('comments') is not None else [item]
            for comment in comments_list:
                created_time = None
                for field in possible_date_fields:
                    if field in comment and comment[field]:
                        created_time = comment[field]
                        break
                author = comment.get('ownerUsername', '')
                comment_data = {
                    **campaign_info,
                    'post_url': url,
                    'post_number': post_number,
                    'platform': 'Instagram',
                    'author_name': self.fix_encoding(author),
                    'author_url': f"https://instagram.com/{author}",
                    'comment_text': self.fix_encoding(comment.get('text')),
                    'created_time': created_time,
                    'likes_count': comment.get('likesCount', 0),
                    'replies_count': 0,
                    'is_reply': False,
                    'parent_comment_id': None,
                    'created_time_raw': str(comment)
                }
                processed.append(comment_data)
        logger.info(f"Processed {len(processed)} Instagram comments.")
        return processed

    def _process_tiktok_results(self, items, url, post_number, campaign_info):
        processed = []
        for comment in items:
            author_id = comment.get('user', {}).get('uniqueId', '')
            comment_data = {
                **campaign_info,
                'post_url': url,
                'post_number': post_number,
                'platform': 'TikTok',
                'author_name': self.fix_encoding(comment.get('user', {}).get('nickname')),
                'author_url': f"https://www.tiktok.com/@{author_id}",
                'comment_text': self.fix_encoding(comment.get('text')),
                'created_time': comment.get('createTime'),
                'likes_count': comment.get('diggCount', 0),
                'replies_count': comment.get('replyCommentTotal', 0),
                'is_reply': 'replyToId' in comment,
                'parent_comment_id': comment.get('replyToId'),
                'created_time_raw': str(comment)
            }
            processed.append(comment_data)
        logger.info(f"Processed {len(processed)} TikTok comments.")
        return processed


def create_post_registry_entry(url, platform, campaign_info):
    """
    Crea una entrada de registro para una pauta procesada sin comentarios.
    Esto asegura que todas las pautas se registren en el Excel, 
    incluso si no tienen comentarios en una ejecución específica.
    """
    return {
        **campaign_info,
        'post_url': url,
        'post_number': None,  # Se asignará después
        'platform': platform,
        'author_name': None,
        'author_url': None,
        'comment_text': None,
        'created_time': None,
        'likes_count': 0,
        'replies_count': 0,
        'is_reply': False,
        'parent_comment_id': None,
        'created_time_raw': None
    }


def assign_consistent_post_numbers(df):
    """
    Asigna números de pauta consistentes basados en el orden de primera aparición.
    
    IMPORTANTE: Preserva los post_numbers que ya existen en el DataFrame.
    Solo asigna números nuevos a URLs que no tenían número previamente.
    
    Esto mantiene los post_numbers estables entre ejecuciones.
    """
    if df.empty:
        return df
    
    # Paso 1: Identificar URLs que YA tienen un post_number asignado
    # Crear un mapeo de URL → post_number existente
    df_with_numbers = df[df['post_number'].notna()].copy()
    existing_mapping = {}
    
    if not df_with_numbers.empty:
        # Para cada URL, obtener su post_number más común (en caso de inconsistencias)
        for url in df_with_numbers['post_url'].unique():
            if pd.notna(url):
                # Obtener el post_number más frecuente para esta URL
                url_numbers = df_with_numbers[df_with_numbers['post_url'] == url]['post_number']
                most_common = url_numbers.mode()
                if len(most_common) > 0:
                    existing_mapping[url] = int(most_common.iloc[0])
    
    logger.info(f"Found {len(existing_mapping)} URLs with existing post_numbers")
    
    # Paso 2: Identificar URLs que NO tienen post_number
    all_urls = df['post_url'].dropna().unique()
    new_urls = [url for url in all_urls if url not in existing_mapping]
    
    logger.info(f"Found {len(new_urls)} URLs without post_numbers (will assign new numbers)")
    
    # Paso 3: Encontrar el siguiente número disponible
    if existing_mapping:
        next_number = max(existing_mapping.values()) + 1
    else:
        next_number = 1
    
    # Paso 4: Asignar números a URLs nuevas
    for url in new_urls:
        existing_mapping[url] = next_number
        logger.info(f"Assigning post_number {next_number} to new URL: {url[:50]}...")
        next_number += 1
    
    # Paso 5: Aplicar el mapeo completo al DataFrame
    df['post_number'] = df['post_url'].map(existing_mapping)
    
    return df


def load_existing_comments(filename):
    """
    Carga los comentarios existentes del archivo Excel.
    Retorna un DataFrame vacío si el archivo no existe.
    """
    if not Path(filename).exists():
        logger.info(f"No existing file found: {filename}. Will create new file.")
        return pd.DataFrame()
    
    try:
        df_existing = pd.read_excel(filename, sheet_name='Comentarios')
        logger.info(f"Loaded {len(df_existing)} existing comments from {filename}")
        return df_existing
    except Exception as e:
        logger.error(f"Error loading existing file: {e}")
        logger.info("Starting with empty DataFrame")
        return pd.DataFrame()


def create_comment_id(row):
    """
    Crea un identificador único para cada comentario basado en:
    - Plataforma
    - Autor
    - Texto del comentario
    - Fecha/hora (si está disponible)
    
    Para entradas de registro (pautas sin comentarios), usa solo la URL.
    Esto permite identificar duplicados incluso cuando el texto es igual.
    """
    # Caso especial: entrada de registro de pauta sin comentarios
    if 'comment_text' in row.index and pd.isna(row['comment_text']):
        # Para registros de pautas sin comentarios, usar solo la URL
        post_url = str(row['post_url']) if 'post_url' in row.index and pd.notna(row['post_url']) else ''
        return f"REGISTRY|{post_url}"
    
    # Normalizar valores None/NaN - usar notación de corchetes para Series/DataFrame
    platform = str(row['platform']) if 'platform' in row.index and pd.notna(row['platform']) else ''
    platform = platform.strip().lower()
    
    author = str(row['author_name']) if 'author_name' in row.index and pd.notna(row['author_name']) else ''
    author = author.strip().lower()
    
    text = str(row['comment_text']) if 'comment_text' in row.index and pd.notna(row['comment_text']) else ''
    text = text.strip().lower()
    
    # Para la fecha, intentamos usar created_time_processed primero, luego created_time
    date_str = ''
    if 'created_time_processed' in row.index and pd.notna(row['created_time_processed']):
        date_str = str(row['created_time_processed'])
    elif 'created_time' in row.index and pd.notna(row['created_time']):
        date_str = str(row['created_time'])
    
    # Crear un ID único concatenando los valores
    unique_id = f"{platform}|{author}|{text}|{date_str}"
    return unique_id


def merge_comments(df_existing, df_new):
    """
    Combina comentarios existentes con nuevos, evitando duplicados.
    
    Args:
        df_existing: DataFrame con comentarios existentes
        df_new: DataFrame con comentarios nuevos
    
    Returns:
        DataFrame combinado sin duplicados
    """
    if df_existing.empty:
        logger.info("No existing comments. All new comments will be added.")
        return df_new
    
    if df_new.empty:
        logger.info("No new comments to add.")
        return df_existing
    
    logger.info(f"Starting merge process...")
    logger.info(f"  - Existing comments: {len(df_existing)}")
    logger.info(f"  - New comments extracted: {len(df_new)}")
    
    # Crear IDs únicos para ambos DataFrames
    logger.info("Creating unique identifiers for existing comments...")
    df_existing['_comment_id'] = df_existing.apply(create_comment_id, axis=1)
    
    logger.info("Creating unique identifiers for new comments...")
    df_new['_comment_id'] = df_new.apply(create_comment_id, axis=1)
    
    # Debug: Mostrar algunos ejemplos de IDs generados
    logger.info(f"Sample existing IDs (first 3):")
    for i, id_val in enumerate(df_existing['_comment_id'].head(3)):
        logger.info(f"  {i+1}. {id_val}")
    
    logger.info(f"Sample new IDs (first 3):")
    for i, id_val in enumerate(df_new['_comment_id'].head(3)):
        logger.info(f"  {i+1}. {id_val}")
    
    # Identificar comentarios duplicados
    existing_ids = set(df_existing['_comment_id'])
    new_ids = set(df_new['_comment_id'])
    
    duplicate_ids = existing_ids.intersection(new_ids)
    unique_new_ids = new_ids - existing_ids
    
    logger.info(f"Duplicate detection results:")
    logger.info(f"  - Duplicates found: {len(duplicate_ids)}")
    logger.info(f"  - Unique new comments: {len(unique_new_ids)}")
    
    # Filtrar solo comentarios nuevos
    df_truly_new = df_new[df_new['_comment_id'].isin(unique_new_ids)].copy()
    
    # Combinar existentes con nuevos
    df_combined = pd.concat([df_existing, df_truly_new], ignore_index=True)
    
    # Eliminar la columna temporal de ID
    df_combined = df_combined.drop(columns=['_comment_id'])
    
    logger.info(f"Merge complete. Total comments: {len(df_combined)}")
    return df_combined


def save_to_excel(df, filename):
    """
    Guarda el DataFrame en Excel con dos hojas:
    1. Comentarios: Todos los comentarios
    2. Resumen_Posts: Resumen agrupado por post
    """
    try:
        with pd.ExcelWriter(filename, engine='openpyxl') as writer:
            df.to_excel(writer, sheet_name='Comentarios', index=False)
            
            if not df.empty and 'post_number' in df.columns:
                summary = df.groupby(['post_number', 'platform', 'post_url']).agg(
                    Total_Comentarios=('comment_text', 'count'),
                    Total_Likes=('likes_count', 'sum')
                ).reset_index()
                summary.to_excel(writer, sheet_name='Resumen_Posts', index=False)
        
        logger.info(f"Excel file saved successfully: {filename}")
        return True
    except Exception as e:
        logger.error(f"Error saving Excel file: {e}")
        return False


def process_datetime_columns(df):
    """
    Procesa las columnas de fecha/hora para crear campos adicionales útiles.
    """
    if 'created_time' not in df.columns:
        return df
    
    logger.info("Processing datetime columns...")
    
    # Intentar convertir created_time a datetime
    df['created_time_processed'] = pd.to_datetime(
        df['created_time'], 
        errors='coerce', 
        utc=True, 
        unit='s'
    )
    
    # Si falló la conversión con unit='s', intentar sin unit
    mask = df['created_time_processed'].isna()
    df.loc[mask, 'created_time_processed'] = pd.to_datetime(
        df.loc[mask, 'created_time'], 
        errors='coerce', 
        utc=True
    )
    
    # Crear columnas de fecha y hora si hay valores válidos
    if df['created_time_processed'].notna().any():
        df['created_time_processed'] = df['created_time_processed'].dt.tz_localize(None)
        df['fecha_comentario'] = df['created_time_processed'].dt.date
        df['hora_comentario'] = df['created_time_processed'].dt.time
    
    return df


def run_extraction():
    """
    Función principal que ejecuta todo el proceso de extracción.
    Ahora con lógica de append en lugar de sobrescritura.
    """
    logger.info("=" * 60)
    logger.info("--- STARTING COMMENT EXTRACTION PROCESS ---")
    logger.info("=" * 60)
    
    if not APIFY_TOKEN:
        logger.error("APIFY_TOKEN not found in environment variables. Aborting.")
        return

    valid_urls = [url.strip() for url in URLS_A_PROCESAR if url.strip()]
    logger.info(f"URLs to process: {len(valid_urls)}")
    
    if not valid_urls:
        logger.warning("No valid URLs to process. Exiting.")
        logger.warning("Please add URLs to the URLS_A_PROCESAR list.")
        return

    filename = "Comentarios Campaña.xlsx"
    logger.info(f"Output file: {filename}")
    
    # --- NUEVA LÓGICA: Cargar comentarios existentes ---
    df_existing = load_existing_comments(filename)
    
    # Extraer nuevos comentarios
    scraper = SocialMediaScraper(APIFY_TOKEN)
    all_comments = []
    processed_urls_info = []  # Registro de todas las URLs procesadas
    post_counter = 0

    for url in valid_urls:
        post_counter += 1
        platform = scraper.detect_platform(url)
        
        # Registrar esta URL como procesada
        processed_urls_info.append({
            'url': url,
            'platform': platform,
            'temp_post_number': post_counter
        })
        
        comments = []
        
        if platform == 'facebook':
            comments = scraper.scrape_facebook_comments(
                url, 
                campaign_info=CAMPAIGN_INFO, 
                post_number=post_counter
            )
        elif platform == 'instagram':
            comments = scraper.scrape_instagram_comments(
                url, 
                campaign_info=CAMPAIGN_INFO, 
                post_number=post_counter
            )
        elif platform == 'tiktok':
            comments = scraper.scrape_tiktok_comments(
                url, 
                campaign_info=CAMPAIGN_INFO, 
                post_number=post_counter
            )
        else:
            logger.warning(f"Unknown platform for URL: {url}")
        
        # Si no hay comentarios, crear entrada de registro
        if not comments:
            logger.info(f"No comments found for {url}. Creating registry entry.")
            registry_entry = create_post_registry_entry(url, platform, CAMPAIGN_INFO)
            registry_entry['post_number'] = post_counter
            all_comments.append(registry_entry)
        else:
            all_comments.extend(comments)
        
        # Pausa aleatoria entre posts
        if not SOLO_PRIMER_POST and post_counter < len(valid_urls):
            pausa_aleatoria = random.uniform(60, 120) 
            logger.info(f"Pausing for {pausa_aleatoria:.2f} seconds to avoid detection...")
            time.sleep(pausa_aleatoria)

    logger.info(f"Processed {len(valid_urls)} URLs with {len([c for c in all_comments if c.get('comment_text')])} actual comments")

    if not all_comments:
        logger.warning("No comments or URLs were processed in this run.")
        # Si ya hay comentarios existentes, mantenerlos y guardar el archivo
        if not df_existing.empty:
            logger.info(f"Keeping {len(df_existing)} existing comments in file.")
            save_to_excel(df_existing, filename)
            logger.info("--- EXTRACTION PROCESS FINISHED ---")
            logger.info(f"Total comments in file: {len(df_existing)}")
            return
        else:
            logger.info("No new or existing comments. Creating empty file for future use.")
            # Crear un DataFrame vacío con las columnas correctas
            empty_df = pd.DataFrame(columns=[
                'post_number', 'platform', 'campaign_name', 'post_url', 
                'author_name', 'comment_text', 'created_time_processed', 
                'fecha_comentario', 'hora_comentario', 'likes_count', 
                'replies_count', 'is_reply', 'author_url', 'created_time_raw'
            ])
            save_to_excel(empty_df, filename)
            logger.info("Empty Excel file created. Process finished.")
            return

    logger.info("--- PROCESSING FINAL RESULTS ---")
    
    # Procesar nuevos comentarios
    df_new_comments = pd.DataFrame(all_comments)
    df_new_comments = process_datetime_columns(df_new_comments)
    
    # --- NUEVA LÓGICA: Merge con comentarios existentes ---
    df_combined = merge_comments(df_existing, df_new_comments)
    
    # --- IMPORTANTE: Reasignar post_numbers de forma consistente ---
    # Esto asegura que cada URL única tenga siempre el mismo post_number
    df_combined = assign_consistent_post_numbers(df_combined)
    
    # Ordenar por fecha de comentario (más recientes primero), pero manteniendo registros sin comentario al final
    df_with_comments = df_combined[df_combined['comment_text'].notna()].copy()
    df_without_comments = df_combined[df_combined['comment_text'].isna()].copy()
    
    if not df_with_comments.empty and 'created_time_processed' in df_with_comments.columns:
        df_with_comments = df_with_comments.sort_values(
            'created_time_processed', 
            ascending=False
        )
    
    # Combinar: primero comentarios, luego registros de pautas sin comentarios
    df_combined = pd.concat([df_with_comments, df_without_comments], ignore_index=True)
    
    # Organizar columnas
    final_columns = [
        'post_number', 'platform', 'campaign_name', 'post_url', 
        'author_name', 'comment_text', 'created_time_processed', 
        'fecha_comentario', 'hora_comentario', 'likes_count', 
        'replies_count', 'is_reply', 'author_url', 'created_time_raw'
    ]
    existing_cols = [col for col in final_columns if col in df_combined.columns]
    df_combined = df_combined[existing_cols]

    # Guardar archivo actualizado
    save_to_excel(df_combined, filename)
    
    # Calcular estadísticas finales
    total_rows = len(df_combined)
    total_comments = df_combined['comment_text'].notna().sum()
    total_posts = df_combined['post_url'].nunique()
    
    logger.info("=" * 60)
    logger.info("--- EXTRACTION PROCESS FINISHED ---")
    logger.info(f"Total unique posts/pautas tracked: {total_posts}")
    logger.info(f"Total comments in file: {total_comments}")
    logger.info(f"Total rows in file: {total_rows} (includes {total_rows - total_comments} post registry entries)")
    logger.info(f"File saved: {filename}")
    logger.info("=" * 60)


if __name__ == "__main__":
    run_extraction()




