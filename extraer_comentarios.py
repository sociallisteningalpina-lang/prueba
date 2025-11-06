import pandas as pd
from apify_client import ApifyClient
import time
import re
import logging
import html
import unicodedata
import os
import random # <-- MODIFICACIÓN: Se importa la librería para generar números aleatorios

# Configurar logging más limpio
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)

# --- PARÁMETROS DE CONFIGURACIÓN ---
APIFY_TOKEN = os.environ.get("APIFY_TOKEN")
SOLO_PRIMER_POST = False

# LISTA DE URLs A PROCESAR
URLS_A_PROCESAR = [
    # INSTAGRAM
    "https://www.instagram.com/p/DOL-s8yAEpK/",
    "https://www.instagram.com/p/DOL-vK2AEau/",
    "https://www.instagram.com/p/DN8-4jkgPZ9/",
    "https://www.instagram.com/p/DN8-jkkgLpv/",
    "https://www.instagram.com/p/DOL-utugPsM/",
    "https://www.instagram.com/p/DOL-s98gFfw/",
    "https://www.instagram.com/p/DOL_ShzgF8e/",
    "https://www.instagram.com/p/DOL-m7CgKdV/",
    "https://www.instagram.com/p/DOL-mwPgJuw/",
    "https://www.instagram.com/p/DOL-oBdgA01/",
    "https://www.instagram.com/p/DOL-n6OAOXo/",
    "https://www.instagram.com/p/DOL-m3ngF3N/",
    "https://www.instagram.com/p/DOL-ms9APs0/",
    
    # FACEBOOK - Demo/Ads
    "https://www.facebook.com/?feed_demo_ad=120233418145910432&h=AQCP9RxlmBjXz0LtaJAh",
    "https://www.facebook.com/?feed_demo_ad=120233417504070432&h=AQAQCH3Rl_lce0gF0_4",
    "https://www.facebook.com/?feed_demo_ad=120233417168840432&h=AQA4sGqS4BetjNxbaVU",
    "https://www.facebook.com/?feed_demo_ad=120233417074240432&h=AQB06AurVyvI00lu8K4",
    "https://www.facebook.com/?feed_demo_ad=120233054832200432&h=AQDDns3pft0Vh4Z-2iA",
    "https://www.facebook.com/?feed_demo_ad=120233063358500432&h=AQB2gbbCcn_X0UX-hOI",
    "https://www.facebook.com/?feed_demo_ad=120233055819280432&h=AQBgqCgBPhgHaSvBmjQ",
    "https://www.facebook.com/?feed_demo_ad=120233418582750432&h=AQA3C-BhfkEDKjRWGuc",
    "https://www.facebook.com/?feed_demo_ad=120233555370800432&h=AQBWtXEcnrVRvvpNVEk",
    "https://www.facebook.com/?feed_demo_ad=120233418238300432&h=AQCshb4xg4w3aZQNhso",
    "https://www.facebook.com/?feed_demo_ad=120233554870910432&h=AQBbgdd3MGvw9q6WCwY",
    "https://www.facebook.com/?feed_demo_ad=120233417504070432&h=AQAQCH3Rl_lce0gF7k0",
    "https://www.facebook.com/?feed_demo_ad=120233418145910432&h=AQCP9RxlmBjXz0LtaJA",
    
    # FACEBOOK - Posts
    "https://www.facebook.com/100064867445065/posts/pfbid02tU1gU4dpz5E4hdSJQbQfHHRWQeebhrGLvGDdRwCfEENUfiJM7LBKg2ZXL8xjh6Qdl?dco_ad_id=120233418145910432",
    "https://www.facebook.com/100064867445065/posts/pfbid09ESTvbRgHFXPvPnzu5Wuxc17mLHGzbvh3PXviVmWXTC63LtA7fwKWhoAmnoPCGLKl?dco_ad_id=120233417504070432",
    "https://www.facebook.com/100064867445065/posts/pfbid0vhyQ3E2hLKNx7xAhucBeXJMXJQRevYeXPBqbjv8aLadNxYp5XSnYdasEHXTmTf4gl?dco_ad_id=120233417168840432",
    "https://www.facebook.com/100064867445065/posts/pfbid02M6noi2G6CkPLtfRGQEzZw48B7i2pGmk2KrDTUixNn4inUUrxG47VFVVtsgjsVpGXl?dco_ad_id=120233417074240432",
    "https://www.facebook.com/100064867445065/posts/pfbid04uas4JSTf7CfNJ7vygqn7LEq2kaRS43o2TgqHWuttDDG6A6DnrCLjtP2MFScAZwbl?dco_ad_id=120233054832200432",
    "https://www.facebook.com/100064867445065/posts/pfbid0neLQsQFk4CkV8yBRRzcXEdBtA91Wt3C6RrHFz4QaUZevWDh1SzqUPCcWHiN31f3xl?dco_ad_id=120233063358500432",
    "https://www.facebook.com/100064867445065/posts/pfbid0KRNkHEJBgq8mga9zMUZCcxGF44ZFTkDCR79aecj6UMVc1guCGFs1nxvxVMecBu8hl?dco_ad_id=120233055819280432",
    "https://www.facebook.com/100064867445065/posts/pfbid02DMBD3zQxW4n6oSVNcbBR4CQZvuFtDVvrQTo7qBkFDnUXcCNzxzDK79rAk68C3kv5l?dco_ad_id=120233417504070432",
    
    # FACEBOOK - Videos
    "https://www.facebook.com/100064867445065/videos/759074660165987",
    "https://www.facebook.com/100064867445065/videos/646994368025589",
    "https://www.facebook.com/100064867445065/videos/1448503073025625",
    "https://www.facebook.com/100064867445065/videos/2033891010750740",
    "https://www.facebook.com/100064867445065/videos/1522847888884919",
    "https://www.facebook.com/100064867445065/videos/830303536353287",
    
    # TIKTOK
    "https://www.tiktok.com/@alpinacol/video/7542657663943838983?_r=1&_t=ZS-8zoRtzUAc5A",
    "https://www.tiktok.com/@alpinacol/video/7541457975207136530?_r=1&_t=ZS-8zoRifAKgbK",
    "https://www.tiktok.com/@alpinacol/video/7547743186593008916?_r=1&_t=ZS-8zoReoFAugR",
    "https://www.tiktok.com/@alpinacol/video/7548118828790664456?_r=1&_t=ZS-8zoRa8RYbDu",
    "https://www.tiktok.com/@alpinacol/video/7547746657144720660?_r=1&_t=ZS-8zoRVpEExWM",
    # NUEVOS 24/09/2025
    "https://www.tiktok.com/@Alpina/video/7548130480986639632?_r=1&_t=ZS-900eQOfqR8y",
    "https://www.tiktok.com/@Alpina/video/7548129116541685009?_r=1&_t=ZS-900eP8KESlm",
    "https://www.tiktok.com/@Alpina/video/7548117942119042322?_r=1&_t=ZS-900eGcSaFj5",
    "https://www.tiktok.com/@user360907502/video/7542610432733859080?_r=1&_t=ZS-900eDVc8Hkg",
    "https://www.tiktok.com/@Alpina/video/7548130532375186696?_r=1&_t=ZS-900eA9845DE",
    "https://www.tiktok.com/@Alpina/video/7548130486535589136?_r=1&_t=ZS-900e6AOgLwt"
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
            comment_data = {**campaign_info, 'post_url': url, 'post_number': post_number, 'platform': 'Facebook', 'author_name': self.fix_encoding(comment.get('authorName')), 'author_url': comment.get('authorUrl'), 'comment_text': self.fix_encoding(comment.get('text')), 'created_time': created_time, 'likes_count': comment.get('likesCount', 0), 'replies_count': comment.get('repliesCount', 0), 'is_reply': False, 'parent_comment_id': None, 'created_time_raw': str(comment)}
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
                comment_data = {**campaign_info, 'post_url': url, 'post_number': post_number, 'platform': 'Instagram', 'author_name': self.fix_encoding(author), 'author_url': f"https://instagram.com/{author}", 'comment_text': self.fix_encoding(comment.get('text')), 'created_time': created_time, 'likes_count': comment.get('likesCount', 0), 'replies_count': 0, 'is_reply': False, 'parent_comment_id': None, 'created_time_raw': str(comment)}
                processed.append(comment_data)
        logger.info(f"Processed {len(processed)} Instagram comments.")
        return processed

    def _process_tiktok_results(self, items, url, post_number, campaign_info):
        processed = []
        for comment in items:
            author_id = comment.get('user', {}).get('uniqueId', '')
            comment_data = {**campaign_info, 'post_url': url, 'post_number': post_number, 'platform': 'TikTok', 'author_name': self.fix_encoding(comment.get('user', {}).get('nickname')), 'author_url': f"https://www.tiktok.com/@{author_id}", 'comment_text': self.fix_encoding(comment.get('text')), 'created_time': comment.get('createTime'), 'likes_count': comment.get('diggCount', 0), 'replies_count': comment.get('replyCommentTotal', 0), 'is_reply': 'replyToId' in comment, 'parent_comment_id': comment.get('replyToId'), 'created_time_raw': str(comment)}
            processed.append(comment_data)
        logger.info(f"Processed {len(processed)} TikTok comments.")
        return processed

def save_to_excel(df, filename):
    try:
        with pd.ExcelWriter(filename, engine='openpyxl') as writer:
            df.to_excel(writer, sheet_name='Comentarios', index=False)
            if not df.empty and 'post_number' in df.columns:
                summary = df.groupby(['post_number', 'platform', 'post_url']).agg(Total_Comentarios=('comment_text', 'count'), Total_Likes=('likes_count', 'sum')).reset_index()
                summary.to_excel(writer, sheet_name='Resumen_Posts', index=False)
        logger.info(f"Excel file saved successfully: {filename}")
        return True
    except Exception as e:
        logger.error(f"Error saving Excel file: {e}")
        return False

def process_datetime_columns(df):
    if 'created_time' not in df.columns: return df
    logger.info("Processing datetime columns...")
    df['created_time_processed'] = pd.to_datetime(df['created_time'], errors='coerce', utc=True, unit='s')
    mask = df['created_time_processed'].isna()
    df.loc[mask, 'created_time_processed'] = pd.to_datetime(df.loc[mask, 'created_time'], errors='coerce', utc=True)
    if df['created_time_processed'].notna().any():
        df['created_time_processed'] = df['created_time_processed'].dt.tz_localize(None)
        df['fecha_comentario'] = df['created_time_processed'].dt.date
        df['hora_comentario'] = df['created_time_processed'].dt.time
    return df

def run_extraction():
    logger.info("--- STARTING COMMENT EXTRACTION PROCESS ---")
    if not APIFY_TOKEN:
        logger.error("APIFY_TOKEN not found in environment variables. Aborting.")
        return

    valid_urls = [url.strip() for url in URLS_A_PROCESAR if url.strip()]
    if not valid_urls:
        logger.warning("No valid URLs to process. Exiting.")
        return

    scraper = SocialMediaScraper(APIFY_TOKEN)
    all_comments = []
    post_counter = 0

    for url in valid_urls:
        post_counter += 1
        platform = scraper.detect_platform(url)
        comments = []
        if platform == 'facebook':
            comments = scraper.scrape_facebook_comments(url, campaign_info=CAMPAIGN_INFO, post_number=post_counter)
        elif platform == 'instagram':
            comments = scraper.scrape_instagram_comments(url, campaign_info=CAMPAIGN_INFO, post_number=post_counter)
        elif platform == 'tiktok':
            comments = scraper.scrape_tiktok_comments(url, campaign_info=CAMPAIGN_INFO, post_number=post_counter)
        else:
            logger.warning(f"Unknown platform for URL: {url}")
        
        all_comments.extend(comments)
        # --- MODIFICACIÓN CLAVE ---
        if not SOLO_PRIMER_POST and post_counter < len(valid_urls):
            # Pausa aleatoria entre 1 y 2 minutos para simular comportamiento humano
            pausa_aleatoria = random.uniform(60, 120) 
            logger.info(f"Pausing for {pausa_aleatoria:.2f} seconds to avoid detection...")
            time.sleep(pausa_aleatoria)
        # --- FIN DE LA MODIFICACIÓN ---

    if not all_comments:
        logger.warning("No comments were extracted. Process finished.")
        # Pequeña corrección: si no hay comentarios, no se debería intentar guardar un excel vacío
        # y dar un error, sino terminar limpiamente.
        return

    logger.info("--- PROCESSING FINAL RESULTS ---")
    df_comments = pd.DataFrame(all_comments)
    df_comments = process_datetime_columns(df_comments)
    
    final_columns = ['post_number', 'platform', 'campaign_name', 'post_url', 'author_name', 'comment_text', 'created_time_processed', 'fecha_comentario', 'hora_comentario', 'likes_count', 'replies_count', 'is_reply', 'author_url', 'created_time_raw']
    existing_cols = [col for col in final_columns if col in df_comments.columns]
    df_comments = df_comments[existing_cols]

    filename = "Comentarios Campaña.xlsx"
    save_to_excel(df_comments, filename)
    logger.info("--- EXTRACTION PROCESS FINISHED ---")

if __name__ == "__main__":
    run_extraction()

