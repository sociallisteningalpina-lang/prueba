#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Script de Extracción de Comentarios de Redes Sociales
VERSIÓN ESTABLE - Solo fixes esenciales
"""

import pandas as pd
from apify_client import ApifyClient
import time
import logging
import html
import unicodedata
import os
import json
import random
from pathlib import Path
from datetime import datetime
import hashlib
from typing import List, Dict, Optional, Tuple

# ============================================================================
# CONFIGURACIÓN DE LOGGING
# ============================================================================
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ============================================================================
# CONSTANTES GLOBALES
# ============================================================================
APIFY_TOKEN = os.environ.get("APIFY_TOKEN")
CONFIG_DIR = Path(__file__).parent / "config"


# ============================================================================
# FUNCIONES DE CARGA DE CONFIGURACIÓN
# ============================================================================

def load_json_config(filename: str) -> dict:
    """Carga un archivo de configuración JSON"""
    config_path = CONFIG_DIR / filename
    try:
        with open(config_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except FileNotFoundError:
        logger.error(f"Config file not found: {config_path}")
        raise
    except json.JSONDecodeError as e:
        logger.error(f"Invalid JSON in {config_path}: {e}")
        raise


def load_urls_from_file(filename: str = "urls.txt") -> List[str]:
    """Carga URLs desde un archivo de texto."""
    urls_path = CONFIG_DIR / filename
    try:
        with open(urls_path, 'r', encoding='utf-8') as f:
            urls = []
            for line in f:
                line = line.strip()
                if line and not line.startswith('#'):
                    urls.append(line)
            logger.info(f"Loaded {len(urls)} URLs from {urls_path}")
            return urls
    except FileNotFoundError:
        logger.error(f"URLs file not found: {urls_path}")
        raise


# ============================================================================
# FUNCIONES DE VALIDACIÓN
# ============================================================================

def validate_url(url: str) -> bool:
    """Valida que la URL no sea genérica o vacía."""
    if not url or pd.isna(url):
        return False
    
    url = str(url).strip()
    
    invalid_urls = [
        'https://www.facebook.com/',
        'https://www.facebook.com',
        'https://facebook.com/',
        'https://facebook.com',
        'https://instagram.com/',
        'https://www.instagram.com/',
        'https://tiktok.com/',
        'https://www.tiktok.com/'
    ]
    
    if url in invalid_urls:
        return False
    
    if len(url) < 30:
        return False
    
    return True


def validate_comment_data(comment: dict) -> Tuple[bool, Optional[str]]:
    """Valida que un comentario tenga los campos mínimos requeridos."""
    required_fields = ['platform', 'post_url', 'comment_text']
    
    for field in required_fields:
        if field not in comment:
            return False, f"Missing required field: {field}"
        if pd.isna(comment[field]) or str(comment[field]).strip() == '':
            return False, f"Empty required field: {field}"
    
    return True, None


# ============================================================================
# CLASE PRINCIPAL DE SCRAPING
# ============================================================================

class SocialMediaScraper:
    """Clase para extraer comentarios de redes sociales usando Apify APIs."""
    
    def __init__(self, apify_token: str, settings: dict):
        self.client = ApifyClient(apify_token)
        self.settings = settings
        self.failed_urls = []
        self.extraction_stats = {
            'total_attempts': 0,
            'successful': 0,
            'failed': 0,
            'no_comments': 0,
            'invalid_comments': 0
        }

    def detect_platform(self, url: str) -> Optional[str]:
        """Detecta la plataforma de la URL."""
        if pd.isna(url) or not url:
            return None
        
        url = str(url).lower()
        
        if any(d in url for d in ['facebook.com', 'fb.com', 'fb.me']):
            return 'Facebook'
        if 'instagram.com' in url:
            return 'Instagram'
        if 'tiktok.com' in url or 'vt.tiktok.com' in url:
            return 'TikTok'
        
        return None

    def clean_url(self, url: str) -> str:
        """Limpia parámetros de query de la URL"""
        return str(url).split('?')[0] if '?' in str(url) else str(url)

    def fix_encoding(self, text: str) -> str:
        """
        Normaliza y limpia el encoding del texto.
        ✅ FIX: Usa NFC para preservar emojis
        """
        if pd.isna(text) or text == '':
            return ''
        
        try:
            text = str(text)
            text = html.unescape(text)
            # ✅ CAMBIO CRÍTICO: NFC en vez de NFKD
            text = unicodedata.normalize('NFC', text)
            return text.strip()
        except Exception as e:
            logger.warning(f"Could not fix encoding: {e}")
            return str(text)

    def _wait_for_run_finish(self, run: dict) -> Optional[dict]:
        """Espera a que termine la ejecución del scraper de Apify."""
        logger.info("Scraper initiated, waiting for results...")
        max_wait_time = 600  # 10 minutos
        start_time = time.time()
        
        while True:
            run_status = self.client.run(run["id"]).get()
            
            if run_status["status"] in ["SUCCEEDED", "FAILED", "TIMED-OUT"]:
                return run_status
            
            if time.time() - start_time > max_wait_time:
                logger.error("Timeout reached while waiting for scraper.")
                return None
            
            time.sleep(10)

    def _deduplicate_items(self, items: List[dict], platform: str) -> List[dict]:
        """Elimina duplicados de los items devueltos por Apify."""
        if not items:
            return items
    
        seen_hashes = set()
        unique_items = []
        duplicates_found = 0
    
        for item in items:
            if platform == 'Facebook':
                text = str(item.get('text', ''))
                date = str(item.get('date', item.get('createdTime', '')))
                unique_key = f"{text}|{date}"
        
            elif platform == 'Instagram':
                text = str(item.get('text', ''))
                timestamp = str(item.get('timestamp', item.get('createdTime', '')))
                unique_key = f"{text}|{timestamp}"
        
            elif platform == 'TikTok':
                cid = item.get('cid')
                if cid:
                    unique_key = f"cid_{cid}"
                else:
                    text = str(item.get('text', ''))
                    create_time = str(item.get('createTime', ''))
                    unique_key = f"{text}|{create_time}"
        
            else:
                text = str(item.get('text', ''))
                unique_key = text
        
            item_hash = hashlib.md5(unique_key.encode('utf-8')).hexdigest()
        
            if item_hash not in seen_hashes:
                seen_hashes.add(item_hash)
                unique_items.append(item)
            else:
                duplicates_found += 1
    
        if duplicates_found > 0:
            logger.warning(f"⚠️  Removed {duplicates_found} duplicate items from Apify response")
    
        return unique_items

    def scrape_with_retry(
        self, 
        scrape_function, 
        url: str, 
        max_comments: int, 
        campaign_info: dict, 
        post_number: int
    ) -> List[dict]:
        """Ejecuta una función de scraping con reintentos automáticos."""
        max_retries = self.settings.get('max_retries', 3)
        self.extraction_stats['total_attempts'] += 1
        
        for attempt in range(max_retries):
            try:
                result = scrape_function(url, max_comments, campaign_info, post_number)
                
                if result:
                    valid_comments = []
                    for comment in result:
                        is_valid, error_msg = validate_comment_data(comment)
                        if is_valid:
                            valid_comments.append(comment)
                        else:
                            logger.warning(f"Invalid comment data: {error_msg}")
                            self.extraction_stats['invalid_comments'] += 1
                    
                    if valid_comments:
                        self.extraction_stats['successful'] += 1
                        return valid_comments
                    else:
                        logger.warning(f"All comments from {url} failed validation")
                
                if attempt < max_retries - 1:
                    wait_time = (attempt + 1) * 30
                    logger.warning(
                        f"Attempt {attempt + 1}/{max_retries} failed. "
                        f"Waiting {wait_time} seconds before retry..."
                    )
                    time.sleep(wait_time)
                    
            except Exception as e:
                logger.error(f"Attempt {attempt + 1}/{max_retries} failed with error: {e}")
                if attempt < max_retries - 1:
                    wait_time = (attempt + 1) * 30
                    time.sleep(wait_time)
        
        self.failed_urls.append(url)
        self.extraction_stats['failed'] += 1
        logger.error(f"All {max_retries} attempts failed for URL: {url}")
        return []

    def scrape_facebook_comments(
        self, 
        url: str, 
        max_comments: int = 1500, 
        campaign_info: dict = None, 
        post_number: int = 1
    ) -> List[dict]:
        """Extrae comentarios de Facebook con replies."""
        try:
            logger.info(f"Processing Facebook Post {post_number}: {url}")
            
            max_replies = self.settings.get('max_replies_per_comment', 100)
        
            run_input = {
                "startUrls": [{"url": self.clean_url(url)}],
                "maxComments": max_comments,
                "maxPostComments": max_comments,
                "commentsMode": "RANKED_UNFILTERED",
                "scrapeReplies": True,
                "maxReplies": max_replies  # ✅ FIX: era maxCommentReplies
            }
        
            run = self.client.actor("apify/facebook-comments-scraper").call(
                run_input=run_input
            )
            run_status = self._wait_for_run_finish(run)
        
            if not run_status or run_status["status"] != "SUCCEEDED":
                logger.error(
                    f"Facebook extraction failed. Status: {run_status.get('status', 'UNKNOWN')}"
                )
                return []
        
            dataset = self.client.dataset(run["defaultDatasetId"])
            
            # ✅ FIX: Límite dinámico
            total_count = dataset.get().get('itemCount', 0)
            items_response = dataset.list_items(clean=True, limit=total_count)
            items = items_response.items
        
            logger.info(f"Extraction complete: {len(items)} items found.")
        
            items = self._deduplicate_items(items, platform='Facebook')
            logger.info(f"After deduplication: {len(items)} unique items.")
        
            return self._process_facebook_results(items, url, post_number, campaign_info)
        
        except Exception as e:
            logger.error(f"Error in scrape_facebook_comments: {e}")
            raise

    def scrape_instagram_comments(
        self, 
        url: str, 
        max_comments: int = 1500,
        campaign_info: dict = None, 
        post_number: int = 1
    ) -> List[dict]:
        """Extrae comentarios de Instagram."""
        try:
            logger.info(f"Processing Instagram Post {post_number}: {url}")
        
            run_input = {
                "directUrls": [url],
                "resultsType": "comments",
                "resultsLimit": max_comments,
                "addParentData": False
            }
        
            # ✅ FIX: Usar instagram-scraper (NO instagram-comment-scraper)
            run = self.client.actor("apify/instagram-scraper").call(
                run_input=run_input
            )
            run_status = self._wait_for_run_finish(run)
        
            if not run_status or run_status["status"] != "SUCCEEDED":
                logger.error(
                    f"Instagram extraction failed. Status: {run_status.get('status', 'UNKNOWN')}"
                )
                return []
        
            dataset = self.client.dataset(run["defaultDatasetId"])
            
            # ✅ FIX: Límite dinámico
            total_count = dataset.get().get('itemCount', 0)
            items_response = dataset.list_items(clean=True, limit=total_count)
            items = items_response.items
        
            logger.info(f"Extraction complete: {len(items)} items found.")
        
            items = self._deduplicate_items(items, platform='Instagram')
            logger.info(f"After deduplication: {len(items)} unique items.")
        
            return self._process_instagram_results(items, url, post_number, campaign_info)
        
        except Exception as e:
            logger.error(f"Error in scrape_instagram_comments: {e}")
            raise

    def scrape_tiktok_comments(
        self, 
        url: str, 
        max_comments: int = 1500,
        campaign_info: dict = None, 
        post_number: int = 1
    ) -> List[dict]:
        """Extrae comentarios de TikTok con replies."""
        try:
            logger.info(f"Processing TikTok Post {post_number}: {url}")
            
            max_replies = self.settings.get('max_replies_per_comment', 100)
        
            run_input = {
                "postURLs": [self.clean_url(url)],
                "maxCommentsPerPost": max_comments,
                "commentsPerPost": max_comments,
                "maxRepliesPerComment": max_replies,
            }
        
            run = self.client.actor("clockworks/tiktok-comments-scraper").call(
                run_input=run_input
            )
            run_status = self._wait_for_run_finish(run)
        
            if not run_status or run_status["status"] != "SUCCEEDED":
                logger.error(
                    f"TikTok extraction failed. Status: {run_status.get('status', 'UNKNOWN')}"
                )
                return []
        
            dataset = self.client.dataset(run["defaultDatasetId"])
            
            # ✅ FIX: Límite dinámico
            total_count = dataset.get().get('itemCount', 0)
            items_response = dataset.list_items(clean=True, limit=total_count)
            items = items_response.items
        
            logger.info(f"Extraction complete: {len(items)} comments found.")
        
            items = self._deduplicate_items(items, platform='TikTok')
            logger.info(f"After deduplication: {len(items)} unique items.")
        
            return self._process_tiktok_results(items, url, post_number, campaign_info)
        
        except Exception as e:
            logger.error(f"Error in scrape_tiktok_comments: {e}")
            raise

    def _process_facebook_results(
        self, 
        items: List[dict], 
        url: str, 
        post_number: int, 
        campaign_info: dict
    ) -> List[dict]:
        """Procesa los resultados extraídos de Facebook"""
        processed = []
        possible_date_fields = [
            'createdTime', 'timestamp', 'publishedTime', 
            'date', 'createdAt', 'publishedAt'
        ]
        
        for comment in items:
            created_time = None
            for field in possible_date_fields:
                if field in comment and comment[field]:
                    created_time = comment[field]
                    break
            
            comment_data = {
                **campaign_info,
                'post_url': url,
                'post_url_original': url,
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
                'created_time_raw': str(comment)[:500]
            }
            processed.append(comment_data)
        
        logger.info(f"Processed {len(processed)} Facebook comments.")
        return processed

    def _process_instagram_results(
        self, 
        items: List[dict], 
        url: str, 
        post_number: int, 
        campaign_info: dict
    ) -> List[dict]:
        """Procesa los resultados extraídos de Instagram incluyendo replies."""
        processed = []
        possible_date_fields = [
            'timestamp', 'createdTime', 'publishedAt', 
            'date', 'createdAt', 'taken_at'
        ]
        
        for item in items:
            comments_list = (
                item.get('comments', [item]) 
                if item.get('comments') is not None 
                else [item]
            )
            
            for comment in comments_list:
                created_time = None
                for field in possible_date_fields:
                    if field in comment and comment[field]:
                        created_time = comment[field]
                        break
                
                author = comment.get('ownerUsername', '')
                
                is_reply = bool(comment.get('parentCommentId') or comment.get('replyToId'))
                parent_id = comment.get('parentCommentId') or comment.get('replyToId')
                
                comment_data = {
                    **campaign_info,
                    'post_url': url,
                    'post_url_original': url,
                    'post_number': post_number,
                    'platform': 'Instagram',
                    'author_name': self.fix_encoding(author),
                    'author_url': f"https://instagram.com/{author}" if author else None,
                    'comment_text': self.fix_encoding(comment.get('text')),
                    'created_time': created_time,
                    'likes_count': comment.get('likesCount', 0),
                    'replies_count': comment.get('repliesCount', 0),
                    'is_reply': is_reply,
                    'parent_comment_id': parent_id,
                    'created_time_raw': str(comment)[:500]
                }
                processed.append(comment_data)
                
                # Procesar replies si existen
                if 'replies' in comment and isinstance(comment['replies'], list):
                    for reply in comment['replies']:
                        reply_author = reply.get('ownerUsername', '')
                        reply_time = None
                        for field in possible_date_fields:
                            if field in reply and reply[field]:
                                reply_time = reply[field]
                                break
                        
                        reply_data = {
                            **campaign_info,
                            'post_url': url,
                            'post_url_original': url,
                            'post_number': post_number,
                            'platform': 'Instagram',
                            'author_name': self.fix_encoding(reply_author),
                            'author_url': f"https://instagram.com/{reply_author}" if reply_author else None,
                            'comment_text': self.fix_encoding(reply.get('text')),
                            'created_time': reply_time,
                            'likes_count': reply.get('likesCount', 0),
                            'replies_count': 0,
                            'is_reply': True,
                            'parent_comment_id': comment.get('id') or comment.get('pk'),
                            'created_time_raw': str(reply)[:500]
                        }
                        processed.append(reply_data)
        
        logger.info(f"Processed {len(processed)} Instagram comments (including replies).")
        return processed

    def _process_tiktok_results(
        self, 
        items: List[dict], 
        url: str, 
        post_number: int, 
        campaign_info: dict
    ) -> List[dict]:
        """Procesa los resultados extraídos de TikTok"""
        processed = []
        
        for comment in items:
            author_id = comment.get('user', {}).get('uniqueId', '')
            comment_data = {
                **campaign_info,
                'post_url': url,
                'post_url_original': url,
                'post_number': post_number,
                'platform': 'TikTok',
                'author_name': self.fix_encoding(
                    comment.get('user', {}).get('nickname')
                ),
                'author_url': f"https://www.tiktok.com/@{author_id}",
                'comment_text': self.fix_encoding(comment.get('text')),
                'created_time': comment.get('createTime'),
                'likes_count': comment.get('diggCount', 0),
                'replies_count': comment.get('replyCommentTotal', 0),
                'is_reply': 'replyToId' in comment,
                'parent_comment_id': comment.get('replyToId'),
                'created_time_raw': str(comment)[:500]
            }
            processed.append(comment_data)
        
        logger.info(f"Processed {len(processed)} TikTok comments.")
        return processed

    def get_stats_summary(self) -> dict:
        """Retorna resumen de estadísticas de extracción"""
        return self.extraction_stats.copy()


# ============================================================================
# FUNCIONES DE PROCESAMIENTO DE DATOS
# ============================================================================

def create_post_registry_entry(
    url: str, 
    platform: str, 
    campaign_info: dict, 
    post_number: int
) -> dict:
    """Crea una entrada de registro para una pauta procesada sin comentarios."""
    return {
        **campaign_info,
        'post_url': url,
        'post_url_original': url,
        'post_number': post_number,
        'platform': platform,
        'author_name': None,
        'author_url': None,
        'comment_text': None,
        'created_time': None,
        'likes_count': 0,
        'replies_count': 0,
        'is_reply': False,
        'parent_comment_id': None,
        'created_time_raw': None,
        'extraction_status': 'NO_COMMENTS'
    }


def create_failed_registry_entry(
    url: str, 
    platform: str, 
    campaign_info: dict, 
    post_number: int
) -> dict:
    """Crea una entrada de registro para una URL que falló la extracción."""
    return {
        **campaign_info,
        'post_url': url,
        'post_url_original': url,
        'post_number': post_number,
        'platform': platform,
        'author_name': None,
        'author_url': None,
        'comment_text': None,
        'created_time': None,
        'likes_count': 0,
        'replies_count': 0,
        'is_reply': False,
        'parent_comment_id': None,
        'created_time_raw': None,
        'extraction_status': 'FAILED'
    }


def normalize_timestamp_for_hash(timestamp_value) -> str:
    """Normaliza un timestamp a formato estándar para el hash."""
    if pd.isna(timestamp_value) or timestamp_value is None or timestamp_value == '':
        return 'UNKNOWN'
    
    try:
        if isinstance(timestamp_value, (int, float)):
            return str(int(timestamp_value))
        
        if isinstance(timestamp_value, str) and timestamp_value.isdigit():
            return timestamp_value
        
        if isinstance(timestamp_value, (pd.Timestamp, datetime)):
            return str(int(timestamp_value.timestamp()))
        
        dt = pd.to_datetime(timestamp_value, errors='coerce')
        if pd.notna(dt):
            return str(int(dt.timestamp()))
        
        return str(timestamp_value)
        
    except Exception as e:
        logger.warning(f"Could not normalize timestamp {timestamp_value}: {e}")
        return 'UNKNOWN'


def create_unique_comment_hash(row: pd.Series) -> str:
    """Crea un hash único para cada comentario basado en campos CONFIABLES."""
    platform = str(row.get('platform', '')).strip().lower()
    
    comment_text = row.get('comment_text', '')
    if pd.isna(comment_text) or str(comment_text).strip() == '':
        post_url = str(row.get('post_url', '')).strip()
        extraction_status = str(row.get('extraction_status', 'UNKNOWN'))
        return f"REGISTRY_{platform}_{extraction_status}_{hashlib.md5(post_url.encode('utf-8')).hexdigest()}"
    
    post_url = str(row.get('post_url', '')).strip()
    comment_text_clean = str(comment_text).strip()
    created_time_normalized = normalize_timestamp_for_hash(row.get('created_time'))
    
    unique_string = (
        f"{platform}|{post_url}|"
        f"{comment_text_clean}|{created_time_normalized}"
    )
    
    return hashlib.md5(unique_string.encode('utf-8')).hexdigest()


def normalize_existing_data(df: pd.DataFrame) -> pd.DataFrame:
    """Normaliza los datos existentes para asegurar consistencia."""
    if df.empty:
        return df
    
    df = df.copy()
    
    if 'platform' in df.columns:
        platform_mapping = {
            'facebook': 'Facebook',
            'instagram': 'Instagram',
            'tiktok': 'TikTok',
            'Facebook': 'Facebook',
            'Instagram': 'Instagram',
            'TikTok': 'TikTok'
        }
        df['platform'] = df['platform'].apply(
            lambda x: platform_mapping.get(str(x).strip().lower(), str(x)) 
            if pd.notna(x) else x
        )
    
    if 'comment_text' in df.columns:
        df['comment_text'] = df['comment_text'].replace('', pd.NA)
        df['comment_text'] = df['comment_text'].apply(
            lambda x: pd.NA if isinstance(x, str) and x.strip() == '' else x
        )
    
    if 'extraction_status' not in df.columns:
        df['extraction_status'] = df.apply(
            lambda row: 'NO_COMMENTS' if pd.isna(row.get('comment_text')) else None,
            axis=1
        )
    
    logger.info(f"Normalized {len(df)} existing rows")
    return df


def merge_comments(
    df_existing: pd.DataFrame, 
    df_new: pd.DataFrame
) -> pd.DataFrame:
    """Combina comentarios existentes con nuevos, evitando duplicados."""
    if df_existing.empty:
        return df_new
    if df_new.empty:
        return df_existing
    
    logger.info(f"Merging: {len(df_existing)} existing + {len(df_new)} new rows")
    
    df_existing = normalize_existing_data(df_existing)
    
    logger.info("Creating hashes for existing data...")
    df_existing['_comment_hash'] = df_existing.apply(
        create_unique_comment_hash, axis=1
    )
    
    logger.info("Creating hashes for new data...")
    df_new['_comment_hash'] = df_new.apply(
        create_unique_comment_hash, axis=1
    )
    
    existing_hashes = set(df_existing['_comment_hash'])
    df_truly_new = df_new[~df_new['_comment_hash'].isin(existing_hashes)].copy()
    
    duplicates_filtered = len(df_new) - len(df_truly_new)
    logger.info(f"Found {len(df_truly_new)} truly new entries")
    logger.info(f"Filtered out {duplicates_filtered} duplicate entries")
    
    urls_with_new_comments = set(
        df_truly_new[df_truly_new['comment_text'].notna()]['post_url'].unique()
    )
    
    if urls_with_new_comments:
        mask_to_remove = (
            df_existing['comment_text'].isna() & 
            df_existing['post_url'].isin(urls_with_new_comments) &
            (df_existing.get('extraction_status', '') == 'NO_COMMENTS')
        )
        removed_count = mask_to_remove.sum()
        df_existing = df_existing[~mask_to_remove].copy()
        
        if removed_count > 0:
            logger.info(f"Removed {removed_count} obsolete registry entries")
    
    df_combined = pd.concat([df_existing, df_truly_new], ignore_index=True)
    df_combined = df_combined.drop(columns=['_comment_hash'])
    
    return df_combined


def process_datetime_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Procesa las columnas de fecha/hora, creando campos adicionales."""
    if 'created_time' not in df.columns:
        return df
    
    df['created_time_processed'] = pd.to_datetime(
        df['created_time'], 
        errors='coerce', 
        utc=True, 
        unit='s'
    )
    
    mask = df['created_time_processed'].isna()
    if mask.any():
        df.loc[mask, 'created_time_processed'] = pd.to_datetime(
            df.loc[mask, 'created_time'], 
            errors='coerce', 
            utc=True
        )
    
    if df['created_time_processed'].notna().any():
        df['created_time_processed'] = (
            df['created_time_processed'].dt.tz_localize(None)
        )
        df['fecha_comentario'] = df['created_time_processed'].dt.date
        df['hora_comentario'] = df['created_time_processed'].dt.time
    
    return df


# ============================================================================
# FUNCIONES DE PERSISTENCIA
# ============================================================================

def save_to_excel(
    df: pd.DataFrame, 
    filename: str, 
    scraper: Optional[SocialMediaScraper] = None
) -> bool:
    """Guarda el DataFrame en Excel con múltiples hojas de resumen."""
    try:
        with pd.ExcelWriter(filename, engine='openpyxl') as writer:
            df.to_excel(writer, sheet_name='Comentarios', index=False)
            
            if not df.empty and 'post_number' in df.columns:
                df_copy = df.copy()
                df_copy['post_number'] = pd.to_numeric(df_copy['post_number'], errors='coerce')
                df_copy['likes_count'] = pd.to_numeric(df_copy['likes_count'], errors='coerce').fillna(0).astype(int)
                
                summary = df_copy.groupby(['post_number', 'platform', 'post_url'], dropna=False).agg(
                    Total_Comentarios=('comment_text', lambda x: int(x.notna().sum())),
                    Total_Replies=('is_reply', lambda x: int(x.sum()) if x.dtype == bool else 0),
                    Total_Likes=('likes_count', 'sum'),
                    Primera_Extraccion=(
                        'created_time_processed', 
                        lambda x: x.min() if x.notna().any() else None
                    ),
                    Ultima_Extraccion=(
                        'created_time_processed', 
                        lambda x: x.max() if x.notna().any() else None
                    )
                ).reset_index()
                
                summary = summary.sort_values('post_number')
                summary.to_excel(writer, sheet_name='Resumen_Posts', index=False)
                
                df_with_comments = df_copy[df_copy['comment_text'].notna()].copy()
                
                if not df_with_comments.empty:
                    platform_stats = df_with_comments.groupby('platform').agg(
                        Total_Posts=('post_url', 'nunique'),
                        Total_Comentarios=('comment_text', 'count'),
                        Total_Replies=('is_reply', lambda x: int(x.sum()) if x.dtype == bool else 0),
                        Promedio_Likes=('likes_count', 'mean'),
                        Total_Likes=('likes_count', 'sum')
                    ).round(2).reset_index()
                    platform_stats.to_excel(writer, sheet_name='Stats_Plataforma', index=False)
                
                if scraper and scraper.failed_urls:
                    failed_df = pd.DataFrame({
                        'URL': scraper.failed_urls,
                        'Status': 'FAILED_ALL_ATTEMPTS'
                    })
                    failed_df.to_excel(writer, sheet_name='URLs_Fallidas', index=False)
                
                if scraper:
                    stats = scraper.get_stats_summary()
                    stats_df = pd.DataFrame([stats])
                    stats_df.to_excel(writer, sheet_name='Stats_Extraccion', index=False)
        
        logger.info(f"Excel file saved successfully: {filename}")
        return True
        
    except Exception as e:
        logger.error(f"Error saving Excel file: {e}", exc_info=True)
        return False


def load_existing_comments(filename: str) -> pd.DataFrame:
    """Carga los comentarios existentes del archivo Excel."""
    if not Path(filename).exists():
        logger.info(f"No existing file found: {filename}. Will create new file.")
        return pd.DataFrame()
    
    try:
        df_existing = pd.read_excel(filename, sheet_name='Comentarios')
        logger.info(f"Loaded {len(df_existing)} existing rows from {filename}")
        
        df_existing = normalize_existing_data(df_existing)
        
        if 'post_url_original' not in df_existing.columns:
            df_existing['post_url_original'] = df_existing['post_url'].copy()
        
        return df_existing
        
    except Exception as e:
        logger.error(f"Error loading existing file: {e}")
        return pd.DataFrame()


# ============================================================================
# FUNCIÓN PRINCIPAL
# ============================================================================

def run_extraction():
    """Función principal que ejecuta todo el proceso de extracción."""
    logger.info("=" * 70)
    logger.info("--- STARTING COMMENT EXTRACTION PROCESS ---")
    logger.info(f"--- Start Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ---")
    logger.info("=" * 70)
    
    if not APIFY_TOKEN:
        logger.error("APIFY_TOKEN not found in environment variables. Aborting.")
        return
    
    try:
        settings = load_json_config("settings.json")
        campaign_info = load_json_config("campaign_info.json")
        urls_to_process = load_urls_from_file("urls.txt")
    except Exception as e:
        logger.error(f"Failed to load configuration: {e}")
        return
    
    valid_urls = [url for url in urls_to_process if validate_url(url)]
    invalid_urls = [url for url in urls_to_process if not validate_url(url)]
    
    if invalid_urls:
        logger.warning(f"Skipping {len(invalid_urls)} invalid URLs:")
        for url in invalid_urls:
            logger.warning(f"  - {url}")
    
    logger.info(f"Valid URLs to process: {len(valid_urls)}")
    
    if not valid_urls:
        logger.warning("No valid URLs to process. Exiting.")
        return
    
    filename = settings.get('output_filename', 'Comentarios Campaña.xlsx')
    df_existing = load_existing_comments(filename)
    
    scraper = SocialMediaScraper(APIFY_TOKEN, settings)
    all_comments = []
    
    url_to_post_number = {}
    
    if not df_existing.empty and 'post_number' in df_existing.columns:
        for url in df_existing['post_url'].unique():
            if pd.notna(url):
                existing_numbers = df_existing[
                    df_existing['post_url'] == url
                ]['post_number'].dropna()
                
                if not existing_numbers.empty:
                    url_to_post_number[url] = int(existing_numbers.mode().iloc[0])
    
    next_number = max(url_to_post_number.values()) + 1 if url_to_post_number else 1
    for url in valid_urls:
        if url not in url_to_post_number:
            url_to_post_number[url] = next_number
            next_number += 1
    
    solo_primer_post = settings.get('solo_primer_post', False)
    max_comments = settings.get('max_comments_per_post', 1500)
    pause_min = settings.get('pause_between_urls_min', 30)
    pause_max = settings.get('pause_between_urls_max', 60)
    
    logger.info(f"Extraction settings: max_comments={max_comments}, "
                f"max_replies={settings.get('max_replies_per_comment', 100)}")
    
    for idx, url in enumerate(valid_urls, 1):
        post_number = url_to_post_number[url]
        platform = scraper.detect_platform(url)
        
        if not platform:
            logger.warning(f"Could not detect platform for URL: {url}")
            continue
        
        logger.info(f"\n--- Processing URL {idx}/{len(valid_urls)} (Post #{post_number}) ---")
        logger.info(f"Platform: {platform}")
        logger.info(f"URL: {url}")
        
        comments = []
        
        if platform == 'Facebook':
            comments = scraper.scrape_with_retry(
                scraper.scrape_facebook_comments, 
                url, max_comments, campaign_info, post_number
            )
        elif platform == 'Instagram':
            comments = scraper.scrape_with_retry(
                scraper.scrape_instagram_comments, 
                url, max_comments, campaign_info, post_number
            )
        elif platform == 'TikTok':
            comments = scraper.scrape_with_retry(
                scraper.scrape_tiktok_comments, 
                url, max_comments, campaign_info, post_number
            )
        
        if url in scraper.failed_urls:
            failed_entry = create_failed_registry_entry(
                url, platform, campaign_info, post_number
            )
            all_comments.append(failed_entry)
        elif not comments:
            registry_entry = create_post_registry_entry(
                url, platform, campaign_info, post_number
            )
            all_comments.append(registry_entry)
            scraper.extraction_stats['no_comments'] += 1
        else:
            all_comments.extend(comments)
        
        if not solo_primer_post and idx < len(valid_urls):
            pausa = random.uniform(pause_min, pause_max)
            logger.info(f"Pausing for {pausa:.2f} seconds before next URL...")
            time.sleep(pausa)
        
        if solo_primer_post:
            logger.info("SOLO_PRIMER_POST enabled - stopping after first URL")
            break
    
    if all_comments:
        df_new_comments = pd.DataFrame(all_comments)
        df_new_comments = process_datetime_columns(df_new_comments)
        
        df_combined = merge_comments(df_existing, df_new_comments)
        
        if 'created_time_processed' in df_combined.columns:
            df_combined = df_combined.sort_values(
                ['post_number', 'created_time_processed'], 
                ascending=[True, False],
                na_position='last'
            )
        
        final_columns = [
            'post_number', 'platform', 'campaign_name', 'post_url', 
            'post_url_original', 'author_name', 'comment_text', 'created_time',
            'created_time_processed', 'fecha_comentario', 'hora_comentario', 
            'likes_count', 'replies_count', 'is_reply', 'parent_comment_id',
            'author_url', 'extraction_status', 'created_time_raw'
        ]
        existing_cols = [col for col in final_columns if col in df_combined.columns]
        df_combined = df_combined[existing_cols]
        
        save_to_excel(df_combined, filename, scraper)
        
        total_comments = df_combined['comment_text'].notna().sum()
        total_replies = df_combined['is_reply'].sum() if 'is_reply' in df_combined.columns else 0
        total_posts = df_combined['post_number'].nunique()
        stats = scraper.get_stats_summary()
        
        logger.info("=" * 70)
        logger.info("--- EXTRACTION PROCESS FINISHED ---")
        logger.info(f"--- End Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ---")
        logger.info("")
        logger.info("📊 EXTRACTION STATISTICS:")
        logger.info(f"  • Total unique posts tracked: {total_posts}")
        logger.info(f"  • Total comments in database: {total_comments}")
        logger.info(f"  • Total replies in database: {total_replies}")
        logger.info(f"  • Extraction attempts: {stats['total_attempts']}")
        logger.info(f"  • Successful extractions: {stats['successful']}")
        logger.info(f"  • Failed extractions: {stats['failed']}")
        logger.info(f"  • Posts with no comments: {stats['no_comments']}")
        logger.info(f"  • Invalid comments filtered: {stats['invalid_comments']}")
        
        if scraper.failed_urls:
            logger.warning("")
            logger.warning(f"⚠️  FAILED URLs ({len(scraper.failed_urls)}):")
            for failed_url in scraper.failed_urls:
                logger.warning(f"  - {failed_url}")
        
        logger.info("")
        logger.info(f"✅ File saved: {filename}")
        logger.info("=" * 70)
    else:
        logger.warning("No new data to process")
        if not df_existing.empty:
            save_to_excel(df_existing, filename, scraper)


# ============================================================================
# PUNTO DE ENTRADA
# ============================================================================

if __name__ == "__main__":
    run_extraction()

