#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Script de Extracción de Comentarios de Redes Sociales
VERSIÓN ESTABLE - Con fix para URLs acortadas
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

    def expand_tiktok_url(self, url: str) -> str:
        """
        Expande URLs acortadas de TikTok (vt.tiktok.com) a URLs completas.
        ✅ NUEVO: Fix para URLs acortadas
        """
        if 'vt.tiktok.com' in url or 'vm.tiktok.com' in url:
            try:
                import requests
                logger.info(f"Expanding shortened TikTok URL: {url}")
                response = requests.head(url, allow_redirects=True, timeout=10)
                expanded_url = response.url
                logger.info(f"✅ Expanded to: {expanded_url}")
                return expanded_url
            except Exception as e:
                logger.warning(f"⚠️  Could not expand TikTok URL {url}: {e}")
                return url
        return url

    def expand_facebook_url(self, url: str) -> str:
        """
        Expande URLs acortadas de Facebook.
        ✅ NUEVO: Fix para URLs acortadas de Facebook
        """
        if 'fb.me' in url or 'm.facebook.com' in url:
            try:
                import requests
                logger.info(f"Expanding shortened Facebook URL: {url}")
                response = requests.head(url, allow_redirects=True, timeout=10)
                expanded_url = response.url
                logger.info(f"✅ Expanded to: {expanded_url}")
                return expanded_url
            except Exception as e:
                logger.warning(f"⚠️  Could not expand Facebook URL {url}: {e}")
                return url
        return url

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
            # ✅ EXPANDIR URL ACORTADA
            url = self.expand_facebook_url(url)
            
            logger.info(f"Processing Facebook Post {post_number}: {url}")
            
            max_replies = self.settings.get('max_replies_per_comment', 100)
        
            run_input = {
                "startUrls": [{"url": self.clean_url(url)}],
                "maxComments": max_comments,
                "maxPostComments": max_comments,
                "commentsMode": "RANKED_UNFILTERED",
                "scrapeReplies": True,
                "maxReplies": max_replies
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
            
            total_count = dataset.get().get('itemCount', 0)
            
            # ✅ VALIDAR QUE REALMENTE HAY COMENTARIOS
            if total_count == 0:
                logger.warning(f"⚠️  Facebook scraper returned 0 comments for URL: {url}")
                return []
            
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
            
            total_count = dataset.get().get('itemCount', 0)
            
            # ✅ VALIDAR QUE REALMENTE HAY COMENTARIOS
            if total_count == 0:
                logger.warning(f"⚠️  Instagram scraper returned 0 comments for URL: {url}")
                return []
            
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
            # ✅ EXPANDIR URL ACORTADA
            url = self.expand_tiktok_url(url)
            
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
            
            total_count = dataset.get().get('itemCount', 0)
            
            # ✅ VALIDAR QUE REALMENTE HAY COMENTARIOS
            if total_count == 0:
                logger.warning(f"⚠️  TikTok scraper returned 0 comments for URL: {url}")
                logger.warning(f"   This could mean:")
                logger.warning(f"   - Comments are disabled on this post")
                logger.warning(f"   - TikTok blocked the scraper")
                logger.warning(f"   - The post doesn't exist or is private")
                return []
            
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


# [El resto del código permanece igual: funciones de procesamiento, merge, save, etc.]
# Por brevedad no lo repito aquí, pero TODO lo demás es idéntico al código anterior


