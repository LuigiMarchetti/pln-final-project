"""
News Service - Handles ticker, news, and text processing operations
Provides methods to save tickers, news articles, and text processing data
"""

import logging
import json
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, List, Tuple
from mysql.connector import Error
from src.financial_analysis.persistance.database import get_database_manager

class NewsService:
    """Service class for managing news data operations"""
    
    def __init__(self):
        self.db_manager = get_database_manager()
        self.logger = logging.getLogger(__name__)
    
    def get_ticker(self, simbolo: str) -> Optional[Dict[str, Any]]:
        """
        Get ticker information by symbol
        
        Args:
            simbolo: Stock symbol (e.g., 'PETR4', 'VALE3', 'AAPL')
            
        Returns:
            Ticker data if found, None otherwise
        """
        try:
            with self.db_manager.get_cursor() as cursor:
                cursor.execute(
                    "SELECT * FROM tickers WHERE simbolo = %s",
                    (simbolo,)
                )
                ticker = cursor.fetchone()
                
                if ticker:
                    self.logger.info(f"Ticker {simbolo} found with ID {ticker['id']}")
                else:
                    self.logger.info(f"Ticker {simbolo} not found")
                
                return ticker
                
        except Error as e:
            self.logger.error(f"Error getting ticker {simbolo}: {e}")
            return None
    
    def save_ticker(self, simbolo: str, nome_empresa: str, 
                   setor: Optional[str] = None, 
                   mercado: str = 'B3') -> Optional[int]:
        """
        Save new ticker to database
        
        Args:
            simbolo: Stock symbol (e.g., 'PETR4', 'VALE3', 'AAPL')
            nome_empresa: Company name
            setor: Company sector (optional)
            mercado: Market type ('B3', 'NYSE', 'NASDAQ', 'OUTROS')
            
        Returns:
            Ticker ID if successful, None otherwise
        """
        try:
            with self.db_manager.get_cursor() as cursor:
                # Insert new ticker
                cursor.execute("""
                    INSERT INTO tickers (simbolo, nome_empresa, setor, mercado)
                    VALUES (%s, %s, %s, %s)
                """, (simbolo, nome_empresa, setor, mercado))
                
                ticker_id = cursor.lastrowid
                self.logger.info(f"New ticker {simbolo} created with ID {ticker_id}")
                return ticker_id
                
        except Error as e:
            self.logger.error(f"Error saving ticker {simbolo}: {e}")
            return None
    
    def save_or_get_ticker(self, simbolo: str, nome_empresa: str, 
                          setor: Optional[str] = None, 
                          mercado: str = 'B3') -> Optional[int]:
        """
        Save ticker if it doesn't exist, or get existing ticker ID
        
        Args:
            simbolo: Stock symbol (e.g., 'PETR4', 'VALE3', 'AAPL')
            nome_empresa: Company name
            setor: Company sector (optional)
            mercado: Market type ('B3', 'NYSE', 'NASDAQ', 'OUTROS')
            
        Returns:
            Ticker ID if successful, None otherwise
        """
        # First try to get existing ticker
        existing_ticker = self.get_ticker(simbolo)
        if existing_ticker:
            return existing_ticker['id']
        
        # If not found, save new ticker
        return self.save_ticker(simbolo, nome_empresa, setor, mercado)
    
    def save_news(self, ticker_id: int, url: str, data_publicacao: Optional[datetime] = None,
                  autor: Optional[str] = None,
                  tipo_fonte: str = 'EXAME', categoria: Optional[str] = None,
                  sentimento: Optional[str] = None, relevancia: float = 0.0) -> Optional[int]:
        """
        Save news article if it doesn't exist
        
        Args:
            ticker_id: ID of the associated ticker
            url: News article URL (unique identifier)
            data_publicacao: Publication date
            autor: Article author
            tipo_fonte: Source type ('EXAME', 'INFO_MONEY')
            categoria: News category
            sentimento: Sentiment analysis ('POSITIVO', 'NEUTRO', 'NEGATIVO')
            relevancia: Relevance score (0.00 to 1.00)
            
        Returns:
            News ID if successful, None otherwise
        """
        try:
            with self.db_manager.get_cursor() as cursor:
                # Check if news already exists
                cursor.execute(
                    "SELECT id FROM noticias WHERE url = %s",
                    (url,)
                )
                existing_news = cursor.fetchone()
                
                if existing_news:
                    self.logger.info(f"News with URL {url} already exists with ID {existing_news['id']}")
                    return existing_news['id']
                
                # Insert new news
                cursor.execute("""
                    INSERT INTO noticias (ticker_id, url, data_publicacao, autor, 
                                        tipo_fonte, categoria, sentimento, relevancia)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """, (ticker_id, url, data_publicacao, autor, tipo_fonte, 
                     categoria, sentimento, relevancia))
                
                news_id = cursor.lastrowid
                self.logger.info(f"New news article saved with ID {news_id}")
                return news_id
                
        except Error as e:
            self.logger.error(f"Error saving news article {url}: {e}")
            return None
    
    def save_text_processing(self, noticia_id: int, tipo_conteudo: str, 
                           texto_bruto: str, tokens_normalizados: Optional[str] = None,
                           tokens_stemming: Optional[str] = None,
                           tokens_lemma: Optional[str] = None,
                           outros_dados: Optional[Dict[str, Any]] = None) -> Optional[int]:
        """
        Save text processing data
        
        Args:
            noticia_id: ID of the associated news article
            tipo_conteudo: Content type ('TITULO', 'MANCHETE', 'CORPO')
            texto_bruto: Raw text content
            tokens_normalizados: Normalized tokens as string
            tokens_stemming: Stemmed tokens as string
            tokens_lemma: Lemmatized tokens as string
            outros_dados: Other metadata as dictionary
            
        Returns:
            Text processing ID if successful, None otherwise
        """
        try:
            with self.db_manager.get_cursor() as cursor:
                # Check if text processing already exists for this news and content type
                cursor.execute("""
                    SELECT id FROM processamento_texto 
                    WHERE noticia_id = %s AND tipo_conteudo = %s
                """, (noticia_id, tipo_conteudo))
                existing_processing = cursor.fetchone()
                
                if existing_processing:
                    self.logger.info(f"Text processing for news {noticia_id} and type {tipo_conteudo} already exists")
                    return existing_processing['id']
                
                # Convert outros_dados to JSON string (only this one needs JSON conversion)
                outros_dados_json = json.dumps(outros_dados) if outros_dados else None
                
                # Insert text processing data
                cursor.execute("""
                    INSERT INTO processamento_texto (noticia_id, tipo_conteudo, texto_bruto,
                                                   tokens_normalizados, tokens_stemming,
                                                   tokens_lemma, outros_dados)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                """, (noticia_id, tipo_conteudo, texto_bruto, tokens_normalizados,
                     tokens_stemming, tokens_lemma, outros_dados_json))
                
                processing_id = cursor.lastrowid
                self.logger.info(f"Text processing data saved with ID {processing_id}")
                return processing_id
                
        except Error as e:
            self.logger.error(f"Error saving text processing for news {noticia_id}: {e}")
            return None
    
    def save_complete_news_data_with_ticker_id(self, ticker_id: int, 
                                             news_data: Dict[str, Any], 
                                             text_processing_data: Dict[str, Any]) -> Tuple[Optional[int], Optional[int], List[Optional[int]], bool]:
        """
        Save complete news data using existing ticker_id and multiple text processing entries
        
        Args:
            ticker_id: Existing ticker ID
            news_data: Dictionary containing news information
            text_processing_data: Dictionary containing text processing information for título, manchete, and corpo
            
        Returns:
            Tuple of (ticker_id, news_id, [processing_ids], is_new_news) or (None, None, [], False) if failed
            is_new_news: True if news was newly saved, False if it already existed
        """
        try:
            # Check if news already exists before attempting to save
            url = news_data.get('url')
            if not url:
                self.logger.error(f"No URL provided for news data")
                return ticker_id, None, [], False
            
            # Check if news already exists
            with self.db_manager.get_cursor() as cursor:
                cursor.execute(
                    "SELECT id FROM noticias WHERE url = %s",
                    (url,)
                )
                existing_news = cursor.fetchone()
            
            if existing_news:
                self.logger.info(f"News with URL {url} already exists with ID {existing_news['id']} - skipping save")
                return ticker_id, existing_news['id'], [], False
            
            # Save news (it doesn't exist)
            news_id = self.save_news(
                ticker_id=ticker_id,
                url=url,
                data_publicacao=news_data.get('data_publicacao'),
                autor=news_data.get('autor'),
                tipo_fonte=news_data.get('tipo_fonte', 'EXAME'),
                categoria=news_data.get('categoria'),
                sentimento=news_data.get('sentimento'),
                relevancia=news_data.get('relevancia', 0.0)
            )
            
            if not news_id:
                self.logger.error(f"Failed to save news for ticker_id {ticker_id}")
                return ticker_id, None, [], False
            
            # Save multiple text processing entries (título, manchete, corpo)
            processing_ids = []
            content_types = ['TITULO', 'MANCHETE', 'CORPO']
            
            for content_type in content_types:
                content_key = content_type.lower()
                if content_key in text_processing_data:
                    processing_id = self.save_text_processing(
                        noticia_id=news_id,
                        tipo_conteudo=content_type,
                        texto_bruto=text_processing_data[content_key].get('texto_bruto', ''),
                        tokens_normalizados=text_processing_data[content_key].get('tokens_normalizados'),
                        tokens_stemming=text_processing_data[content_key].get('tokens_stemming'),
                        tokens_lemma=text_processing_data[content_key].get('tokens_lemma'),
                        outros_dados=text_processing_data[content_key].get('outros_dados')
                    )
                    processing_ids.append(processing_id)
                else:
                    self.logger.warning(f"No text processing data found for {content_type}")
                    processing_ids.append(None)
            
            self.logger.info(f"Complete news data saved successfully for ticker_id {ticker_id}")
            return ticker_id, news_id, processing_ids, True
            
        except Exception as e:
            self.logger.error(f"Error saving complete news data with ticker_id: {e}")
            return None, None, [], False
    
    
    def get_news_by_ticker(self, simbolo: str, limit: int = 10) -> List[Dict[str, Any]]:
        """Get recent news for a specific ticker"""
        try:
            with self.db_manager.get_cursor() as cursor:
                cursor.execute("""
                    SELECT n.*, t.simbolo, t.nome_empresa
                    FROM noticias n
                    JOIN tickers t ON n.ticker_id = t.id
                    WHERE t.simbolo = %s
                    ORDER BY n.data_publicacao DESC
                    LIMIT %s
                """, (simbolo, limit))
                return cursor.fetchall()
        except Error as e:
            self.logger.error(f"Error getting news for ticker {simbolo}: {e}")
            return []
    
    def get_text_processing_by_news(self, noticia_id: int) -> List[Dict[str, Any]]:
        """Get text processing data for a specific news article"""
        try:
            with self.db_manager.get_cursor() as cursor:
                cursor.execute("""
                    SELECT * FROM processamento_texto
                    WHERE noticia_id = %s
                    ORDER BY tipo_conteudo
                """, (noticia_id,))
                return cursor.fetchall()
        except Error as e:
            self.logger.error(f"Error getting text processing for news {noticia_id}: {e}")
            return []
    
    def filter_news_by_ticker_and_urls(self, simbolo: Optional[str] = None, 
                                     urls: Optional[List[str]] = None) -> List[Dict[str, Any]]:
        """
        Filter news by ticker symbol and/or multiple URLs, returning processed text data
        
        Args:
            simbolo: Stock symbol to filter by (optional)
            urls: List of URLs to filter by (optional)
            
        Returns:
            List of dictionaries containing ticker, news, and text processing data
        """
        try:
            with self.db_manager.get_cursor() as cursor:
                # Build dynamic query based on provided filters
                query = """
                    SELECT 
                        t.simbolo,
                        n.url,
                        n.data_publicacao,
                        pt.tipo_conteudo,
                        pt.texto_bruto
                    FROM tickers t
                    JOIN noticias n ON t.id = n.ticker_id
                    JOIN processamento_texto pt ON n.id = pt.noticia_id
                """
                
                conditions = []
                params = []
                
                if simbolo:
                    conditions.append("t.simbolo = %s")
                    params.append(simbolo)
                
                if urls:
                    placeholders = ','.join(['%s'] * len(urls))
                    conditions.append(f"n.url IN ({placeholders})")
                    params.extend(urls)
                
                if conditions:
                    query += " WHERE " + " AND ".join(conditions)
                
                query += " ORDER BY n.data_publicacao DESC, pt.tipo_conteudo"
                
                cursor.execute(query, params)
                results = cursor.fetchall()
                
                self.logger.info(f"Found {len(results)} records for simbolo={simbolo}, urls={len(urls) if urls else 0}")
                return results
                
        except Error as e:
            self.logger.error(f"Error filtering news by ticker and URLs: {e}")
            return []

    def get_recent_news_text(self, simbolo: str, months_ago: int) -> List[str]:
        """
        Get all raw text (title, headline, body) for a ticker from a certain date onwards.

        Args:
            simbolo: The stock symbol (e.g., "PETR4").
            months_ago: How many months back to search.

        Returns:
            A list of raw text strings.
        """
        try:
            # Calculate the start date
            start_date = datetime.now() - timedelta(days=int(months_ago * 30.44)) # Avg days in month
            self.logger.info(f"Fetching news text for {simbolo} since {start_date.date()}...")

            with self.db_manager.get_cursor() as cursor:
                query = """
                    SELECT 
                        pt.texto_bruto
                    FROM processamento_texto pt
                    JOIN noticias n ON pt.noticia_id = n.id
                    JOIN tickers t ON n.ticker_id = t.id
                    WHERE 
                        t.simbolo = %s 
                        AND n.data_publicacao >= %s
                    ORDER BY
                        n.data_publicacao DESC;
                """
                cursor.execute(query, (simbolo, start_date))
                results = cursor.fetchall()

                if not results:
                    self.logger.warning(f"No recent news text found in DB for {simbolo} since {start_date.date()}")
                    return []

                # Flatten list of dictionaries and filter out empty strings
                texts = [row['texto_bruto'] for row in results if row['texto_bruto']]

                self.logger.info(f"Found {len(texts)} text fragments in database for {simbolo}.")
                return texts

        except Error as e:
            self.logger.error(f"Error getting recent news text for {simbolo}: {e}")
            return []

# Global service instance
news_service = NewsService()

def get_news_service() -> NewsService:
    """Get the global news service instance"""
    return news_service
