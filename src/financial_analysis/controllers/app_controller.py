"""
App Controller
Wraps the main application logic to be called from the GUI.
Handles scraping, database operations, and analysis.
"""
import logging
from typing import Callable

from src.financial_analysis.persistance.database import initialize_database
from src.financial_analysis.services.news_service import get_news_service
from src.financial_analysis.services import yahoo_finance_service
from src.financial_analysis.web_scraping.exame_scraper import web_scrapping as exame_web_scrapping
from src.financial_analysis.web_scraping.infomoney_scraper import web_scrapping as infomoney_web_scrapping

from src.financial_analysis.services.analysis_service import get_analysis_service

class AppController:
    """Coordinates all backend tasks for the GUI"""

    def __init__(self, ticker: str, months_ago: int,
                 progress_callback: Callable[[int], None],
                 results_callback: Callable[[str], None],
                 language: str = "English"):
        """
        Args:
            ticker: The stock ticker symbol (e.g., "PETR4").
            months_ago: How many months back to search for news.
            progress_callback: A function to call to update the GUI progress bar (takes int 0-100).
            results_callback: A function to call to display the final string results.
        """
        self.ticker = ticker
        self.months_ago = months_ago
        self.language = language
        self.progress_callback = progress_callback
        self.results_callback = results_callback

        self.news_service = get_news_service()
        self.analysis_service = get_analysis_service()

        logging.info(f"AppController initialized for {ticker}, {months_ago} months, Language: {language}.")

    def run(self):
        """
        Executes the full pipeline:
        1. Init DB
        2. Get Ticker
        3. Scrape News (Exame, InfoMoney)
        4. Fetch News from DB
        5. Analyze with LLM
        """
        try:
            # --- 1. Initialize Database ---
            self.progress_callback(5)
            logging.info("🔧 Initializing database connection...")
            if not initialize_database():
                logging.error("Database initialization failed. Please check .env and MySQL server.")
                self.results_callback("Error: Failed to initialize database.")
                return
            logging.info("✅ Database initialized successfully.")
            self.progress_callback(10)

            # --- 2. Get Ticker Info ---
            logging.info(f"📊 Fetching info for ticker: {self.ticker}")
            ticker_id, company_name = yahoo_finance_service.get_ativo(self.ticker, self.news_service)
            if not ticker_id or not company_name:
                logging.error(f"Could not find ticker {self.ticker}.")
                self.results_callback(f"Error: Could not find ticker {self.ticker} via Yahoo Finance.")
                return
            logging.info(f"✅ Found: {company_name} (ID: {ticker_id})")
            self.progress_callback(20)

            # --- 3. Scrape News ---
            logging.info(f"📰 Starting web scraping for {company_name}...")
            logging.info("... scraping Exame ...")
            exame_success = exame_web_scrapping(
                ticker_id=ticker_id,
                company_name=company_name,
                news_service=self.news_service,
                months_ago=self.months_ago
            )
            if not exame_success:
                logging.warning("Exame scraping failed or returned no new data.")
            logging.info("✅ Exame scraping complete.")
            self.progress_callback(40)

            logging.info("... scraping InfoMoney ...")
            infomoney_success = infomoney_web_scrapping(
                ticker_id=ticker_id,
                company_name=company_name,
                news_service=self.news_service,
                months_ago=self.months_ago
            )
            if not infomoney_success:
                logging.warning("InfoMoney scraping failed or returned no new data.")
            logging.info("✅ InfoMoney scraping complete.")
            self.progress_callback(60)

            # --- 4. Fetch News from DB for Analysis ---
            logging.info(f"📚 Fetching recent news from database for analysis...")
            # Use the NEW function to get all text
            news_texts = self.news_service.get_recent_news_text(self.ticker, self.months_ago)

            if not news_texts:
                logging.warning("No news text found in the database for this period.")
                self.results_callback(f"Scraping complete, but no recent news text was found in the database for {self.ticker} to analyze.")
                self.progress_callback(100)
                return

            logging.info(f"Found {len(news_texts)} text fragments. Consolidating...")
            # Consolidate all text into one large block
            # Using a set to remove duplicate text fragments (e.g., identical headlines)
            consolidated_text = "\n\n---\n\n".join(list(set(news_texts)))
            logging.info(f"Total text size for analysis: {len(consolidated_text)} characters.")
            self.progress_callback(75)

            # --- 5. Analyze with LLM ---
            logging.info(f"🤖 Sending consolidated text to Gemini for fundamental analysis...")
            if not self.analysis_service.is_configured():
                logging.error("Analysis service is not configured. Check GEMINI_API_KEY.")
                self.results_callback("Error: AnalysisService is not configured. Please set GEMINI_API_KEY in your .env file.")
                return

            analysis_result = self.analysis_service.analyze_company_fundamentals(
                company_name=company_name,
                ticker=self.ticker,
                all_news_text=consolidated_text,
                language=self.language
            )

            if analysis_result:
                logging.info("✅ Analysis complete.")
                self.results_callback(analysis_result)
            else:
                logging.error("Failed to get analysis from LLM.")
                self.results_callback("Error: Failed to get a response from the analysis service.")

            self.progress_callback(100)

        except Exception as e:
            logging.error(f"A critical error occurred during execution: {e}", exc_info=True)
            self.results_callback(f"An unexpected error occurred: {e}")
        finally:
            # Ensure progress bar is full
            self.progress_callback(100)