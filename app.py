"""
App de Web Scraping para coleta de notícias de ativos financeiros.
Executa uma única vez para o ticker e período (em meses) especificados.
"""

import time
import schedule
from datetime import datetime, timedelta
import argparse
import logging
import sys

# Importa módulos utilitários para consulta de ações, banco de dados e serviços de notícias
from utils import yahoo_finance
from utils.database import initialize_database
from utils.news_service import get_news_service
from web_scraping.exame import web_scrapping as exame_web_scrapping
from web_scraping.infomoney import web_scrapping as infomoney_web_scrapping

# Configuração de logging para registrar eventos no terminal e em arquivo
logging.basicConfig(
    level=logging.INFO,  # Nível mínimo de log
    format='%(asctime)s - %(levelname)s - %(message)s',  # Formato do log
    handlers=[
        logging.FileHandler('web_scraping.log'),  # Salva em arquivo
        logging.StreamHandler(sys.stdout)         # Mostra no console
    ]
)


class WebScrapingApp:
    def __init__(self, ticker: str, months_ago: int):
        """Inicializa a aplicação de scraping"""
        self.ticker = ticker
        self.months_ago = months_ago
        self.news_service = get_news_service()
        logging.info(f"WebScrapingApp inicializada para {ticker}, {months_ago} meses atrás.")

    def web_scrapping_exame(self, ticker_id, company_name):
        """Executa o scraping do site Exame"""
        try:
            logging.info(f"Executando web scraping - Exame (Ticker: {self.ticker}, Meses: {self.months_ago})")
            # Chama função do módulo exame.py para coletar dados
            success = exame_web_scrapping(
                ticker=self.ticker,
                ticker_id=ticker_id,
                company_name=company_name,
                news_service=self.news_service,
                months_ago=self.months_ago
            )

            if success:
                print("✅ Web scraping Exame executado com sucesso")
                logging.info("Web scraping Exame concluído")
                return True
            else:
                print("❌ Web scraping Exame falhou")
                logging.error("Web scraping Exame falhou")
                return False
        except Exception as e:
            logging.error(f"Erro no web scraping Exame: {e}")
            return False

    def web_scrapping_info_money(self, ticker_id, company_name):
        """Executa o scraping do site InfoMoney"""
        try:
            logging.info(f"Executando web scraping - InfoMoney (Ticker: {self.ticker}, Meses: {self.months_ago})")
            # Chama função do módulo infomoney.py para coletar dados
            success = infomoney_web_scrapping(
                ticker=self.ticker,
                ticker_id=ticker_id,
                company_name=company_name,
                news_service=self.news_service,
                months_ago=self.months_ago
            )
            if success:
                print("✅ Web scraping InfoMoney executado com sucesso")
                logging.info("Web scraping InfoMoney concluído")
                return True
            else:
                print("❌ Web scraping InfoMoney falhou")
                logging.error("Web scraping InfoMoney falhou")
                return False
        except Exception as e:
            logging.error(f"Erro no web scraping InfoMoney: {e}")
            return False

    def execute_scraping(self, ticker_id, company_name):
        """Executa todos os scrapers (Exame e InfoMoney)"""
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"\n🚀 Iniciando execução - {timestamp}")
        logging.info(f"Iniciando execução do scraping para {self.ticker}")

        # Executa cada scraping individual
        success_exame = self.web_scrapping_exame(ticker_id, company_name)
        success_info_money = self.web_scrapping_info_money(ticker_id, company_name)

        # Verifica resultado final
        if success_exame and success_info_money:
            print("✅ Todas as execuções concluídas com sucesso!")
            logging.info("Execução completa bem-sucedida")
        else:
            print("❌ Algumas execuções falharam. Verifique os logs.")
            logging.warning("Execução completa com falhas")

    def run_buy_and_hold(self):
        """Executa scraping apenas uma vez"""

        # Recupera ID e nome da empresa pelo ticker
        try:
            ticker_id, company_name = yahoo_finance.get_ativo(self.ticker, self.news_service)
            if not ticker_id or not company_name:
                logging.error(f"Não foi possível encontrar informações para o ticker {self.ticker}.")
                print(f"❌ Não foi possível encontrar informações para o ticker {self.ticker}. Encerrando.")
                return
        except Exception as e:
            logging.error(f"Erro ao buscar informações do ticker {self.ticker}: {e}")
            print(f"❌ Erro ao buscar informações do ticker {self.ticker}. Encerrando.")
            return

        print(f"📊 Executando scraping para {self.ticker} ({company_name}) buscando notícias dos últimos {self.months_ago} meses.")
        self.execute_scraping(ticker_id, company_name)
        print("\n🏁 Execução finalizada. Aplicação encerrada.")


def main():
    # Configuração de argumentos de linha de comando
    parser = argparse.ArgumentParser(description='App de Web Scraping de Notícias Financeiras')
    parser.add_argument(
        'ticker',
        type=str,
        help='O símbolo do ticker a ser pesquisado (ex: PETR4, VALE3, MGLU3)'
    )
    parser.add_argument(
        'months',
        type=int,
        help='A quantidade de meses no passado para buscar notícias (ex: 3)'
    )

    args = parser.parse_args()

    ticker_arg = args.ticker.upper()
    months_arg = args.months

    if months_arg <= 0:
        print("❌ O número de meses deve ser maior que zero.")
        logging.warning("Número de meses inválido (<= 0).")
        return

    # Inicializa conexão com banco de dados
    print("🔧 Initializing database connection...")
    if not initialize_database():
        print("❌ Failed to initialize database. Please check your MySQL connection (verify .env file).")
        logging.error("Database initialization failed")
        return
    print("✅ Database initialized successfully")


    # Cria instância da aplicação de scraping
    app = WebScrapingApp(ticker=ticker_arg, months_ago=months_arg)

    # Cabeçalho visual
    print("=" * 60)
    print("WEB SCRAPING DE NOTÍCIAS FINANCEIRAS")
    print("=" * 60)

    # Executa a única vez
    app.run_buy_and_hold()

# Ponto de entrada da aplicação
if __name__ == "__main__":
    main()