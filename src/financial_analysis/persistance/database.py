"""
MySQL Database Configuration and Connection Utility
Handles database connection, initialization, and table creation
Reads configuration from .env file.
"""

import mysql.connector
from mysql.connector import Error
import logging
import os
from dotenv import load_dotenv
from typing import Optional, Dict, Any
from contextlib import contextmanager

# Load environment variables from .env file
load_dotenv()

# Database configuration from environment variables
DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': int(os.getenv('DB_PORT', 3306)),
    'user': os.getenv('DB_USER', 'root'),
    'password': os.getenv('DB_PASSWORD', 'root'),
    'database': os.getenv('DB_NAME', 'investment_news'),
    'charset': 'utf8mb4',
    'collation': 'utf8mb4_unicode_ci',
    'autocommit': True
}

class DatabaseManager:
    """Manages MySQL database connections and operations"""

    def __init__(self):
        self.connection = None
        self.logger = logging.getLogger(__name__)

    def connect(self) -> bool:
        """Establish connection to MySQL database"""
        try:
            # First connect without database to create it if needed
            config_without_db = DB_CONFIG.copy()
            db_name = config_without_db.pop('database') # Remove db name for initial connection

            if not db_name:
                self.logger.error("DB_NAME not set in .env file.")
                return False

            temp_connection = mysql.connector.connect(**config_without_db)
            cursor = temp_connection.cursor()

            # Create database if it doesn't exist
            cursor.execute(f"CREATE DATABASE IF NOT EXISTS {db_name} CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci")
            temp_connection.commit()
            cursor.close()
            temp_connection.close()

            # Now connect to the specific database
            self.connection = mysql.connector.connect(**DB_CONFIG)
            self.logger.info(f"Successfully connected to MySQL database: {DB_CONFIG['database']}")
            return True

        except Error as e:
            self.logger.error(f"Error connecting to MySQL database: {e}")
            self.logger.error("Please check your .env file and MySQL server status.")
            return False

    def disconnect(self):
        """Close database connection"""
        if self.connection and self.connection.is_connected():
            self.connection.close()
            self.logger.info("MySQL database connection closed")

    def create_tables(self) -> bool:
        """Create all necessary tables if they don't exist"""
        if not self.connection or not self.connection.is_connected():
            self.logger.error("No database connection available")
            if not self.connect():
                return False

        try:
            cursor = self.connection.cursor()

            # Use the database
            cursor.execute(f"USE {DB_CONFIG['database']}")

            # Table 1: TICKERS
            tickers_table = """
            CREATE TABLE IF NOT EXISTS tickers (
                id INT AUTO_INCREMENT PRIMARY KEY,
                simbolo VARCHAR(20) NOT NULL UNIQUE COMMENT 'Símbolo do ticker (ex: PETR4, VALE3, AAPL)',
                nome_empresa VARCHAR(255) NOT NULL COMMENT 'Nome da empresa',
                setor VARCHAR(100) COMMENT 'Setor da empresa',
                mercado ENUM('B3', 'NYSE', 'NASDAQ', 'OUTROS') DEFAULT 'B3',
                ativo BOOLEAN DEFAULT TRUE,
                data_criacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                INDEX idx_simbolo (simbolo),
                INDEX idx_setor (setor)
            ) ENGINE=InnoDB COMMENT='Tabela de tickers/ações'
            """

            # Table 2: NOTÍCIAS
            noticias_table = """
            CREATE TABLE IF NOT EXISTS noticias (
                id INT AUTO_INCREMENT PRIMARY KEY,
                ticker_id INT NOT NULL,
                url VARCHAR(500) UNIQUE COMMENT 'URL da notícia como identificação única',
                data_publicacao DATETIME COMMENT 'Data de publicação da notícia',
                autor VARCHAR(255) COMMENT 'Autor da notícia',
                tipo_fonte ENUM('EXAME', 'INFO_MONEY') COMMENT 'De qual site',
                categoria VARCHAR(100) COMMENT 'Categoria da notícia',
                sentimento ENUM('POSITIVO', 'NEUTRO', 'NEGATIVO') COMMENT 'Análise de sentimento',
                relevancia DECIMAL(3,2) DEFAULT 0.00 COMMENT 'Score de relevância (0.00 a 1.00)',
                data_criacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                data_atualizacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                FOREIGN KEY (ticker_id) REFERENCES tickers(id) ON DELETE CASCADE,
                INDEX idx_ticker_id (ticker_id),
                INDEX idx_data_publicacao (data_publicacao),
                INDEX idx_sentimento (sentimento)
            ) ENGINE=InnoDB COMMENT='Tabela de notícias relacionadas aos tickers'
            """

            # Table 3: PROCESSAMENTO DE TEXTO
            processamento_texto_table = """
            CREATE TABLE IF NOT EXISTS processamento_texto (
                id INT AUTO_INCREMENT PRIMARY KEY,
                noticia_id INT NOT NULL,
                tipo_conteudo ENUM('TITULO', 'MANCHETE', 'CORPO') NOT NULL COMMENT 'Tipo do conteúdo processado',
                texto_bruto LONGTEXT NOT NULL COMMENT 'Conteúdo textual bruto, sem processamento (texto completo)',
                tokens_normalizados LONGTEXT COMMENT 'Conteúdo textual após normalização, remoção de stopwords, pontuação etc.',
                tokens_stemming LONGTEXT COMMENT 'Tokens processados por stemming',
                tokens_lemma LONGTEXT COMMENT 'Tokens processados por lemmatização',
                outros_dados JSON COMMENT 'Rótulos, preços, datas, nomes de usuários, metadados etc.',
                data_criacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                data_atualizacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                FOREIGN KEY (noticia_id) REFERENCES noticias(id) ON DELETE CASCADE,
                INDEX idx_noticia_id (noticia_id),
                INDEX idx_tipo_conteudo (tipo_conteudo),
                FULLTEXT idx_texto_bruto (texto_bruto),
                FULLTEXT idx_tokens_normalizados (tokens_normalizados),
                FULLTEXT idx_tokens_stemming (tokens_stemming),
                FULLTEXT idx_tokens_lemma (tokens_lemma),
                UNIQUE KEY unique_noticia_tipo (noticia_id, tipo_conteudo)
            ) ENGINE=InnoDB COMMENT='Tabela de processamento de texto seguindo estrutura da imagem'
            """

            # Execute table creation
            tables = [
                ("tickers", tickers_table),
                ("noticias", noticias_table),
                ("processamento_texto", processamento_texto_table)
            ]

            for table_name, table_sql in tables:
                cursor.execute(table_sql)
                self.logger.info(f"Table '{table_name}' created or verified successfully")

            # Check if migration is needed for existing tables
            self._migrate_existing_tables(cursor)

            self.connection.commit()
            cursor.close()
            self.logger.info("All database tables created/verified successfully")
            return True

        except Error as e:
            self.logger.error(f"Error creating tables: {e}")
            return False

    def _migrate_existing_tables(self, cursor) -> bool:
        """Migrate existing tables to new schema if needed"""
        try:
            # Check if processamento_texto table exists and needs migration
            cursor.execute("""
                SELECT COLUMN_TYPE 
                FROM INFORMATION_SCHEMA.COLUMNS 
                WHERE TABLE_SCHEMA = %s 
                AND TABLE_NAME = 'processamento_texto' 
                AND COLUMN_NAME IN ('tokens_stemming', 'tokens_lemma')
            """, (DB_CONFIG['database'],))

            columns = cursor.fetchall()

            # If we have results, check if they are JSON type (need migration)
            needs_migration = False
            for column in columns:
                if 'json' in column[0].lower():
                    needs_migration = True
                    break

            if needs_migration:
                self.logger.info("Migrating existing processamento_texto table schema...")

                # Drop existing fulltext indexes if they exist
                try:
                    cursor.execute("ALTER TABLE processamento_texto DROP INDEX idx_tokens_stemming")
                except:
                    pass  # Index might not exist

                try:
                    cursor.execute("ALTER TABLE processamento_texto DROP INDEX idx_tokens_lemma")
                except:
                    pass  # Index might not exist

                # Modify columns from JSON to LONGTEXT
                cursor.execute("""
                    ALTER TABLE processamento_texto 
                    MODIFY COLUMN tokens_stemming LONGTEXT COMMENT 'Tokens processados por stemming'
                """)

                cursor.execute("""
                    ALTER TABLE processamento_texto 
                    MODIFY COLUMN tokens_lemma LONGTEXT COMMENT 'Tokens processados por lemmatização'
                """)

                # Add fulltext indexes
                cursor.execute("""
                    ALTER TABLE processamento_texto 
                    ADD FULLTEXT idx_tokens_stemming (tokens_stemming)
                """)

                cursor.execute("""
                    ALTER TABLE processamento_texto 
                    ADD FULLTEXT idx_tokens_lemma (tokens_lemma)
                """)

                self.logger.info("Migration completed successfully")

            return True

        except Error as e:
            self.logger.error(f"Error during migration: {e}")
            return False

    def test_connection(self) -> bool:
        """Test database connection"""
        try:
            if not self.connection or not self.connection.is_connected():
                return False

            cursor = self.connection.cursor()
            cursor.execute("SELECT 1")
            result = cursor.fetchone()
            cursor.close()
            return result is not None

        except Error as e:
            self.logger.error(f"Database connection test failed: {e}")
            return False

    @contextmanager
    def get_cursor(self):
        """Context manager for database cursor"""
        cursor = None
        try:
            if not self.connection or not self.connection.is_connected():
                self.connect()
            cursor = self.connection.cursor(dictionary=True)
            yield cursor
        except Error as e:
            self.logger.error(f"Database cursor error: {e}")
            if cursor:
                cursor.close()
            raise
        finally:
            if cursor:
                cursor.close()

# Global database manager instance
db_manager = DatabaseManager()

def initialize_database() -> bool:
    """Initialize database connection and create tables"""
    try:
        if db_manager.connect():
            if db_manager.create_tables():
                logging.info("Database initialized successfully")
                return True
            else:
                logging.error("Failed to create database tables")
                return False
        else:
            logging.error("Failed to connect to database")
            return False
    except Exception as e:
        logging.error(f"Database initialization failed: {e}")
        return False

def get_database_manager() -> DatabaseManager:
    """Get the global database manager instance"""
    return db_manager