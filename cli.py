"""
CLI Application for the Financial News Scraper and Analyzer
-------------------------------------------------------------
Runs the complete analysis pipeline from the command line.

Usage:
    python cli_app.py [TICKER] [MONTHS_AGO]

Example:
    python cli_app.py PETR4 3
"""

import argparse
import logging
import sys
from typing import Callable

# Import the controller that wraps the application logic
from src.financial_analysis.controllers.app_controller import AppController

def setup_console_logging():
    """Configures the root logger to output all messages to the console."""
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)  # Set the minimum level to INFO

    # Remove any default or existing handlers
    for handler in logger.handlers[:]:
        logger.removeHandler(handler)

    # Create a new handler that writes to the console (stdout)
    console_handler = logging.StreamHandler(sys.stdout)

    # Set a clear format
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    console_handler.setFormatter(formatter)

    # Add the handler to the root logger
    logger.addHandler(console_handler)
    logging.info("Console logging configured.")

def log_progress(value: int):
    """
    Callback function to log progress updates to the console.

    Args:
        value: The progress percentage (0-100).
    """
    logging.info(f"--- PROGRESS: {value}% ---")

def log_results(results: str):
    """
    Callback function to print the final analysis results to the console.

    Args:
        results: The final analysis string from the controller.
    """
    logging.info("========================================")
    logging.info("          ANALYSIS RESULTS              ")
    logging.info("========================================")
    logging.info(f"\n{results}")

def parse_arguments():
    """
    Parses command-line arguments for ticker and months_ago.

    Returns:
        An argparse.Namespace object with 'ticker' and 'months_ago' attributes.
    """
    parser = argparse.ArgumentParser(
        description="Financial News Analysis CLI Tool.",
        epilog="Example: python cli_app.py PETR4 3"
    )

    parser.add_argument(
        "ticker",
        type=str,
        help="The stock ticker symbol to analyze (e.g., PETR4, MGLU3)."
    )

    parser.add_argument(
        "months_ago",
        type=int,
        help="How many months back to search for news."
    )

    return parser.parse_args()

def main():
    """
    Main execution function for the CLI.
    1. Sets up logging.
    2. Parses arguments.
    3. Runs the AppController.
    """
    # 1. Configure logging to print everything to console
    setup_console_logging()

    # 2. Parse command-line arguments
    try:
        args = parse_arguments()
        ticker = args.ticker.strip().upper()
        months = args.months_ago

        if months <= 0:
            logging.error("Months Ago must be a positive integer.")
            sys.exit(1)

    except Exception as e:
        logging.error(f"Error parsing arguments: {e}")
        sys.exit(1)

    logging.info(f"Starting analysis for Ticker: {ticker}, Months: {months}")

    try:
        # 3. Create and run the controller
        # We pass our console logging functions as callbacks
        controller = AppController(
            ticker=ticker,
            months_ago=months,
            progress_callback=log_progress,
            results_callback=log_results
        )

        # This will run the entire pipeline and log everything
        controller.run()

        logging.info("Analysis task finished.")

    except Exception as e:
        logging.error(f"A critical error occurred during execution: {e}", exc_info=True)
        logging.error("Script terminated unexpectedly.")
    finally:
        logging.info("Script execution complete.")

if __name__ == "__main__":
    main()