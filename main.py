import logging
import sys
from src.api_client import APIClient
from src.definitions_fetcher import get_definitions
from src.database_manager import run_process


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(module)s.%(funcName)s - %("
               "message)s",
        handlers=[
            logging.FileHandler("app.log", mode="w"),
            logging.StreamHandler(sys.stdout)
        ]
    )
    logging.info("Application started...")
    try:
        api_client = APIClient('config/config.ini')
        try:
            period = int(input("Enter the period of the period you wish to extract "
                               "data from: "))
        except ValueError:
            logging.error(f"Invalid input: {period}. Please enter an integer.")
            return

        api_client.run(period)
        data = api_client.extracted_data
        definitions = get_definitions('config/config.ini')

        if not data:
            logging.critical(f"No data found for period {period}. Aborting "
                             f"process.")
            return
        if not definitions:
            logging.warning(f'Definitions file might be empty or missing...')

        run_process(data, definitions, 'config/secrets.ini')
    except Exception as e:
        logging.critical(f'An unhandled exception occurred: {e}')
    finally:
        logging.info("Application ended...")

if __name__ == '__main__':
    main()
