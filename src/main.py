import logging
import sys
from pathlib import Path

from api_client import APIClient
from database_manager import run_process
from definitions_fetcher import get_definitions


def main():
    """
    The main function of the program. Extracts data from the Statistics
    Canada Web Data Service (API), transforms it into a simpler format for
    efficiency, and Loads it into a MySQL database (ETL).

    :except ValueError: Raises an error if period input is not an integer and
    prompts the user to input an integer.
    :except Exception: Raises an exception if an unknown error occurs in the
    process.
    """
    project_root = Path(__file__).resolve().parent.parent
    config_path = project_root / "config" / "config.ini"
    secrets_path = project_root / "config" / "secrets.ini"
    log_path = project_root / "app.log"

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(module)s.%(funcName)s - %(message)s",
        handlers=[
            logging.FileHandler(log_path, mode="w"),
            logging.StreamHandler(sys.stdout),
        ],
    )
    logging.info("Application started...")
    try:
        api_client = APIClient(config_path)
        period = None
        try:
            period = int(
                input("Enter the period of the period you wish to extract data from: ")
            )
        except ValueError:
            logging.error(f"Invalid input: {period}. Please enter an integer.")
            return

        api_client.run(period)
        data = api_client.extracted_data
        definitions = get_definitions(config_path)

        if not data:
            logging.critical(f"No data found for period {period}. Aborting process.")
            return
        if not definitions:
            logging.warning("Definitions file might be empty or missing...")

        run_process(data, definitions, secrets_path)
    except Exception as e:
        logging.critical(f"An unhandled exception occurred: {e}")
    finally:
        logging.info("Application ended...")


if __name__ == "__main__":
    main()
