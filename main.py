from src.api_client import APIClient
from src.definitions_fetcher import get_definitions
from src.database_manager import run_process


def main():
    api_client = APIClient('config/config.ini')
    period = int(input("Enter the period of the period you wish to extract "
                       "data from: "))
    api_client.run(period)
    data = api_client.extracted_data
    definitions = get_definitions('config/config.ini')

    run_process(data, definitions, 'config/secrets.ini')

if __name__ == '__main__':
    main()
