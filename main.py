from src.api_client import APIClient
from src.definitions_fetcher import get_definitions


def main():
    api_client = APIClient('config/config.ini')
    period = int(input("Enter the period of the period you wish to extract "
                       "data from: "))
    api_client.run(period)
    data = api_client.extracted_data

    definitions = get_definitions('config/config.ini')

    update_database(data, definitions) #Needs to be implemented

if __name__ == '__main__':
    main()
