from src.api_client import APIClient


def main():
    api_client = APIClient()
    period = int(input("Enter the period of the period you wish to extract "
                       "data from: "))
    api_client.run(period)

if __name__ == '__main__':
    main()
