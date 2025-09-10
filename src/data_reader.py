import requests
import json

def get_vectors(filepath: str):
    with open(filepath, 'r') as vector_file:
        list_of_vectors = []
        for line in vector_file:
            new_line = line.replace('v', '').strip()
            list_of_vectors.append(new_line)
        return list_of_vectors

def fetch_data(list_of_vectors: list[str], period: int = 1):
    if not list_of_vectors:
        return None

    url = f'https://www150.statcan.gc.ca/t1/wds/rest/getDataFromVectorsAndLatestNPeriods'
    payload = [{'vectorId': v, 'latestN': period} for v in list_of_vectors]
    headers = {'Content-Type': 'application/json'}

    response = requests.post(url, headers=headers, data=json.dumps(payload))
    return response.json()

try:
    vectors = get_vectors('../info/vectors.txt')
    fetch_data(vectors, 1)
except Exception as e:
    print(e)