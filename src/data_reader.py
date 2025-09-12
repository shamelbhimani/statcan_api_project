import requests
import json
from typing import Any

def get_vectors(filepath: str) -> list[str]:
    with open(filepath, 'r') as vector_file:
        list_of_vectors = []
        for line in vector_file:
            list_of_vectors.append(line.replace('v', '').strip())
        return list_of_vectors

def fetch_data(list_of_vectors: list[str], period: int = 1) -> list | None:
    if not list_of_vectors:
        return None

    url = f'https://www150.statcan.gc.ca/t1/wds/rest/getDataFromVectorsAndLatestNPeriods'
    payload = [{'vectorId': v, 'latestN': period} for v in list_of_vectors]
    headers = {'Content-Type': 'application/json'}

    response = requests.post(url, headers=headers, data=json.dumps(payload))
    return response.json()

def extract_data(api_response: list) -> dict[Any, Any]:
    if not api_response:
        return {}

    extracted_data = {}

    for item in api_response:
        if (isinstance(item, dict) and item.get('status') == 'SUCCESS' and
                'object' in item):
            data_dict = {'vectorId': item['object']['vectorId']}
            sub_data_dict = {}

            for subitem in item['object']['vectorDataPoint']:
                sub_data_dict.update({str(subitem.get('refPerRaw')):
                    subitem.get(
                    'value')})

            data_dict['rawData'] = sort_dictionary(sub_data_dict,
                                                   ascending=False)

            if item['object'].get('productId') in extracted_data.keys():
                extracted_data[item['object'].get('productId')].append(
                    data_dict)
            else:
                extracted_data.update({item['object'].get('productId'):
                                       [data_dict]})

    return extracted_data

def sort_dictionary(dictionary: dict[str, Any],
                    ascending:bool = True) -> dict[str, Any] | None:
    if ascending:
        return dict(sorted(dictionary.items(), key=lambda item: item[0]))
    elif not ascending:
        return dict(sorted(dictionary.items(), key=lambda item: item[0],
                           reverse=True))
    return None

try:
    vectors = get_vectors('../info/vectors.txt')
    print(extract_data(fetch_data(vectors, 2)))
except Exception as e:
    print(e)