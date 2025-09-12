import requests
import json
from typing import Any

def get_vectors(filepath: str) -> list[str]:
    with open(filepath, 'r') as vector_file:
        list_of_vectors = []
        for line in vector_file:
            new_line = line.replace('v', '').strip()
            list_of_vectors.append(new_line)
        return list_of_vectors

def fetch_data(list_of_vectors: list[str], period: int = 1) -> list | None:
    if not list_of_vectors:
        return None

    url = f'https://www150.statcan.gc.ca/t1/wds/rest/getDataFromVectorsAndLatestNPeriods'
    payload = [{'vectorId': v, 'latestN': period} for v in list_of_vectors]
    headers = {'Content-Type': 'application/json'}

    response = requests.post(url, headers=headers, data=json.dumps(payload))
    return response.json()

def extract_data(api_response: list) -> list[Any] | dict[Any, Any]:
    if not api_response:
        return []

    final_list = []
    for item in api_response:
        extracted_data = {}
        if (isinstance(item, dict) and item.get('status') == 'SUCCESS' and
                'object' in item):
            data_point_upper = item['object']
            data_point_lower = data_point_upper['vectorDataPoint']

            data_dict = {'vectorId': data_point_upper['vectorId']}
            sub_data_dict = {}
            for subitem in data_point_lower:
                sub_data_dict.update({str(subitem.get('refPerRaw')):
                    subitem.get(
                    'value')})
            data_dict['rawData'] = sub_data_dict

            extracted_data[data_point_upper.get('productId')] = data_dict
        final_list.append(extracted_data)
    return final_list

try:
    vectors = get_vectors('../info/vectors.txt')
    print(extract_data(fetch_data(vectors, 2)))
except Exception as e:
    print(e)