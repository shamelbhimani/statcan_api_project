import configparser
import requests
import json
from typing import Any

class APIClient:
    def __init__(self, config_path: str = '../config/config.ini') -> None:
        self.config_path = config_path
        self.config = configparser.ConfigParser()
        self.config.read(self.config_path)
        self.api_response = None
        self.url = f'https://www150.statcan.gc.ca/t1/wds/rest/getDataFromVectorsAndLatestNPeriods'
        self.extracted_data = {}

    def _get_vectors(self) -> list[str]:
        path = self.config.get('paths', 'vectors_file')
        try:
            with open(path, 'r') as vector_file:
                list_of_vectors = []
                for line in vector_file:
                    list_of_vectors.append(line.replace('v',
                                                        '').strip())
                return list_of_vectors
        except FileNotFoundError:
            return []

    def _fetch_data(self, list_of_vectors: list[str], period: int = 1) -> None:
        if not list_of_vectors:
            return None

        payload = [{'vectorId': v, 'latestN': period} for v in list_of_vectors]
        headers = {'Content-Type': 'application/json'}

        try:
            response = requests.post(self.url, headers=headers, data=json.dumps(
                payload))
            response.raise_for_status()
            self.api_response = response.json()
            return self.api_response
        except requests.exceptions.RequestException as e:
            print(e)
            return None

    def _extract_data(self) -> None:
        if not self.api_response:
            return None

        for item in self.api_response:
            if (isinstance(item, dict) and item.get('status') == 'SUCCESS' and
                    'object' in item):
                sub_data_dict = {}
                for subitem in item['object']['vectorDataPoint']:
                    sub_data_dict.update({str(subitem.get('refPerRaw')):
                        subitem.get(
                            'value')})

                data_dict = {
                    item['object']['vectorId']: self._sort_dictionary(
                        sub_data_dict, False)
                }

                if (item['object'].get('productId') in
                        self.extracted_data.keys()):
                    self.extracted_data[item['object'].get('productId')].update(
                        data_dict)
                else:
                    self.extracted_data.update({item['object'].get('productId'):
                                                    data_dict})
            else:
                pass

        return None

    @staticmethod
    def _sort_dictionary(dictionary: dict[str, Any],
                        ascending:bool = True) -> dict[str, Any] | None:
        if ascending:
            return dict(sorted(dictionary.items(), key=lambda item: item[0]))
        elif not ascending:
            return dict(sorted(dictionary.items(), key=lambda item: item[0],
                               reverse=True))
        return None

    def run(self, period: int = 1) -> None:
        list_of_vectors = self._get_vectors()
        self._fetch_data(list_of_vectors, period=period)
        self._extract_data()
        return None
