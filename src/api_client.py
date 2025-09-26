import configparser
import requests
import json
import logging
from typing import Any

class APIClient:
    def __init__(self, config_path: str = '../config/config.ini') -> None:
        self.config_path = config_path
        self.config = configparser.ConfigParser()
        self.api_response = None
        self.url = f'https://www150.statcan.gc.ca/t1/wds/rest/getDataFromVectorsAndLatestNPeriods'
        self.extracted_data = {}
        try:
            self.config.read(self.config_path)
            logging.info(f'APIClient initialized for url: {self.url} and '
                         f'config at path: {self.config_path}')
        except configparser.Error:
            logging.error(f'Config file {self.config_path} could not be read.')

    def _get_vectors(self) -> list[str]:
        try:
            vector_path = self.config.get('path', 'vectors_file')
            logging.info(f'Reading vectors from {vector_path}')
            with open(vector_path, 'r') as vector_file:
                list_of_vectors = [line.replace('v', '').strip() for line in
                                   vector_file]
                logging.info(f"Found {len(list_of_vectors)} vectors.")
                return list_of_vectors
        except FileNotFoundError:
            logging.error(f"Could not find file at {self.config_path}.")
            return []
        except (configparser.NoSectionError, configparser.NoOptionError) as e:
            logging.error(f'Configuration error in {self.config_path}: {e}')
            return []

    def _fetch_data(self, list_of_vectors: list[str], period: int = 1) -> None:
        if not list_of_vectors:
            logging.warning('Vector list is empty. Skipping API call...')
            return None

        payload = [{'vectorId': v, 'latestN': period} for v in list_of_vectors]
        headers = {'Content-Type': 'application/json'}

        logging.info(f'Fetching data for {len(list_of_vectors)} from'
                     f'for the latest {period} months from {self.url}...')
        try:
            response = requests.post(self.url, headers=headers, data=json.dumps(
                payload))
            response.raise_for_status()
            self.api_response = response.json()
            logging.info(f'API response fetched successfully.')
            return self.api_response
        except requests.exceptions.RequestException as e:
            logging.error(f'API request failed: {e}')
            return None

    def _extract_data(self) -> None:
        if not self.api_response:
            logging.warning('No response from API. Skipping API call...')
            return None

        logging.info(f'Extracting data from API response...')
        extracted_count = 0
        skipped_count = 0
        for item in self.api_response:
            if (isinstance(item, dict) and item.get('status') == 'SUCCESS' and
                    'object' in item):
                sub_data_dict = {}
                for subitem in item['object']['vectorDataPoint']:
                    sub_data_dict.update({str(subitem.get('refPerRaw')):
                                              subitem.get('value')})

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
                extracted_count += 1
                logging.info(f'Added {item['object']['vectorId']} to'
                             f' {item["object"].get("productId")}.')
            else:
                skipped_count += 1
                logging.warning(f"Skipping an item in response "
                                f"Status: {item.get('status')}, "
                                f"Object: {item.get('object')}")
        logging.info(f'Data extracted from API response. Data extracted for '
                     f'{extracted_count} items. Items skipped:'
                     f' {skipped_count}.')
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
        logging.info('Starting API communication process...')
        list_of_vectors = self._get_vectors()
        self._fetch_data(list_of_vectors, period=period)
        self._extract_data()
        logging.info('API call completed.')
        return None
