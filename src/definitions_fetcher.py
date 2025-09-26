import configparser
import os
from typing import Any
import logging

def get_definitions(config_path: str = '../config/config.ini') -> dict[Any,
                                                                        Any]:
    if not os.path.exists(config_path):
        logging.info(f'Config file {config_path} does not exist.')
        config = None
    else:
        config = configparser.ConfigParser()
        config.read(config_path)
        logging.info(f'Config read at path {config_path} successfully.')

    try:
        path1 = config.get('paths', 'vectors_definitions_file')
        logging.info(f'Vectors definitions file at {path1} read successfully.')
        path2 = config.get('paths', 'table_definitions_file')
        logging.info(f'Table definitions file at {path2} read successfully.')
    except (configparser.NoSectionError, configparser.NoOptionError) as e:
        logging.error(f'Configuration error at {config_path}: {e}')
        path1 = None
        path2 = None

    list_of_paths = [path1, path2]
    definitions = {}

    for path in list_of_paths:
        try:
            with open(path, 'r') as file:
                logging.info(f'Reading file at {path} successfully.')
                for line in file:
                    line = line.strip()
                    if not line:
                        logging.info(f'Empty line at {path}. Skipping...')
                        continue
                    parts = line.split(',', 1)
                    if len(parts) == 2:
                        if parts[0].startswith('v'):
                            key_str = parts[0].replace('v',
                                                       '').strip()
                            value_str = parts[1].strip().strip('"')
                        else:
                            key_str = parts[0].replace('-',
                                                       '').strip()
                            value_str = parts[1].strip().strip('"')
                        try:
                            definitions[int(key_str)] = value_str
                            logging.info(f'Value {value_str} at {key_str} '
                                         f'read successfully.')
                        except ValueError:
                            logging.warning(f'Skipping malformed line in '
                                            f'{path} at {line}: Invalid key '
                                            f'{key_str}')
                    else:
                        logging.warning(f'Skipping malformed line in {path} '
                                        f'at {line}: Does not have two parts '
                                        f'separated by a comma.')
        except FileNotFoundError:
            logging.info(f'Error: File {path} not found.')
        except Exception as e:
            logging.error(f'An unexpected error ocurred while reading {path}: '
                          f'{e}')
    logging.info(f'Finished fetching definitions. Found {len(definitions)} '
                 f'definitions.')
    return definitions