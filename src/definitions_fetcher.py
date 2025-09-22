import configparser
from typing import Any

def get_definitions(config_path: str = '../config/config.ini') -> dict[Any,
                                                                        Any]:
    config = configparser.ConfigParser()
    config.read(config_path)
    path = config.get('paths', 'vectors_definitions_file')
    definitions = {}

    try:
        with open(path, 'r') as vectors_definitions_file:
            for line in vectors_definitions_file:
                line = line.strip()
                if not line:
                    continue

                parts = line.split(',', 1)
                if len(parts) == 2:
                    key_str = parts[0].replace('v', '').strip()
                    value_str = parts[1].strip().strip('"')

                    try:
                        definitions[int(key_str)] = value_str
                    except ValueError:
                        print(f"Skipping malformed line (invalid key): "
                              f"{line}")
    except FileNotFoundError:
        print(f"Error: file {path} not found")

    return definitions