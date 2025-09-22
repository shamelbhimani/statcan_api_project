import configparser
from typing import Any

def get_definitions(config_path: str = '../config/config.ini') -> dict[Any,
                                                                        Any]:
    config = configparser.ConfigParser()
    config.read(config_path)
    path1 = config.get('paths', 'vectors_definitions_file')
    path2 = config.get('paths', 'table_definitions_file')
    list_of_paths = [path1, path2]
    definitions = {}

    for path in list_of_paths:
        try:
            with open(path, 'r') as file:
                for line in file:
                    line = line.strip()
                    if not line:
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
                        except ValueError:
                            print(f"Skipping malformed line (invalid key): "
                                  f"{line}")


        except FileNotFoundError:
            print(f"Error: file {path} not found")
    return definitions