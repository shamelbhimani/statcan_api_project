from typing import Dict

def load_vector_definitions(path: str) -> Dict[str, str]:
    definitions = {}
    try:
        with open(path, 'r') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#'):
                    parts = []
                    in_quotes = False
                    current_part = ''
                    for char in line:
                        if char == '"':
                            in_quotes = not in_quotes
                        elif char == ',' and not in_quotes:
                            parts.append(current_part)
                            current_part = ''
                        else:
                            current_part += char
                    parts.append(current_part)

                if len(parts) == 2:
                    a, b = parts
                    if b.startswith('"') and b.endswith('"'):
                        b = b[1:-1]
                    definitions[int(a.strip().replace("v", ""))] = b
                else:
                    print(f'Skipping malformed line {line}')

    except FileNotFoundError:
        print(f'File {path} not found')
    return definitions



try:
    print(load_vector_definitions('../info/vector_definitions.csv'))
except FileNotFoundError:
    print('No vector definitions file found')