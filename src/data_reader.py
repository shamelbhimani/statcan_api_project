vectors = []

with open('../info/vectors.txt', 'r') as vector_file:
    for line in vector_file:
        new_line = line.replace('v', '').strip()
        vectors.append(new_line)
