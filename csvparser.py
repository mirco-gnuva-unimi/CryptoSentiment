import csv
from tqdm import tqdm

INPUT_FILE = '/mnt/hgfs/VMs_Shared/datasets/filtered/twitter2_filtered.csv'
OUTPUT_FILE = '/mnt/hgfs/VMs_Shared/datasets/filtered/twitter2_filtered_1.csv'
HEADER = ['label', 'datetime', 'username', 'full_name', 'text', 'id', 'source', 'followers', 'follows', 'retweets', 'favorites', 'verified', 'user_created', 'location', 'bio', 'profile_img', 'google_map']
INPUT_DELIMITER = ','
OUTPUT_DELIMITER = ','


def print_first_lines():
	with open(INPUT_FILE) as file:
		reader = csv.reader(file)
		for idx, l in enumerate(reader):
			print(l)
			if idx >= 4:
				exit()


def lines_count(filepath: str) -> int:
	print('Reading csv lines...')
	lines = 0
	with open(filepath) as file:
		reader = csv.reader(file)
		for _ in reader:
			lines += 1
	print('File closed')
	return lines


def valid_line(line: list) -> bool:
	return len(line) == len(HEADER)


# print_first_lines()

lines = lines_count(INPUT_FILE)
wrote_lines = 0

with open(OUTPUT_FILE, 'w') as output_file:
	writer = csv.writer(output_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
	writer.writerow(HEADER)
	with open(INPUT_FILE, 'r') as input_file:
		reader = csv.reader(input_file)
		for line in tqdm(reader, total=lines, desc="Parsing lines"):
			if valid_line(line):
				writer.writerow(line)
				wrote_lines += 1

print(f'{wrote_lines} valid lines ({round(wrote_lines * 100 / lines, 2)}%)')
