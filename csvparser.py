import csv
from tqdm import tqdm

INPUT_FILE = './datasets/raw/Bitcoin_tweets.csv'
OUTPUT_FILE = './datasets/filtered/bitcoin_tweets.csv'
HEADER = ['username', 'user_location', 'user_desc', 'account_creation', 'followers', 'friends', 'user_favourites', 'user_verified', 'datetime', 'text', 'hashtags', 'source', 'is_retweet']
INPUT_DELIMITER = ';'
OUTPUT_DELIMITER = ','


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


lines = lines_count(INPUT_FILE)
wrote_lines = 0

with open(OUTPUT_FILE, 'w') as output_file:
	writer = csv.writer(output_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
	with open(INPUT_FILE, 'r') as input_file:
		reader = csv.reader(input_file)
		for line in tqdm(reader, total=lines, desc="Parsing lines"):
			if valid_line(line):
				writer.writerow(line)
				wrote_lines += 1

print(f'{wrote_lines} valid lines ({round(wrote_lines * 100 / lines, 2)}%)')