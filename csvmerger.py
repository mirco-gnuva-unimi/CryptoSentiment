import os
import csv
from tqdm import tqdm

FOLDER = '/mnt/hgfs/VMs_Shared/datasets/filtered'
OUTPUT_FILE = 'merged.csv'
DELIMITER = ','
output_path = os.path.join(FOLDER, OUTPUT_FILE)

files = ['bitcoin_filtered.csv',
		 'df_Final_filtered.csv',
		 'Bitcoin_tweets_filtered.csv',
		 'BitcoinTweets_filtered.csv',
		 'tweets_filtered.csv',
		 'bitcoin-twitter_filtered.csv',
		 'twits_BTC_filtered.csv',
		 'BTC_tweets_daily_example_filtered.csv',
		 'twitter2_filtered.csv',
		 'cleanprep_filtered.csv']


def merged_headers() -> list:
	merged = set()
	for filename in files:
		with open(os.path.join(FOLDER, filename), 'r') as file:
			reader = csv.reader(file)
			for line in reader:
				merged.update(line)
				break
	return list(merged)


def get_header(reader: csv.reader) -> list:
	for line in reader:
		return line


def lines_count(filepath: str) -> int:
	print('Reading csv lines...')
	lines = 0
	with open(filepath) as file:
		reader = csv.reader(file)
		for _ in reader:
			lines += 1
	print('File closed')
	return lines


def get_new_line(old_header: list, merged_header: list, old_values: list) -> list:
	named_values = {col: old_values[old_header.index(col)] for col in old_header}
	new_values = [named_values[col] if col in named_values.keys() else None for col in merged_header]
	return new_values


merged_header = merged_headers()

if not os.path.isfile(output_path) or lines_count(output_path) == 0:
	with open(os.path.join(FOLDER, OUTPUT_FILE), 'w') as file:
		writer = csv.writer(file)
		writer.writerow(merged_header)


lines = 0
with open(output_path, 'a') as out_file:
	writer = csv.writer(out_file)
	for filename in files:
		input_path = os.path.join(FOLDER, filename)
		with open(input_path, 'r') as in_file:
			reader = csv.reader(in_file)
			header = get_header(reader)
			buffer = []
			for line in tqdm(reader, total=lines_count(input_path)-1, desc=f'Reading "{filename}"'):
				buffer.append(line)
				if len(buffer) == 1000:
					writer.writerows([get_new_line(header, merged_header, e) for e in buffer])
					buffer = []
				lines += 1
			writer.writerows([get_new_line(header, merged_header, e) for e in buffer])

print(f'{lines} wrote to "{OUTPUT_FILE}"')
