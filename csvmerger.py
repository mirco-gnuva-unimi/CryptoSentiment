import os
import csv
from tqdm import tqdm
import sqlite3
import threading
from datetime import datetime
import gc

header = ['id', 'user_id', 'username', 'full_name', 'datetime', 'text', 'replies', 'retweets', 'hashtags', 'favorites',
		  'links', 'location', 'bio', 'user_created', 'followers', 'friends', 'verified', 'source', 'retweet',
		  'follows', 'label', 'profile_img', 'google_map', 'link', 'url', 'permalink', 'sent', 'sent_score', 'pol',
		  'sens', 'likes', 'to_user', 'to_user_id', 'lang', 'lat', 'long', 'conf']

text_inputs = ['Bitcoin_tweets_filtered.csv', 'BTC_tweets_daily_example_filtered.csv', 'cleanprep_filtered.csv']
id_inputs = ['bitcoin-twitter_filtered.csv', 'BitcoinTweets_filtered.csv', 'bitcoin_filtered.csv',
			 'merging_db.sqlite', 'tweets_filtered.csv', 'twits_BTC_filtered.csv', 'twitter2_filtered.csv',
			 'oct17-oct18_merged.csv']


id_tables = []
text_tables = ['cleanprep_filtered']

inputs_paths = {}

root_path = '/mnt/hgfs/VMs_Shared/datasets/filtered/twitter'

DELIMITER = ','
conn = sqlite3.connect('/mnt/hgfs/VMs_Shared/datasets/filtered/twitter/merging_db.sqlite', check_same_thread=False)
conn.row_factory = sqlite3.Row
table = 'tweet'
THREADS = 10


class LineMerger(threading.Thread):
	def __init__(self, last_line: dict, original_header: list, insert_queue: list, update_queue: list):
		self.header = original_header
		self.last_line = self.parse_line(last_line)
		self.insert_queue = insert_queue
		self.update_queue = update_queue
		self.tweet_id = int(self.last_line['id']) if self.last_line['id'] is not None else None
		super().__init__()

	def update_row(self, query: str):
		self.update_queue.append(query)

	def insert_row(self, line: dict):
		quoted_values = [f'"{value}"' for value in line.values()]
		query = f'INSERT INTO {table} ({",".join(line.keys())})' \
				f' VALUES ({",".join(quoted_values)});'
		self.insert_queue.append(query)

	def parse_line(self, line: dict) -> dict:
		no_quoted_line = self.remove_double_quote(line)
		padded_line = self.line_padding(no_quoted_line)
		return padded_line

	@staticmethod
	def quote_values(line: dict) -> dict:
		quoted_dict = {key: f'"{value}"' if type(value) == str else value for key, value in line.items()}
		return quoted_dict

	@staticmethod
	def remove_double_quote(line: dict) -> dict:
		line = {key: value.replace('"', '') if type(value) == str else value for key, value in line.items()}
		return line

	def search_in_dbs(self, column, value) -> list:
		lines = []
		for id_table in id_tables:
			query = f'SELECT * FROM {id_table} WHERE {column} = {value}'
			try:
				cursor = conn.execute(query)
			except sqlite3.OperationalError as oe:
				print(f'{query}\n{oe}')
			if cursor.rowcount > 0:
				lines.append(dict(cursor.fetchone()))
		return lines

	def merge_lines(self, present_line: dict) -> dict:
		merged_line = {key: value if value is not None else self.last_line[key] for key, value in present_line.items()}
		return merged_line

	@staticmethod
	def line_padding(line: dict) -> dict:
		padded_line = {key: line[key] if key in line.keys() else None for key in header}
		return padded_line

	def present_line(self) -> bool:
		raise NotImplementedError

	def run(self) -> None:
		if self.present_line():
			merged_line = self.merge_lines()
			self.update_row(merged_line)
		else:
			self.insert_row(self.last_line)


class LineMergerById(LineMerger):
	def __init__(self, last_line: dict, original_header: list, insert_queue: list, update_queue: list):
		super().__init__(last_line, original_header, insert_queue, update_queue)

	def present_line(self) -> bool:
		query = f'SELECT * FROM {table} WHERE id = {self.tweet_id}'
		try:
			cursor = conn.execute(query)
		except sqlite3.OperationalError as oe:
			print(f'{oe}\n{query}')
		return len(cursor.fetchall()) > 0

	def search_in_dbs(self) -> list:
		return super().search_in_dbs('id', self.tweet_id)

	def update_row(self, line: dict) -> int:
		values = [f'{key} = "{value}"' for key, value in line.items()]
		query = f'UPDATE {table} set {", ".join(values)} WHERE id = {self.tweet_id};'
		return super().update_row(query)

	def merge_lines(self) -> dict:
		present_line_query = f'SELECT * FROM {table} WHERE id = {self.tweet_id}'
		present_line = dict(conn.execute(present_line_query).fetchone())
		return super().merge_lines(present_line)


class LineMergerByText(LineMerger):
	def __init__(self, last_line: dict, original_header: list, insert_queue: list, update_queue: list):
		super().__init__(last_line, original_header, insert_queue, update_queue)
		self.text = self.last_line['text']

	def present_line(self) -> bool:
		query = f'SELECT * FROM {table} WHERE text = "{self.text}"'
		try:
			cursor = conn.execute(query)
		except sqlite3.OperationalError as oe:
			print(f'{oe}\n{query}')
		except KeyError as ke:
			print(f'error: {ke}\nquery: {query}\ntext: {self.text}')
		return len(cursor.fetchall()) > 0

	def search_in_dbs(self) -> list:
		return super().search_in_dbs('text', self.text)

	def update_row(self, line: dict) -> int:
		values = [f'{key} = "{value}"' for key, value in line.items()]
		query = f'UPDATE {table} set {", ".join(values)} WHERE text = "{self.text}";'
		return super().update_row(query)

	def merge_lines(self) -> dict:
		present_line_query = f'SELECT * FROM {table} WHERE text = "{self.text}"'
		present_line = dict(conn.execute(present_line_query).fetchone())
		return super().merge_lines(present_line)


def get_header(reader: csv.reader) -> list:
	return reader.__next__()


def get_new_line(line: list, old_header: list) -> list:
	temp = [None] * len(header)
	try:
		for idx, value in enumerate(line):
			temp[header.index(old_header[idx])] = value
	except IndexError:
		pass
	return temp


def merge_lines(line: list, new_line: list) -> list:
	for idx in range(len(line)):
		if line[idx] == '':
			line[idx] = new_line[idx]
	return line


def get_old_line_by_id(tweet_id: str) -> list:
	try:
		query = f'SELECT * FROM {table} WHERE id == {int(tweet_id)}'
	except ValueError:
		return None
	cursor = conn.execute(query)
	return cursor.next() if cursor.rowcount > 0 else None


def get_old_line_by_text(text: str) -> list:
	no_double_quotes = text.replace('"', '')
	query = f'SELECT * FROM {table} WHERE text = "{no_double_quotes}"'
	cursor = conn.execute(query)
	return cursor.next() if cursor.rowcount > 0 else None


def threads_manager(threads: list, insert_queue: list, update_queue: list):
	inserted_rows = 0
	updated_rows = 0
	[thread.start() for thread in threads]
	[thread.join() for thread in threads]

	for query in insert_queue:
		try:
			conn.execute(query)
		except:
			print(query)

	for query in update_queue:
		conn.execute(query)

	return inserted_rows, updated_rows


for root, dirs, files in os.walk(root_path, topdown=False):
	for file in files:
		if file in id_inputs:
			inputs_paths[file] = os.path.join(root, file)


inserted_rows = 0
updated_rows = 0


for next_table in tqdm(text_tables):
	print(f'Merging {next_table}...')
	start = datetime.now()
	table_info_query = f'PRAGMA table_info({next_table})'
	read_conn = sqlite3.connect('/mnt/hgfs/VMs_Shared/datasets/filtered/twitter/merging_db.sqlite', check_same_thread=False)
	read_conn.row_factory = sqlite3.Row
	cursor = read_conn.execute(table_info_query)
	table_info = [dict(row) for row in cursor.fetchall()]
	table_columns = [row['name'] for row in table_info]
	table_rows_query = f'SELECT COUNT(pol) FROM {next_table}'
	cursor = read_conn.execute(table_rows_query)
	rows_count = cursor.fetchone()[0]
	lines_query = f'SELECT * FROM {next_table}'
	lines_cursor = read_conn.execute(lines_query)
	threads = []
	insert_queue = []
	update_queue = []

	for line in tqdm(lines_cursor, leave=False, total=rows_count):
		line = dict(line)
		threads.append(LineMergerByText(line, table_columns, insert_queue, update_queue))
		if len(threads) == THREADS:
			inserted, updated = threads_manager(threads, insert_queue, update_queue)
			inserted_rows += inserted
			updated_rows += updated
			threads = []
			insert_queue = []
			update_queue = []
			conn.commit()
			conn.close()
			conn = sqlite3.connect('/mnt/hgfs/VMs_Shared/datasets/filtered/twitter/merging_db.sqlite', check_same_thread=False)
			conn.row_factory = sqlite3.Row
	inserted, updated = threads_manager(threads, insert_queue, update_queue)
	inserted_rows += inserted
	updated_rows += updated
	conn.commit()
	conn.close()
	conn = sqlite3.connect('/mnt/hgfs/VMs_Shared/datasets/filtered/twitter/merging_db.sqlite', check_same_thread=False)
	conn.row_factory = sqlite3.Row
	print(f'{next_table} merged in {datetime.now() - start}, {inserted_rows} inserted rows and {updated_rows} updated\n')


'''
print(f'Merging ...')
start = datetime.now()
with open('/mnt/hgfs/VMs_Shared/datasets/filtered/twitter/BitcoinTweets_filtered.csv', 'r') as file:
	reader = csv.reader(file)
	csv_header = reader.__next__()
	threads = []
	insert_queue = []
	update_queue = []

	for line in tqdm(reader):
		line = {label: line[idx] for idx, label in enumerate(csv_header)}
		threads.append(LineMergerById(line, csv_header, insert_queue, update_queue))
		if len(threads) == THREADS:
			inserted, updated = threads_manager(threads, insert_queue, update_queue)
			inserted_rows += inserted
			updated_rows += updated
			threads = []
			insert_queue = []
			update_queue = []
			conn.commit()
			conn.close()
			conn = sqlite3.connect('/mnt/hgfs/VMs_Shared/datasets/filtered/twitter/merging_db.sqlite', check_same_thread=False)
			conn.row_factory = sqlite3.Row
	inserted, updated = threads_manager(threads, insert_queue, update_queue)
	inserted_rows += inserted
	updated_rows += updated
	conn.commit()
	conn.close()
	conn = sqlite3.connect('/mnt/hgfs/VMs_Shared/datasets/filtered/twitter/merging_db.sqlite', check_same_thread=False)
	conn.row_factory = sqlite3.Row
	print(f' merged in {datetime.now() - start}\n')
'''

conn.close()
print('DONE')
