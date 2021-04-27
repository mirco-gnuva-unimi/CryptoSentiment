import sqlite3
from datetime import datetime
from dateutil.parser import parse
from tqdm import tqdm
import math
from threading import Thread
import re


def get_rows_count_and_pages(db_path: str, table: str, bar: tqdm):
	global pages
	with sqlite3.connect(db_path) as conn:
		query = f'SELECT COUNT(ROWID) FROM {table} WHERE formatted = 0;'
		cursor = conn.execute(query)
		rows_count = cursor.fetchone()[0]
		pages = math.ceil(rows_count/500)
	bar.total = rows_count
	bar.refresh()


def check_datetime_format(date_sting: str, format: str) -> datetime:
	try:
		output = datetime.strptime(date_sting, format)
		return output
	except ValueError:
		return None


def dateutil_converter(date_string: str) -> datetime:
	try:
		return parse(date_string)
	except ValueError:
		return None


def timestamp_converter(date_string: str) -> datetime:
	try:
		return datetime.fromtimestamp(float(date_string))
	except ValueError:
		return None


def pattern_converter(date_string: str, patter: str) -> datetime:
	try:
		return datetime.strptime(date_string, patter)
	except ValueError:
		return None


def patterns_converter(date_string: str) -> datetime:
	for pattern in custom_patterns:
		output = pattern_converter(date_string, pattern)
		if output is not None:
			return output


def get_datetime(date_string: str) -> datetime:
	converters = [timestamp_converter, patterns_converter, dateutil_converter]
	for converter in converters:
		output = converter(date_string)
		if output is not None:
			return output
	return None


def correct_format(date_string: str) -> bool:
	return re.match('[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}', date_string) is not None


db_path = '/mnt/hgfs/VMs_Shared/datasets/filtered/bitcoin_reddit_all.sqlite'
table = 'post'
custom_patterns = ['%I:%M %p - %d %b %Y', '%Y-%m-%d']

bar = tqdm()
pages = 99999999999
Thread(target=get_rows_count_and_pages, args=(db_path, table, bar)).start()

executed_queries = 0
offset = 0
limit = 500
pages_read = 0
queue = []

with sqlite3.connect(db_path) as read_conn, sqlite3.connect(db_path) as write_conn:
	#while pages_read <= pages:
	query = f'SELECT ROWID, datetime FROM {table} WHERE formatted = 0;'
	cursor = read_conn.execute(query)

	bar.desc = 'Reading'
	bar.refresh()

	for row in cursor:
		datetime_str: str
		rowid, datetime_str = list(row)
		datetime_str = datetime_str.replace('"', '')
		if correct_format(datetime_str):
			bar.update()
			continue
		datetime_obj = get_datetime(datetime_str)

		if datetime_obj is None:
			print(rowid, f'"datetime_str"')
			exit(1)

		queue.append((datetime_obj, rowid))

		if len(queue) >= 100000:
			bar.desc = 'Committing'
			bar.refresh()
			#print('Committing')
			[write_conn.execute(f'UPDATE {table} SET datetime = "{datetime_obj}", formatted = 1 WHERE ROWID = {rowid};')
			 for datetime_obj, rowid in queue]
			write_conn.commit()

			queue = []
			bar.desc = 'Reading'
			bar.refresh()
		bar.update()

with sqlite3.connect(db_path) as conn:
	conn.execute('PRAGMA wal_checkpoint;')
	conn.commit()
