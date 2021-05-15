import sqlite3
from datetime import datetime, date
from dateutil.parser import parse
from tqdm import tqdm
import math
from threading import Thread
import re


def get_rows_count_and_pages(db_path: str, table: str, bar: tqdm):
	with sqlite3.connect(db_path) as conn:
		query = f'SELECT COUNT(ROWID) FROM {table} WHERE formatted = 0;'
		cursor = conn.execute(query)
		rows_count = cursor.fetchone()[0]
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


def empty_queue(queue: list, connection: sqlite3.Connection) -> list:
	set_bar_desc('executing')
	query = f'UPDATE {table} SET datetime = ?, date = ?, formatted = 1 WHERE ROWID = ?;'
	params = [(datetime_obj, datetime_obj.date(), rowid) for datetime_obj, rowid in queue]
	connection.executemany(query, params)
	set_bar_desc('committing')
	connection.commit()
	set_bar_desc('reading')
	return []


def set_bar_desc(desc: str):
	global bar
	bar.desc = f'{db_path.split("/")[-1]} | {table} | {desc}'
	bar.refresh()


custom_patterns = ['%I:%M %p - %d %b %Y', '%Y-%m-%d']

if __name__ == '__main__':
	db_path = '/mnt/hgfs/VMs_Shared/datasets/filtered/market.sqlite'
	table = 'quotation'
	queue_max_size = 100000

	bar = tqdm()
	Thread(target=get_rows_count_and_pages, args=(db_path, table, bar)).start()

	queue = []

	with sqlite3.connect(db_path) as read_conn, sqlite3.connect(db_path) as write_conn:
		query = f'SELECT ROWID, datetime FROM {table} WHERE formatted = 0;'
		cursor = read_conn.execute(query)

		set_bar_desc('reading')

		for row in cursor:
			datetime_str: str
			rowid, datetime_str = list(row)
			datetime_str = datetime_str.replace('"', '')
			datetime_obj = get_datetime(datetime_str)

			if datetime_obj is None:
				print(rowid, f'"{datetime_str}"')
				exit(1)

			queue.append((datetime_obj, rowid))

			if len(queue) >= queue_max_size:
				queue = empty_queue(queue, write_conn)
			bar.update()
		empty_queue(queue, write_conn)
