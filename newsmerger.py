import sqlite3
from threading import Thread
from tqdm import tqdm
import gc


class ThreadsPool:
	def __init__(self, pool_size: int):
		self.threads = []
		self.pool_size = pool_size

	def add_thread(self, thread: Thread):
		self.threads.append(thread)

	def full(self) -> bool:
		return len(self.threads) >= self.pool_size

	def run_pool(self):
		self.start_threads()
		self.join_threads()

	def start_threads(self):
		[thread.start() for thread in self.threads]

	def join_threads(self):
		[thread.join() for thread in self.threads]

	def close_queue(self):
		self.run_pool()

	def empty_pool(self):
		self.threads = []


class MergeRow(Thread):
	def __init__(self, row: dict, rowid: int, master_table: str, db_path):
		super().__init__()
		self.row = self.parse_row(row)
		self.rowid = rowid
		self.master_table = master_table
		self.path = db_path
		self.merged_row = None
		self.conn = self.get_connection()
		self.update = None

	def parse_row(self, row: dict) -> dict:
		parsed_row = {key: self.parse_string(value) if type(value) == str else value for key, value in row.items()}
		return parsed_row

	@staticmethod
	def parse_string(string: str) -> str:
		temp = string.strip()
		temp = temp.replace('"', '')
		return temp

	def get_connection(self) -> sqlite3.Connection:
		conn = sqlite3.connect(self.path, check_same_thread=False)
		conn.row_factory = sqlite3.Row
		return conn

	def get_header(self) -> list:
		query = f'PRAGMA table_info({self.master_table});'
		cursor = self.conn.execute(query)
		columns = cursor.fetchall()
		header = [column['name'] for column in columns]
		return header

	def get_master_row(self) -> dict:
		text = self.row['text']
		query = f'SELECT * FROM {master_table} WHERE text="{text}";'
		try:
			result = self.conn.execute(query).fetchone()
		except sqlite3.OperationalError:
			print('\n', query, '\n')
		if result is None:
			return None
		master_row = dict(result)
		return master_row

	def run(self) -> None:
		master_row = self.get_master_row()
		if master_row is not None:
			temp_row = {key: value if value is not None else self.row[key] for key, value in master_row.items()}
			self.update = True
		else:
			temp_row = {key: self.row[key] if key in self.row.keys() else None for key in self.get_header()}
			self.update = False
		self.merged_row = temp_row


class MergeTable:
	def __init__(self, master_table: str, table: str, db_path: str):
		self.master = master_table
		self.table = table
		self.path = db_path
		self.read_query = f'SELECT ROWID, * FROM {self.table};'
		self.pool = ThreadsPool(THREADS)

	def get_master_header(self) -> list:
		query = f'PRAGMA table_info({self.master});'
		with self.new_dict_connection() as conn:
			cursor = conn.execute(query)
		columns = cursor.fetchall()
		header = [column['name'] for column in columns]
		return header

	def new_dict_connection(self) -> sqlite3.Connection:
		conn = sqlite3.connect(self.path)
		conn.row_factory = sqlite3.Row
		return conn

	def new_connection(self) -> sqlite3.Connection:
		return sqlite3.connect(self.path, check_same_thread=False)

	def rows_count(self) -> int:
		with self.new_connection() as conn:
			count = conn.execute(f'SELECT COUNT(text) FROM {self.table};').fetchone()[0]
		return count

	def update_row(self, row_obj: MergeRow, conn: sqlite3.Connection):
		values = ', '.join([f'{key} = "{value}"' if type(value) == str else f'{key} = {value}'
				  for key, value in row_obj.merged_row.items()])
		query = f'UPDATE {self.master} SET {values} WHERE ROWID = {row_obj.rowid};'
		conn.execute(query)

	def insert_row(self, row_obj: MergeRow, conn: sqlite3.Connection):
		values = ', '.join([f'"{value}"' for value in row_obj.merged_row.values()])
		keys = ', '.join(self.get_master_header())
		query = f'INSERT INTO {self.master} ({keys}) VALUES ({values});'
		conn.execute(query)

	def update_rows(self, rows: list):
		row: MergeRow
		with self.new_connection() as conn:
			[self.update_row(row, conn) for row in rows]
			conn.commit()

	def insert_rows(self, rows: list):
		row: MergeRow
		with self.new_connection() as conn:
			[self.insert_row(row, conn) for row in rows]
			conn.commit()

	def merge_rows(self):
		with self.new_dict_connection() as read_conn:
			rows = read_conn.execute(self.read_query)
			rows_count = self.rows_count()
			for row in tqdm(rows, total=rows_count):
				row_dict = dict(row)
				rowid = row_dict['rowid']
				del row_dict['rowid']
				self.pool.add_thread(MergeRow(row_dict, rowid, self.master, self.path))

				if self.pool.full():
					self.pool.run_pool()
					row_obj: MergeRow
					update_queue = [row_obj for row_obj in self.pool.threads if row_obj.update]
					insert_queue = [row_obj for row_obj in self.pool.threads if not row_obj.update]
					self.update_rows(update_queue)
					self.insert_rows(insert_queue)
					self.pool.empty_pool()

			self.pool.close_queue()
			self.update_rows([row_obj for row_obj in self.pool.threads if row_obj.update])
			self.insert_rows([row_obj for row_obj in self.pool.threads if not row_obj.update])
			self.pool.empty_pool()


THREADS = 100

master_table = 'news'

tables = [#'cointelegraph_news',
		  #'crypto_news_filtered',
		  #'google_news_crawled'
	]

path = '/mnt/hgfs/VMs_Shared/datasets/filtered/news.sqlite'

conn = sqlite3.connect(path)
conn.execute('PRAGMA journal_mode=WAL;')
conn.close()


with tqdm(total=len(tables)) as bar:
	for table in tables:
		bar.desc = table
		bar.refresh()
		MergeTable(master_table, table, path).merge_rows()
		bar.update()
