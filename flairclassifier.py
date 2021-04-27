# snippets from https://rileymjones.medium.com/sentiment-anaylsis-with-the-flair-nlp-library-cfe830bfd0f4
from threading import Thread
from segtok.segmenter import split_single
from flair.data import Sentence
from flair.models import TextClassifier
import re
import sqlite3
import gc
from tqdm import tqdm
import os
import argparse


class Counter:
	def __init__(self, start=1):
		self.value = float(start)

	def increase(self, step=1):
		self.value += float(step)


class SentencesClassifications:
	def __init__(self):
		self.classifications = {}

	def update(self, label, conf):
		if label not in self.classifications:
			self.classifications[label] = [Counter(conf), Counter()]
		else:
			self.classifications[label][0].increase(conf)
			self.classifications[label][1].increase()


class Text:
	def __init__(self, rowid: int, text: str):
		self.rowid = rowid
		self.text = self.clean_text(text)
		if self.splittable():
			self.sentences = self.split_text(self.text)
			self.sentences_classification = SentencesClassifications()
		self.label = None
		self.conf = 0

	@staticmethod
	def clean_text(raw: str) -> str:
		result = re.sub("<[a][^>]*>(.+?)</[a]>", 'Link.', raw)
		result = re.sub('&gt;', "", result)
		result = re.sub('&#x27;', "'", result)
		result = re.sub('&quot;', '"', result)
		result = re.sub('&#x2F;', ' ', result)
		result = re.sub('<p>', ' ', result)
		result = re.sub('</i>', '', result)
		result = re.sub('&#62;', '', result)
		result = re.sub('<i>', ' ', result)
		result = re.sub("\n", '', result)
		return result

	@staticmethod
	def split_text(text: str) -> list:
		sentences = [Sentence(sentence) for sentence in split_single(text) if len(sentence) > 0]
		return sentences

	def splittable(self) -> bool:
		return self.text is not None and self.text != ''

	def useful(self) -> bool:
		return self.splittable() and len(self.sentences) > 0

	def text_classification(self):
		if self.label is not None:
			return self.label, self.conf

		sentence: Sentence
		for sentence in self.sentences:
			if len(sentence.labels) == 0:
				continue
			label, conf = sentence.labels[0].value, sentence.labels[0].score
			self.sentences_classification.update(label, conf)
		if len(self.sentences_classification.classifications.items()) == 0:
			self.label = "NEUTRAL"
			self.conf = 0
			return self.label, self.conf
		labels_avg = {key: info[0].value/info[1].value for key, info in self.sentences_classification.classifications.items()}
		max_conf = max(labels_avg.values())
		label_idx = list(labels_avg.values()).index(max_conf)
		label = list(labels_avg.keys())[label_idx]
		self.label = label
		self.conf = max_conf
		return label, max_conf


class Updater(Thread):
	def __init__(self, queue_size: int, db_path: str):
		super().__init__()
		self.params = []
		self.path = db_path
		self.queue_size = queue_size
		self.query = f'UPDATE {table} set label = ?, conf = ? WHERE ROWID = ?;'

	def full(self) -> bool:
		return len(self.params) >= self.queue_size

	def add_params(self, params: list):
		self.params.extend(params)

	def run(self) -> None:
		with sqlite3.connect(self.path, check_same_thread=False) as write_conn:
			write_conn.executemany(self.query, self.params)
			write_conn.commit()
			query = 'PRAGMA wal_checkpoint;'
			write_conn.execute(query)


class DBClassifier:
	def __init__(self, path: str, table: str, mini_batch_size: int, classifier_batch_size: int, db_batch_size: int, classifier: str):
		self.path = path
		self.table = table
		self.mini_batch_size = mini_batch_size
		self.classifier_batch_size = classifier_batch_size
		self.db_batch_size = db_batch_size
		self.queue = Updater(self.db_batch_size, self.path)
		self.old_queue = None
		self.bar = tqdm(total=self.count_rows())
		self.text_handlers = []
		self.batch = []
		self.text_classifier = TextClassifier.load(classifier)
		self.read_conn = self.new_connection()
		print(f'Path: {path} | Table: {table} | mini_batch: {mini_batch_size} '
			  f'| classifier_batch: {classifier_batch_size} | db_batch: {db_batch_size} | classifier: {classifier}')

	def reset_buffers(self):
		self.text_handlers = []
		self.batch = []

	def new_connection(self) -> sqlite3.Connection:
		conn = sqlite3.connect(self.path)
		return conn

	def get_rows_cursor(self) -> sqlite3.Cursor:
		query = f'SELECT ROWID, text, conf FROM {self.table} WHERE conf IS NULL;'
		cursor = self.read_conn.execute(query)
		return cursor

	def count_rows(self) -> int:
		query = f'SELECT COUNT(ROWID) FROM {self.table} WHERE conf IS NULL;'
		with self.new_connection() as conn:
			cursor = conn.execute(query)
			rows_count = cursor.fetchone()[0]
		return rows_count

	def old_queue_running(self) -> bool:
		return self.old_queue is not None and self.old_queue.is_alive()

	def wait_old_queue(self):
		if self.old_queue_running():
			self.old_queue.join()
			self.old_queue = None

	def start_queue(self):
		self.old_queue = self.queue
		self.old_queue.start()
		self.queue = Updater(self.db_batch_size, self.path)

	def mark_useless_row(self, text_handler: Text):
		self.start_queue()
		self.wait_old_queue()
		with sqlite3.connect(self.path) as write_conn:
			query = f'UPDATE {self.table} set label = "NEUTRAL", conf = 0 WHERE ROWID = {text_handler.rowid};'
			write_conn.execute(query)

	def reset_queues(self):
		self.queue = Updater(self.db_batch_size, self.path)
		self.old_queue = None

	def set_bar_desc(self, new_desc: str):
		self.bar.desc = new_desc
		self.bar.refresh()

	def batch_ready(self) -> bool:
		return len(self.batch) > self.classifier_batch_size

	def classify_batch(self):
		self.text_classifier.predict(self.batch, mini_batch_size=self.mini_batch_size)
		params = [
			(f'"{text_handler.text_classification()[0]}"', text_handler.text_classification()[1], text_handler.rowid)
			for text_handler in self.text_handlers]
		self.queue.add_params(params)

	def process_batch(self):
		self.set_bar_desc('Classifying')
		self.classify_batch()
		self.reset_buffers()
		self.set_bar_desc('Reading db')

	def close_queues(self):
		self.set_bar_desc('Updating db')
		self.wait_old_queue()
		self.queue.start()
		self.queue.join()

	def classify_db(self):
		self.set_bar_desc('Reading db')
		rows_cursor = self.get_rows_cursor()
		for row in rows_cursor:
			rowid, text = row[:2]

			if not text:
				continue

			text_handler = Text(rowid, text)

			if not text_handler.useful():
				self.mark_useless_row(text_handler)
				continue

			self.text_handlers.append(text_handler)
			self.batch.extend(text_handler.sentences)

			if self.batch_ready():
				self.process_batch()

			if self.queue.full():
				self.set_bar_desc('Updating db')
				self.wait_old_queue()
				self.start_queue()
				self.reset_queues()
				self.set_bar_desc('Reading db')

			self.bar.update()

		self.read_conn.close()
		self.process_batch()
		self.close_queues()


def init_argparser() -> argparse.ArgumentParser:
	parser = argparse.ArgumentParser(
		usage='Flair Classifier',
		description="Classify table's text column to predict its label and confidence"
	)
	parser.add_argument('-p', '--path', help='path to db', required=True)
	parser.add_argument('-t', '--table', help='table to classify', required=True)
	parser.add_argument('-mb', '--mini-batch-size', type=int)
	parser.add_argument('-cb', '--classifier-batch-size', type=int)
	parser.add_argument('-db', '--db-batch-size', type=int)

	return parser


args_parser = init_argparser()
args = args_parser.parse_args()


MINI_BATCH_SIZE = args.mini_batch_size if args.mini_batch_size is not None else 500
CLASSIFIER_BATCH_SIZE = args.classifier_batch_size if args.classifier_batch_size is not None else 1000
DB_BATCH_SIZE = args.db_batch_size if args.db_batch_size is not None else 100000

path = args.path
table = args.table


db_classifier = DBClassifier(path, table, MINI_BATCH_SIZE,
							 CLASSIFIER_BATCH_SIZE, DB_BATCH_SIZE, 'sentiment-fast')

db_classifier.classify_db()
