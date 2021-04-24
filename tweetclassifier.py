# snippets from https://rileymjones.medium.com/sentiment-anaylsis-with-the-flair-nlp-library-cfe830bfd0f4
from threading import Thread
from segtok.segmenter import split_single
from flair.data import Sentence
from flair.models import TextClassifier
import re
import sqlite3
import gc
from tqdm import tqdm


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

	def is_useful(self) -> bool:
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
	def __init__(self, queue_size: int):
		super().__init__()
		self.params = []
		self.queue_size = queue_size
		self.query = f'UPDATE {table} set label = ?, conf = ? WHERE ROWID = ?;'

	def full(self) -> bool:
		return len(self.params) >= self.queue_size

	def add_params(self, params: list):
		self.params.extend(params)

	def run(self) -> None:
		with sqlite3.connect(f'{root}{db}', check_same_thread=False) as write_conn:
			write_conn.executemany(self.query, self.params)
			write_conn.commit()


BATCH_SIZE = 1000
ROWS_BATCH = 100000
table = 'tweet'

root = "/mnt/hgfs/VMs_Shared/datasets/filtered/twitter/"
db = 'merging_db.sqlite'

conn = sqlite3.connect(f'{root}{db}', check_same_thread=False)

conn.execute("PRAGMA journal_mode=WAL;")
rows_count = conn.execute(f'SELECT COUNT(id) FROM tweet WHERE conf IS NULL;').fetchone()[0]
query = 'SELECT ROWID, text, conf FROM tweet WHERE conf IS NULL;'
cursor = conn.execute(query)

text_classifier = TextClassifier.load('sentiment-fast')

threads = []
batch = []
text_handlers = []
queue = Updater(ROWS_BATCH)

with tqdm(total=rows_count, desc='Reading rows') as bar:
	for row in cursor:
		rowid = row[0]
		text = row[1]
		if not text or text.strip() == '' or len(text.split()) < 2:
			continue
		text = text.strip()
		text_handler = Text(rowid, text)
		if text_handler.is_useful():
			text_handlers.append(text_handler)
			batch.extend(text_handler.sentences)
		else:
			with sqlite3.connect(f'{root}{db}', check_same_thread=False) as write_conn:
				query = f'UPDATE {table} set label = "NEUTRAL", conf = 0 WHERE ROWID = {text_handler.rowid};'
				write_conn.execute(query)
				write_conn.commit()
		if len(batch) > BATCH_SIZE:
			try:
				bar.desc = 'Predicting'
				bar.refresh()
				text_classifier.predict(batch, mini_batch_size=12)
				params = [(f'"{text_handler.text_classification()[0]}"', text_handler.text_classification()[1], text_handler.rowid) for text_handler in text_handlers]
				queue.add_params(params)
				if queue.full():
					bar.desc = 'Updating database'
					bar.refresh()
					queue.start()
					queue.join()
					queue = Updater(ROWS_BATCH)
				batch = []
				text_handlers = []
				gc.collect()
				bar.desc = 'Reading rows'
				bar.refresh()
			except KeyboardInterrupt:
				write_conn.commit()
				write_conn.close()
				exit(0)
		bar.update()

conn.close()

try:
	text_classifier.predict(batch)
	queue = Updater(ROWS_BATCH)
	params = [(f'"{text_handler.text_classification()[0]}"', text_handler.text_classification()[1], text_handler.rowid) for text_handler in text_handlers]
	queue.add_params(params)
	queue.start()
	queue.join()
	gc.collect()
except KeyboardInterrupt:
	write_conn.commit()
	write_conn.close()
	exit(0)
