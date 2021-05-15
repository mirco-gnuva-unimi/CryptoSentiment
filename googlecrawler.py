from GoogleNews import GoogleNews
from newspaper import Article
from newspaper import Config
from time import sleep
from random import randint
import pickle
from datetime import date, timedelta
import csv
from tqdm import tqdm
import nltk
import newspaper
import os

DATE_FORMAT = '%d/%m/%Y'
BACKUP_FILE = './googlecrawler.pickle'
OUTPUT_FILE = '/mnt/hgfs/VMs_Shared/datasets/filtered/news/google_news_crawled.csv'
PAGES = 1

user_agent = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.102 Safari/537.36'
config = Config()
config.browser_user_agent = user_agent

nltk.download('punkt')


def read_backup(backup_filepath: str) -> date:
	if not os.path.isfile(backup_filepath):
		with open(backup_filepath, 'wb') as file:
			pickle.dump(date.today(), file)
			sleep(1)
		return read_backup(backup_filepath)

	with open(backup_filepath, 'rb') as file:
		backup = pickle.load(file)
	return backup


def update_backup(state: date, backup_filepath: str):
	with open(backup_filepath, 'wb') as file:
		pickle.dump(state, file)


def wait(secs: int):
	for _ in tqdm(range(secs), desc='Waiting'):
		sleep(1)


last_crawled_day = read_backup(BACKUP_FILE)

next_day = last_crawled_day - timedelta(days=1) if last_crawled_day else date.today()

try:
	while True:
		client = GoogleNews(lang='en', encode='utf-8')
		date_str = next_day.strftime(DATE_FORMAT)
		client.set_time_range(date_str, date_str)
		client.search('bitcoin btc')
		for i in tqdm(range(1, PAGES+1), desc=f"{date_str}'s pages"):
			client.getpage(i)
			results = client.result()
			wait(randint(1, 30))
		print(f'{len(results)} results from {PAGES} pages.')
		print('Saving results')
		parsed_results = []
		for result in results:
			try:
				article = Article(result['link'], config=config)
				article.download()
				article.parse()
			except newspaper.article.ArticleException:
				continue
			article.nlp()
			parsed_result = [result['title'], result['media'], result['datetime'], result['desc'], result['link'],
							 result['img'], article.text, article.summary]
			parsed_results.append(parsed_result)

		with open(OUTPUT_FILE, 'a') as file:
			writer = csv.writer(file)
			writer.writerows(parsed_results)

		update_backup(next_day, BACKUP_FILE)
		next_day -= timedelta(days=1)
		wait(randint(1, 30))

except KeyboardInterrupt:
	exit(0)
