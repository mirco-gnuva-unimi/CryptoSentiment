import pandas as pd
from loguru import logger
import sys
import sqlite3
from textblob import TextBlob
from tweetscrawler import Tweet
from nltk.sentiment.vader import SentimentIntensityAnalyzer
import nltk
import numpy as np
from tqdm import tqdm
import spacy

nltk.download('vader_lexicon')

DB_FILE = 'tweets.db'

LOGGER_FORMAT = '<green>{time: YYYY-MM-DD HH:mm:ss.SSS}</green> <level>{level} | {message}</level>'
LOGGER_LEVEL = "DEBUG"

logger.remove()
logger.add(sys.stdout, format=LOGGER_FORMAT, level=LOGGER_LEVEL)


@logger.catch
def get_db_connection(db_file: str) -> sqlite3.Connection:
	connection = sqlite3.connect(db_file)
	return connection


db_connection = get_db_connection(DB_FILE)

df = pd.read_sql('SELECT * FROM tweet', db_connection)

texts = df['text'][380:]

counters = [0, 0, 0]

for text in tqdm(texts):
	#print(text)
	analysis = TextBlob(text)
	score = SentimentIntensityAnalyzer().polarity_scores(text)
	polarity = analysis.sentiment.polarity
	#print(score)
	# print(analysis.sentiment.polarity)
	#counters[np.argmax(list(score.values())[:-1])] += 1

	if polarity < -(2/6):
		counters[0] += 1
	elif polarity > 2/6:
		counters[2] += 1
	else:
		counters[1] += 1


print(counters)
tot = sum(counters)
print(f'{round(counters[0]/tot*100, 2)}% neg   |   {round(counters[1]/tot*100, 2)}% neu   |   {round(counters[2]/tot*100, 2)}% pos')
