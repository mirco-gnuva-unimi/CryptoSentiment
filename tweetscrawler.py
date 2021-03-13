import tweepy
from loguru import logger
import sys
from configparser import ConfigParser
import sqlite3
from tqdm import tqdm
from time import sleep
from nltk.corpus import stopwords
from nltk.stem import PorterStemmer
import nltk
import re

nltk.download('stopwords')

HASHTAGS_FILE = 'hashtags.txt'
OUTPUT_FILE = 'tweets.db'
API_FILE = 'twitter_api.ini'

MAX_TWEETS = 100

LOGGER_FORMAT = '<green>{time: YYYY-MM-DD HH:mm:ss.SSS}</green> <level>{level} | {message}</level>'
LOGGER_LEVEL = "DEBUG"

DATETIME_FORMAT = '%Y-%m-%d %H:%M:%S'

logger.remove()
logger.add(sys.stdout, format=LOGGER_FORMAT, level=LOGGER_LEVEL)
config = ConfigParser()
config.read(API_FILE)

stop_words = stopwords.words('english')
stemmer = PorterStemmer()


class Tweet:
	def __init__(self, raw_tweet):
		self.id = raw_tweet.id
		text = raw_tweet.text.lower()
		words = text.split(' ')
		words = filter(lambda word: '@' not in word, words)
		words = [stemmer.stem(word) for word in words if words not in stop_words]
		text = ' '.join(words)
		text = re.sub('http:\/\/[a-z]*', '', text)
		text = re.sub('https:\/\/[a-z]*', '', text)
		text = re.sub('[^a-z]', ' ', text)
		text = re.sub('\s+', ' ', text)
		self.text = text
		self.lang = raw_tweet.lang
		self.datetime = raw_tweet.created_at
		self.contributors = raw_tweet.contributors
		self.coordinates = raw_tweet.coordinates
		self.geo = raw_tweet.geo
		self.in_reply_to_id = raw_tweet.in_reply_to_user_id
		self.place = raw_tweet.place
		self.retweeted = raw_tweet.retweeted
		self.platform = raw_tweet.source
		self.author = raw_tweet.user
		self.datetime_str = self.datetime.strftime(DATETIME_FORMAT)
		self.is_en = self.lang == 'en'

	@property
	def query_values(self):
		return self.id, self.text, self.datetime_str


@logger.catch
def get_db_connection(db_file: str) -> sqlite3.Connection:
	connection = sqlite3.connect(db_file)
	return connection


@logger.catch
def get_api_client(config: ConfigParser) -> tweepy.API:
	auth = tweepy.OAuthHandler(config['CONSUMER']['key'], config['CONSUMER']['secret'])
	auth.set_access_token(config['ACCESS']['token'], config['ACCESS']['secret'])
	api = tweepy.API(auth)
	return api


@logger.catch
def read_hashtags(hashtags_file: str) -> list:
	with open(hashtags_file) as hashtag_file:
		logger.debug(f'Reading hashtag from {hashtags_file}...')
		raw_hashtags = [hashtag.strip() for hashtag in hashtag_file.readlines()]
		logger.debug(f'Read {len(raw_hashtags)} hashtags.')

	hashtags = raw_hashtags + [hashtag.capitalize() for hashtag in raw_hashtags]
	return hashtags


@logger.catch
def parse_tweets(tweets: list):
	new_tweets = 0
	for raw_tweet in tqdm(tweets, desc='Parsing tweets'):
		tweet = Tweet(raw_tweet)
		if tweet.is_en:
			query = f'INSERT INTO tweet VALUES (?, ?, ?)'
			try:
				db_connection.execute(query, tweet.query_values)
				new_tweets += 1
			except sqlite3.IntegrityError:
				pass
	return new_tweets


@logger.catch
def get_tweets(hashtag: str, limit: int) -> list:
	try:
		tweets = []
		# Worst performances but better readability
		with tqdm(total=limit, desc='Getting tweets') as bar:
			for idx, tweet in enumerate(tweepy.Cursor(api_client.search, q=hashtag).items(limit)):
				tweets.append(tweet)
				bar.update()
		return tweets
	except tweepy.error.TweepError:
		logger.warning(f"API's rate limit reached, waiting 15 minutes...")
		for _ in tqdm(range(60 * 15), desc='Waiting...'):
			sleep(1)
		try:
			return tweets + get_tweets(hashtag, limit - idx)
		except UnboundLocalError:
			return tweets + get_tweets(hashtag, limit)
	except KeyboardInterrupt:
		return tweets


if __name__ == '__main__':
	hashtags = read_hashtags(HASHTAGS_FILE)
	logger.info(f'Hashtags: {len(hashtags)}')

	api_client = get_api_client(config)
	del config

	db_connection = get_db_connection(OUTPUT_FILE)

	tweets_for_hashtag = int(MAX_TWEETS / len(hashtags))
	total_new_tweets = 0
	for hashtag in hashtags:
		tweets = get_tweets(hashtag, tweets_for_hashtag)
		try:
			total_new_tweets += parse_tweets(tweets)
			db_connection.commit()
		except KeyboardInterrupt:  # using 'except' instead of 'finally' to raise all other exceptions
			db_connection.commit()

	logger.info(f'{total_new_tweets} new tweets')
