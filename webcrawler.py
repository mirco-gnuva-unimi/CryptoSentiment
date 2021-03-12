import tweepy
from loguru import logger
import sys
from configparser import ConfigParser
import sqlite3
from tqdm import tqdm

HASHTAGS_FILE = 'hashtags.txt'
OUTPUT_FILE = 'tweets.db'
API_FILE = 'twitter_api.ini'

MAX_TWEETS = 500 * 1000

LOGGER_FORMAT = '<green>{time: YYYY-MM-DD HH:mm:ss.SSS}</green> <level>{level} | {message}</level>'
LOGGER_LEVEL = "DEBUG"

DATETIME_FORMAT = '%Y-%m-%d %H:%M:%S'

logger.remove()
logger.add(sys.stdout, format=LOGGER_FORMAT, level=LOGGER_LEVEL)
config = ConfigParser()
config.read(API_FILE)


class Tweet:
	def __init__(self, raw_tweet):
		self.id = raw_tweet.id
		self.text = raw_tweet.text
		self.lang = raw_tweet.lang
		self.datetime = raw_tweet.created_at
		self.datetime_str = self.datetime.strftime(DATETIME_FORMAT)
		self.query_values = self.id, self.text, self.datetime_str
		self.is_en = self.lang == 'en'


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
def parse_tweets(tweets):
	new_tweets = 0
	for raw_tweet in tqdm(tweets, desc='Parsing tweets', total=tweets_for_hashtag-1):
		tweet = Tweet(raw_tweet)
		if tweet.is_en:
			query = f'INSERT INTO tweet VALUES (?, ?, ?)'
			try:
				db_connection.execute(query, tweet.query_values)
				new_tweets += 1
			except sqlite3.IntegrityError:
				pass
	return new_tweets


hashtags = read_hashtags(HASHTAGS_FILE)
logger.info(f'Hashtags: {len(hashtags)}')

api_client = get_api_client(config)
del config

db_connection = get_db_connection(OUTPUT_FILE)

tweets_for_hashtag = int(MAX_TWEETS / len(hashtags))
total_new_tweets = 0
for hashtag in hashtags:
	tweets = tweepy.Cursor(api_client.search, q=hashtag).items(tweets_for_hashtag)
	total_new_tweets += parse_tweets(tweets)
	db_connection.commit()

logger.info(f'{total_new_tweets} new tweets')






