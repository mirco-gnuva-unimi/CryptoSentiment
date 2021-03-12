import textblob
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
def parse_tweet(tweet) -> dict:
	descriptor = {'id': tweet.id, 'text': tweet.text, 'lang': tweet.lang, 'datetime': tweet.created_at}
	return descriptor


hashtags = read_hashtags(HASHTAGS_FILE)
logger.info(f'Hashtags: {len(hashtags)}')

api_client = get_api_client(config)
del config

db_connection = get_db_connection(OUTPUT_FILE)

tweets_for_hashtag = int(MAX_TWEETS / len(hashtags))
new_tweets = 0
for hashtag in hashtags:
	tweets = tweepy.Cursor(api_client.search, q=hashtag).items(tweets_for_hashtag)
	for tweet in tqdm(tweets, desc='Parsing tweets'):
		tweet_desc = parse_tweet(tweet)
		if tweet_desc['lang'] == 'en':
			query = f'INSERT INTO tweet VALUES (?, ?, ?)'
			try:
				db_connection.execute(query, (tweet_desc["id"], tweet_desc["text"], tweet_desc["datetime"].strftime(DATETIME_FORMAT)))
				new_tweets += 1
			except sqlite3.IntegrityError:
				pass
	db_connection.commit()

logger.info(f'{new_tweets} new tweets')






