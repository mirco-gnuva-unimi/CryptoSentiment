import os
import csv

FOLDER = '/mnt/hgfs/VMs_Shared/datasets/filtered'
OUTPUT_FILE = 'merged.csv'
HEADER = []

files = ['bitcoin_filtered.csv',
		 'df_Final_filtered.csv',
		 'Bitcoin_tweets_filtered.csv',
		 'BitcoinTweets_filtered.csv',
		 'tweets_filtered.csv',
		 'bitcoin-twitter_filtered.csv',
		 'twits_BTC_filtered.csv',
		 'BTC_tweets_daily_example_filtered.csv',
		 'twitter2_filtered.csv',
		 'cleanprep_filtered.csv']


def merged_headers() -> set:
	merged = set()
	for filename in files:
		with open(os.path.join(FOLDER, filename), 'r') as file:
			reader = csv.reader(file)
			for line in reader:
				merged.update(line)
				break
	return merged



