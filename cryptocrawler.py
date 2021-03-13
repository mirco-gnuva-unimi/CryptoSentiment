from loguru import logger
import cbpro
import sys
from datetime import datetime, timedelta
import sqlite3
from math import ceil


CURRENCIES_FILE = 'cryptocurrencies.txt'
DB_FILE = 'currencies_rates.db'

LOGGER_FORMAT = '<green>{time: YYYY-MM-DD HH:mm:ss.SSS}</green> <level>{level} | {message}</level>'
LOGGER_LEVEL = "DEBUG"
TIME_FORMAT = '%Y-%m-%d %H:%M:%S'

logger.remove()
logger.add(sys.stdout, format=LOGGER_FORMAT, level=LOGGER_LEVEL)

client = cbpro.PublicClient()
SUPPORTED_CURRENCIES = [currency['id'] for currency in client.get_currencies() if currency['details']['type'] == 'crypto']
DATA_HEADER = ['currency', 'time', 'low', 'high', 'open', 'close', 'volume']
DATA_GRANULARITY_MAPPING = {'1m': 60,
							'5m': 300,
							'15m': 900,
							'1h': 3600,
							'6h': 21600,
							'1d': 86400}
DATA_GRANULARITY = DATA_GRANULARITY_MAPPING['1d']
SINCE = '2021-01-1 0:0:0'


def read_cryptocurrencies(file_path: str) -> list:
	logger.debug(f'Reading currencies from "{file_path}"')
	with open(file_path, 'r') as file:
		cryptocurrencies = [row.strip().upper() for row in file.readlines()]
	logger.debug(f'{len(cryptocurrencies)} currencies read.')
	return cryptocurrencies


@logger.catch
def get_historic_data(currency: str) -> list:
	now = datetime.now()
	max_timedelta = timedelta(seconds=300 * DATA_GRANULARITY)
	since = datetime.strptime(SINCE, TIME_FORMAT)
	time_frames = ceil((now - since) / max_timedelta)
	since -= timedelta(seconds=1)
	time_frames_list = [(since + i * max_timedelta + timedelta(seconds=1), min(since + (i+1)*max_timedelta, now))
						for i in range(time_frames)]

	result = []
	pair = f'{currency}-USD'
	logger.debug(f'Pulling data about pair "{pair}" since {since}...')

	for time_frame in time_frames_list:
		start = time_frame[0]
		end = time_frame[1]
		logger.debug(f'Getting data from {start} to {end}')
		api_data = client.get_product_historic_rates(pair, start, end, DATA_GRANULARITY)
		data = [[currency, datetime.fromtimestamp(raw_data[0]).strftime(TIME_FORMAT)] + raw_data[1:] for raw_data in api_data]
		logger.debug(f'{len(data)} entries retrieved')
		result.extend(data)
	logger.debug(f'{len(result)} data pulled.')
	return result


@logger.catch
def get_db_connection(db_file: str) -> sqlite3.Connection:
	connection = sqlite3.connect(db_file)
	return connection


if __name__ == '__main__':
	currencies = read_cryptocurrencies(CURRENCIES_FILE)

	not_supported_currencies = [cur for cur in currencies if cur not in SUPPORTED_CURRENCIES]

	if not_supported_currencies:
		logger.error(f'Detected currencies not supported by coinbase: {not_supported_currencies}')
		exit(1)

	logger.info(f'Will be crawled information about {len(currencies)} currencies')

	db_connection = get_db_connection(DB_FILE)
	query = 'INSERT INTO currency_rate values (?,?,?,?,?,?,?)'
	new_entries = 0

	try:
		for currency in currencies:
			data = get_historic_data(currency)
			for entry in data:
				try:
					db_connection.execute(query, entry)
					new_entries += 1
				except sqlite3.IntegrityError:
					logger.warning(f'{currency}({entry[1]}) already in database')
		db_connection.commit()
	except KeyboardInterrupt:
		db_connection.commit()

	logger.info(f'{new_entries} new database entries')
