from loguru import logger
import cbpro
import sys
from datetime import datetime, timedelta


CURRENCIES_FILE = 'cryptocurrencies.txt'
DB_FILE = 'currencies_rates.db'

LOGGER_FORMAT = '<green>{time: YYYY-MM-DD HH:mm:ss.SSS}</green> <level>{level} | {message}</level>'
LOGGER_LEVEL = "DEBUG"

logger.remove()
logger.add(sys.stdout, format=LOGGER_FORMAT, level=LOGGER_LEVEL)

client = cbpro.PublicClient()
SUPPORTED_CURRENCIES = [currency['id'] for currency in client.get_currencies() if currency['details']['type'] == 'crypto']
DATA_HEADER = ['currency', 'time', 'low', 'high', 'open', 'close', 'volume']
DATA_GRANULARITY_OPTIONS = {'1m': 60,
							'5m': 300,
							'15m': 900,
							'1h': 3600,
							'6h': 21600,
							'1d': 86400}
DATA_GRANULARITY = DATA_GRANULARITY_OPTIONS['1d']
SINCE = '2021-01-1 0:0:0'


def read_cryptocurrencies(file_path: str) -> list:
	logger.debug(f'Reading currencies from "{file_path}"')
	with open(file_path, 'r') as file:
		cryptocurrencies = [row.strip().upper() for row in file.readlines()]
	logger.debug(f'{len(cryptocurrencies)} currencies read.')
	return cryptocurrencies


@logger.catch
def get_historic_data(currency: str) -> list:
	max_timedelta = timedelta(seconds=300 * DATA_GRANULARITY)
	latest_end = datetime.strptime(SINCE, '%Y-%m-%d %H:%M:%S')
	now = datetime.now()
	result = []
	pair = f'{currency}-USD'
	logger.debug(f'Pulling data about pair "{pair}" since {latest_end}...')
	while latest_end < now:
		start = latest_end + timedelta(seconds=1)
		end = min(start + max_timedelta, now)
		logger.debug(f'Getting data from {start} to {end}')
		data = client.get_product_historic_rates(pair, start, end, DATA_GRANULARITY)
		logger.debug(f'{len(data)} entries retrieved')
		result.extend(data)
		latest_end = end
	logger.debug(f'{len(result)} data pulled.')
	return result


currencies = read_cryptocurrencies(CURRENCIES_FILE)

not_supported_currencies = [cur for cur in currencies if cur not in SUPPORTED_CURRENCIES]

if not_supported_currencies:
	logger.error(f'Detected currencies not supported by coinbase: {not_supported_currencies}')
	exit(1)

logger.info(f'Will be crawled information about {len(currencies)} currencies')

