import sqlite3
from tqdm import tqdm

path = '/mnt/hgfs/VMs_Shared/datasets/filtered/twitter/merging_db.sqlite'

buffer_size = 100

buffer = []

with sqlite3.connect(path) as conn, sqlite3.connect(path) as write_conn:
	read_query = 'SELECT ROWID, text  FROM tweet WHERE words IS NULL;'
	write_query = 'UPDATE tweet SET words = ? WHERE ROWID = ?;'
	for record in tqdm(conn.execute(read_query)):
		rowid, tweet = list(record)
		words = len(tweet.split())
		buffer.append((words, rowid))

		if len(buffer) > buffer_size:
			write_conn.executemany(write_query, buffer)
			write_conn.commit()
			buffer = []

	write_conn.executemany(write_query, buffer)
	write_conn.commit()
