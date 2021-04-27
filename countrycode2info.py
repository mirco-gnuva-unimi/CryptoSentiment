# Used to retrieve the "better" name for each county code
import pycountry
import sqlite3
from tqdm import tqdm


def empty_queue(queue: list, connection: sqlite3.Connection) -> list:
    query = f'UPDATE {table} SET country_name = ?, country_id = ? WHERE ROWID = ?'
    connection.executemany(query, queue)
    return []


path = '/home/wasp97/Desktop/worldcitiespop.sqlite'
table = 'city'
queue_size = 100000


queue = []
with sqlite3.connect(path) as read_conn, sqlite3.connect(path) as write_conn:
    query = f'SELECT ROWID, country_code FROM {table} WHERE country_name IS NULL;'
    cursor = read_conn.execute(query)

    for row in tqdm(cursor):
        rowid, code = list(row)
        info = pycountry.countries.get(alpha_2=code)

        if info is None:
            continue
        country_name, country_id = info.name, info.numeric
        queue.append((country_name, country_id, rowid))

        if len(queue) >= queue_size:
            queue = empty_queue(queue, write_conn)

    empty_queue(queue, write_conn)
