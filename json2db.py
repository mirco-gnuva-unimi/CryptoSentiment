import json
import sqlite3
from datetimeformatter import get_datetime
from tqdm import tqdm


class Message:
    def __init__(self, message: str):
        raw = dict(message)
        self.type = raw['_']
        if not self.is_message():
            return
        self.id = int(raw['id'])
        self.datetime = get_datetime(raw['date'])
        self.date = self.datetime.date() if self.datetime is not None else None
        self.text = raw['message']
        if raw['from_id']['_'] != 'PeerUser':
            self.type = 'NotMessage'
            return
        self.user_id = int(raw['from_id']['user_id'])
        views = raw['views']
        self.views = int(views) if views else None
        forwards = raw['forwards']
        self.forwards = int(forwards) if forwards else None

    def query_params(self) -> tuple:
        param = (self.id, self.datetime, self.date, self.text, self.user_id, self.views, self.forwards)
        return param

    def is_message(self) -> bool:
        return self.type == 'Message'


def empty_queue(queue: list, connection: sqlite3.Connection) -> list:
    connection.executemany(insert_query, queue)
    connection.commit()
    return []


json_path = '/home/wasp97/Desktop/crypto_telegram_groups/group_messages_okex.json'
db_path = '/home/wasp97/Desktop/telegram.sqlite'
table = 'groups_messages'
queue_size = 100000

columns = ['message_id', 'datetime', 'date', 'text', 'user_id', 'views', 'forwards']


columns_str = ', '.join(columns)
values_str = ', '.join(['?'] * len(columns))
insert_query = f'INSERT INTO {table} ({columns_str}) VALUES ({values_str});'


queue = []

with open(json_path, 'r') as json_file, sqlite3.connect(db_path) as conn:
    data = json.load(json_file)

    for line in tqdm(data):
        try:
            message = Message(line)
        except KeyError as ke:
            print(dict(line), '\n', ke, '\n\n')
            exit(1)
        if not message.is_message():
            continue
        queue.append(message.query_params())

        if len(queue) >= queue_size:
            queue = empty_queue(queue, conn)
    empty_queue(queue, conn)
