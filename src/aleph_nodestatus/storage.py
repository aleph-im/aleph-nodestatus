import plyvel
import time
import ujson as json
import os
from pathlib import Path
from .settings import settings

class Storage:
    def __init__(self, db_path, db_name):
        self.db_path = os.path.join(db_path, db_name)
        Path(self.db_path).mkdir(parents=True, exist_ok=True)
        self.db = plyvel.DB(self.db_path, create_if_missing=True)

    # async def __aenter__(self):
    #     return self

    # async def __aexit__(self, exc_type, exc_val, exc_tb):
    #     self.db.close()

    def close(self):    
        self.db.close()

    async def store_entry(self, key, data, prefix="item"):
        key = f'{prefix}:{key}'
        existing_entry = self.db.get(key.encode())
        if existing_entry is None:
            json_data = json.dumps(data)
            self.db.put(key.encode(), json_data.encode())

    async def retrieve_entries(self, start_key=None, end_key=None, prefix="item"):
        for key, value in self.db.iterator(prefix=f'{prefix}:'.encode()):
            key = key.decode().split(':')[1]
            if start_key is not None and key < start_key:
                continue
            if end_key is not None and key > end_key:
                continue

            json_data = value.decode()
            data = json.loads(json_data)
            yield (key, data)

    async def get_last_available_key(self, prefix="item"):
        last_key = None
        for key, _ in self.db.iterator(prefix=f'{prefix}:'.encode(), reverse=True):
            val = key.decode().split(':')[1]
            if last_key is None or val > last_key:
                last_key = val
        return last_key

def get_dbs():
    return {
        'erc20': Storage(settings.db_path, 'erc20'),
        'messages': Storage(settings.db_path, 'messages'),
        'balances': Storage(settings.db_path, 'balances'),
        'scores': Storage(settings.db_path, 'scores'),
    }

def close_dbs(dbs):
    for db in dbs.values():
        db.close()

# # Example usage
# db_path = '/path/to/your/database'
# storage = Storage(db_path)

# # Store an entry
# timestamp = time.time()
# data = 'Some data'
# storage.store_entry(timestamp, data)

# # Retrieve entries between two dates
# start_date = '2022-01-01'
# end_date = '2022-01-31'
# entries = storage.retrieve_entries(start_date, end_date)
# for timestamp, data in entries:
#     print(f'Timestamp: {timestamp}, Data: {data}')

# # Get the last available date
# last_date = storage.get_last_available_date()
# print(f'Last available date: {last_date}')