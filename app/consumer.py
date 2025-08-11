import logging
import os
import sys
from datetime import datetime

import json
from confluent_kafka import (
    Consumer, KafkaError, KafkaException
)
from dotenv import load_dotenv
import psycopg2

load_dotenv()

ACKS_LEVEL = os.getenv('ACKS_LEVEL', 'all')
AUTOOFF_RESET = os.getenv('AUTOCOMMIT_RESET', 'earliest')
ENABLE_AUTOCOMMIT = os.getenv('ENABLE_AUTOCOMMIT', False)
FETCH_MIN_BYTES = os.getenv('FETCH_MIN_BYTES', 1)
FETCH_WAIT_MAX_MS = os.getenv('FETCH_WAIT_MAX_MS', 100)
RETRIES = os.getenv('RETRIES', '3')
SESSION_TIME_MS = os.getenv('SESSION_TIME_MS', 1_000)
LINGER_MS = os.getenv('LINGER_MS', 0)
TOPIC = os.getenv('TOPIC', 'practice')
USER = os.getenv('POSTGRES_USER', '')
PASSWORD = os.getenv('POSTGRES_PASSWORD', '')
DBNAME = os.getenv('POSTGRES_DB', '')

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)


class LoggerMsg:
    """Сообщения для логгирования."""

    BLOCK_RECORD = ('Заказчик {client} заблокировал '
                    'отправителей {blocked_users}.')


mandatory_message_fields = [
    "id", "user_id",
    "product_name", "quantity",
    "order_date"
]

conf = {
    "bootstrap.servers":
    "localhost:9094,localhost:9095,localhost:9096",
    "auto.offset.reset": AUTOOFF_RESET,
    "enable.auto.commit": ENABLE_AUTOCOMMIT,
    "session.timeout.ms": SESSION_TIME_MS,
    "group.id": "online",
    "fetch.min.bytes": FETCH_MIN_BYTES,
    "fetch.wait.max.ms": FETCH_WAIT_MAX_MS
}

consumer = Consumer(conf)

conn = psycopg2.connect(
    host='localhost',
    dbname=DBNAME,
    user=USER,
    password=PASSWORD
)
cur = conn.cursor()
cur.execute('select name, id from users;')
rows = cur.fetchall()

user_id_map = {}

for name, id in rows:
    user_id_map[id] = name


def consume_infinite_loop(consumer: Consumer) -> None:
    """Получение сообщений из брокера по одному."""
    consumer.subscribe([TOPIC])
    try:
        while True:
            msg = consumer.poll(0.1)

            if msg is None or msg.error():
                continue

            value = json.loads(msg.value().decode('utf-8')).get('payload', {})

            if isinstance(value, dict) and (
                all(field in mandatory_message_fields
                    for field in value.keys())
            ):
                consumer.commit(asynchronous=False)

                print(
                    f'Клиент {user_id_map[value.get('user_id')]} '
                    f'заказал {value.get('product_name')}. '
                    f'Дата: {datetime.fromtimestamp(value.get('order_date') / 1e6)}.'
                )
            else:
                print('Ошибка.')
    except KafkaException as KE:
        raise KafkaError(KE)
    finally:
        consumer.close()


if __name__ == '__main__':
    """Основной код."""
    while True:
        consume_infinite_loop(consumer=consumer)
