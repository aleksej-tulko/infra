import logging
import os
import sys
from datetime import datetime, timezone

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
ORDERS = os.getenv('ORDERS', '')
USERS = os.getenv('USERS', '')
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

    ORDER_RECORD = ('Клиент {client} заказал '
                    '{product_name}. Дата: '
                    '{date}.')
    USER_RECORD = ('Пользователь {user}, '
                   'email {email} '
                   'добавлен {date}')


mandatory_orders_fields = [
    "id",
    "user_id",
    "product_name",
    "quantity",
    "order_date"
]

mandatory_users_fields = [
    "id",
    "name",
    "email",
    "created_at",
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

user_id_map = {}


def fetch_user_name(user_id: int) -> str | None:
    try:
        with psycopg2.connect(
            host='localhost',
            dbname=DBNAME, user=USER,
            password=PASSWORD
        ) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    'SELECT name FROM users WHERE id = %s;', (user_id,)
                )
                row = cur.fetchone()
                return row[0] if row else None
    except Exception as e:
        logger.warning("DB lookup failed for user_id=%s: %s", user_id, e)
        return None


def consume_orders(consumer: Consumer) -> None:
    """Получение сообщений из брокера по одному."""
    consumer.subscribe([ORDERS, USERS])
    try:
        while True:
            msg = consumer.poll(0.1)

            if msg is None or msg.error():
                continue

            value = json.loads(msg.value().decode('utf-8')).get('payload', {})

            if not isinstance(value, dict):
                continue

            if msg.topic() == ORDERS and (
                all(field in mandatory_orders_fields
                    for field in value.keys())
            ):

                # id = value.get('user_id')
                # name = user_id_map.get(value.get('user_id'))
                # if name is None:
                #     print('chech')
                #     name = fetch_user_name(id)
                #     if name:
                #         user_id_map[id] = name
                #     else:
                #         name = f'id={id}'
                logger.info(msg=LoggerMsg.ORDER_RECORD.format(
                    client=name,
                    product_name=value.get('product_name'),
                    date=datetime.fromtimestamp(
                        value.get('order_date') / 1e6,
                        tz=timezone.utc
                        ).strftime('%Y-%m-%d %H:%M:%S')
                    )
                )
                consumer.commit(asynchronous=False)
            elif msg.topic() == USERS and (
                all(field in mandatory_users_fields
                    for field in value.keys())
            ):

                id = value.get('id')
                name = value.get('name')
                if id is not None and name:
                    user_id_map[id] = name
                    logger.info(msg=LoggerMsg.USER_RECORD.format(
                        user=value.get('name'),
                        email=value.get('email'),
                        date=datetime.fromtimestamp(
                            value.get('created_at') / 1e6,
                            tz=timezone.utc
                            ).strftime('%Y-%m-%d %H:%M:%S')
                        )
                    )
                    consumer.commit(asynchronous=False)
                    continue
            else:
                print('Error')

    except KafkaException as KE:
        raise KafkaError(KE)
    finally:
        consumer.close()


if __name__ == '__main__':
    """Основной код."""
    while True:
        consume_orders(consumer=consumer)
