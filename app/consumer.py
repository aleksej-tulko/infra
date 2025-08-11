import os

TOPIC = os.getenv('TOPIC', 'practice')

mandatory_message_fields = [
    "sender_id", "sender_name",
    "recipient_id", "recipient_name",
    "amount", "content"
]

def consume_infinite_loop(consumer: Consumer) -> None:
    """Получение сообщений из брокера по одному."""
    consumer.subscribe([TOPIC])
    try:
        while True:
            msg = consumer.poll(0.1)

            if msg is None or msg.error():
                continue

            value = json.loads(msg.value().decode('utf-8'))
            if isinstance(value, dict) and (
                all(field in mandatory_message_fields
                    for field in value.keys())
            ):
                consumer.commit(asynchronous=False)

                print(
                    f'Получено сообщение: {msg.key().decode('utf-8')}, '
                    f'{value}, offset={msg.offset()}. '
                    f'Размер сообщения - {len(msg.value())} байтов.'
                )
            else:
                print('Ошибка.')
    except KafkaException as KE:
        raise KafkaError(KE)
    finally:
        consumer.close()