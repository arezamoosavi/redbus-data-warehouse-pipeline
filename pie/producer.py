import json
import os
import uuid
import random
from time import sleep

from kafka import KafkaProducer

KAFKA_BROKER_URL = os.environ.get("KAFKA_BROKER_URL")
TRANSACTIONS_TOPIC = os.environ.get("TRANSACTIONS_TOPIC")
TRANSACTIONS_PER_SECOND = float(os.environ.get("TRANSACTIONS_PER_SECOND"))

SLEEP_TIME = 1 / TRANSACTIONS_PER_SECOND

if __name__ == "__main__":
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER_URL,
        value_serializer=lambda value: json.dumps(value).encode(),
        linger_ms=100,
        max_request_size=10000,
    )
    """
    It will produce messages with bulk every 100 ms or if msg size got to 10000 record
    """
    while True:
        transaction: dict = {'source': uuid.uuid4().hex[:10], 'target': uuid.uuid4(
        ).hex[:10], 'amount': random.uniform(400, 4000), 'currency': 'EUR'}
        producer.send(TRANSACTIONS_TOPIC, value=transaction)
        sleep(SLEEP_TIME)
        print(str(transaction))
