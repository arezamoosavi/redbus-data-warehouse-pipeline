import os
import json
import logging
import traceback

import faust
from faust.sensors.prometheus import setup_prometheus_sensors

import conf
from db import PgConnector

pg = PgConnector()
table_name = os.getenv("TABLE_NAME")

logger = logging.getLogger(__name__)

app = faust.App(
    id="fraud_detection_consumer",
    stream_buffer_maxsize=conf.MAX_BUFFER_SIZE,
    debug=conf.DEBUG,
    autodiscover=False,
    broker=conf.KAFKA_BOOTSTRAP_SERVER,
    store=conf.STORE_URI,
    consumer_auto_offset_reset="earliest",
    topic_allow_declare=conf.TOPIC_ALLOW_DECLARE,
    topic_disable_leader=conf.TOPIC_DISABLE_LEADER,
    broker_credentials=conf.SSL_CONTEXT,
    logging_config=conf.LOGGING,
)

setup_prometheus_sensors(app=app)
app.web.blueprints.add("/stats", "faust.web.apps.stats:blueprint")

topic_trx = app.topic(
    conf.TRX_TOPIC, acks=conf.OFFSET_ACK_ON_KAFKA, partitions=int(conf.TOPIC_PARTITION))


@app.agent(topic_trx)
async def process_transaction(stream):
    async for batch_event in stream.take(10000, within=10):
        batch_data = []
        for event in batch_event:
            try:
                event["tag"] = "FRAUD" if event["amount"] > 1250 else "LEGIT"
                batch_data.append(tuple(event.values()))
            except:
                trace = traceback.format_exc().replace('\n', '  ')
                logger.error(
                    f"error happened with: {str(trace)} for {str(event)}")
        try:
            col_vals = list(event.keys())
            yield await pg.bulk_insert(table_name, col_vals, batch_data)
            logger.info(
                f"count bulk inserted into pg: {str(len(batch_data))}")
        except:
            trace = traceback.format_exc().replace('\n', '  ')
            logger.error(
                f"bulk insert error: {str(trace)} for {str(event)}")
