import logging
import os

from distutils.util import strtobool

logger = logging.getLogger(__name__)

KAFKA_BOOTSTRAP_SERVER = os.getenv("KAFKA_BROKER_URL")
STORE_URI = os.getenv("STORE_URI", "memory://")

TOPIC_ALLOW_DECLARE = strtobool(os.getenv("TOPIC_ALLOW_DECLARE", "True"))
TOPIC_DISABLE_LEADER = strtobool(os.getenv("TOPIC_DISABLE_LEADER", "False"))

SSL_ENABLED = False
SSL_CONTEXT = None
OFFSET_ACK_ON_KAFKA = strtobool(os.getenv("OFFSET_ACK_ON_KAFKA", "False"))

DEBUG = strtobool(os.getenv("DEBUG", "False"))
MAX_BUFFER_SIZE = int(os.getenv("MAX_BUFFER_SIZE", 50000))

LOGGING = {
    "disable_existing_loggers": False,
    "merge": True,
    "formatters": {
        'colored': {
            '()': 'mode.utils.logging.DefaultFormatter',
            'format': "%(asctime)s | %(module)s | %(levelname)s | %(funcName)s | %(message)s",
        },
        'default': {
            '()': 'mode.utils.logging.DefaultFormatter',
            'format': "%(asctime)s | %(filename)s | %(levelname)s | %(funcName)s | %(message)s",
        }
    }
}

TOPIC_PARTITION = os.getenv("TOPIC_PARTITION", 8)

TRX_TOPIC = os.getenv("TRANSACTIONS_TOPIC")
