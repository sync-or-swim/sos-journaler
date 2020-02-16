import os
import sys
from typing import Type

from log import logger


def _from_env(name: str, type_: Type = str, default=None) -> str:
    try:
        return type_(os.environ[name])
    except KeyError:
        if default is None:
            logger.critical(f"Environment variable {name} unset")
            sys.exit(1)
        else:
            return default


rabbitmq_host = _from_env("RABBITMQ_HOST")
"""The hostname of the RabbitMQ server"""

rabbitmq_queue_name = _from_env("RABBITMQ_QUEUE_NAME")
"""The name of the RabbitMQ queue to read FIXM data from"""
