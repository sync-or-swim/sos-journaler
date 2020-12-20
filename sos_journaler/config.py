import os
import sys
from typing import Type

from .log import logger


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

influxdb_hostname = _from_env("INFLUXDB_HOSTNAME")
"""The hostname (domain or IP address) of InfluxDB"""

influxdb_port = _from_env("INFLUXDB_PORT", type_=int, default=8086)
"""The port to connect to InfluxDB on"""

influxdb_database = _from_env("INFLUXDB_DB")
"""The database to write FIXM data to"""

influxdb_username = _from_env("INFLUXDB_USER")
"""The username to log into InfluxDB with"""

influxdb_password = _from_env("INFLUXDB_USER_PASSWORD")
"""The password to log into InfluxDB with"""

influxdb_retention_duration = _from_env("INFLUXDB_RETENTION_DURATION")
"""The length of time to keep data for"""

message_handler_threads = _from_env("MESSAGE_HANDLER_THREADS",
                                    type_=int,
                                    default=1)
"""The number of threads to process messages on"""
