import signal
from time import sleep
from typing import Callable

import pika.exceptions

from sos_journaler import config, logger, FIXMMessageHandler
from sos_journaler.database import FIXMDatabase


def main():
    connection_params = pika.ConnectionParameters(host=config.rabbitmq_host)

    # Connect to RabbitMQ with a retry mechanism
    connection = None
    while connection is None:
        try:
            connection = pika.BlockingConnection(connection_params)
        except pika.exceptions.AMQPConnectionError:
            logger.error("Connection error while connecting to RabbitMQ. "
                         "Retrying...")
            sleep(1)
    logger.info("Connected to RabbitMQ")

    channel = connection.channel()

    # Create the queue if it doesn't already exist
    channel.queue_declare(queue=config.rabbitmq_queue_name)

    # Connect to database
    db = FIXMDatabase()

    # Read FIXM data from the queue
    handler = FIXMMessageHandler(db)
    channel.basic_consume(queue=config.rabbitmq_queue_name,
                          auto_ack=True,
                          on_message_callback=handler.on_message)

    # Close gracefully on Docker exit
    setup_graceful_exit(handler.close)

    channel.start_consuming()


def setup_graceful_exit(exit_func: Callable) -> None:
    """Connect graceful shutdown"""
    signal.signal(signal.SIGINT, lambda *_args: exit_func())
    signal.signal(signal.SIGTERM, lambda *_args: exit_func())


if __name__ == "__main__":
    main()
