from time import sleep

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
    channel.start_consuming()


if __name__ == "__main__":
    main()
