import logging
from argparse import ArgumentParser
from time import sleep
import os

import pika.exceptions

logger = logging.getLogger("sos-journaler")
logging.basicConfig(level=os.environ.get("LOGLEVEL", "INFO"))


def on_fixm_message(channel, method, properties, body):
    logger.info(f"Got a FIXM message from the queue")


def main():
    args = get_arguments()

    connection_params = pika.ConnectionParameters(host=args.rabbitmq_host)

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
    channel.queue_declare(queue=args.rabbitmq_queue_name)

    # Read FIXM data from the queue
    channel.basic_consume(queue=args.rabbitmq_queue_name,
                          auto_ack=True,
                          on_message_callback=on_fixm_message)
    channel.start_consuming()


def get_arguments():
    parser = ArgumentParser(
        description="Writes FIXM data from a RabbitMQ queue to an InfluxDB "
                    "database")

    parser.add_argument("--rabbitmq-host", type=str, required=True,
                        help="The hostname of the RabbitMQ server")
    parser.add_argument("--rabbitmq-queue-name", type=str, required=True,
                        help="The name of the RabbitMQ queue to read FIXM "
                             "data from")

    return parser.parse_args()


if __name__ == "__main__":
    main()
