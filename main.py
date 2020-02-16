import logging
import os
from time import sleep

import pika.exceptions
import pyfixm as fixm

import config
from log import logger


def on_fixm_message(channel, method, properties, body):
    message_collection = fixm.parseString(body, silence=True)

    logger.info(f"Got a FIXM message collection from the queue: "
                f"{message_collection}")
    for message in message_collection.message:
        if isinstance(message, fixm.FlightMessageType):
            flight = message.flight
            logger.info(f"Got a flight message from center: {flight.centre}")


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

    # Read FIXM data from the queue
    channel.basic_consume(queue=config.rabbitmq_queue_name,
                          auto_ack=True,
                          on_message_callback=on_fixm_message)
    channel.start_consuming()


if __name__ == "__main__":
    main()
