import xml.etree.ElementTree as ET
from queue import Queue, Empty
from threading import Thread
import logging
from typing import List

from influxdb import InfluxDBClient

from .transfer import message_to_point
from . import config


class FIXMMessageHandler:
    """Saves received FIXM data to InfluxDB."""

    _logger = logging.getLogger("sos_journaler.transfer")

    def __init__(self, db: InfluxDBClient):
        self._db = db
        self._messages = Queue()
        self._running = True

        # Spin up all messaging handling threads
        self._message_processors: List[Thread] = []
        for i in range(config.message_handler_threads):
            thread = Thread(
                name=f"Message Processor {i}",
                daemon=True,
                target=self._process_messages,
            )
            thread.start()
            self._message_processors.append(thread)

    def on_message(self, _channel, _method, _properties, body) -> None:
        """Handle a received message"""
        message_collection = ET.fromstring(body)
        for message in message_collection:
            self._messages.put(message)

    def close(self) -> None:
        """Clean up all threads"""
        self._logger.info("Stopping message processing threads")
        self._running = True
        for thread in self._message_processors:
            thread.join()

    def _process_messages(self) -> None:
        """Once 50 points have accumulated, batch write them to DB"""
        points = []

        while self._running:
            try:
                message = self._messages.get(timeout=0.01)
            except Empty:
                continue

            point = message_to_point(message)
            points.append(point)

            if len(points) >= 50:
                self._db.write_points(points)
                points.clear()

                # Logging to catch issues with queue build-up
                queue_size = self._messages.qsize()
                if queue_size > 100:
                    message = f"The point saving threads are running behind " \
                              f"by {queue_size} points"
                    self._logger.warning(message)
