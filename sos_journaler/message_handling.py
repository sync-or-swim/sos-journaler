import xml.etree.ElementTree as ET

from influxdb import InfluxDBClient

from .transfer import message_to_point


class FIXMMessageHandler:
    """Saves received FIXM data to InfluxDB."""

    def __init__(self, db: InfluxDBClient):
        self._db = db

    def on_message(self, _channel, _method, _properties, body):
        message_collection = ET.fromstring(body)

        for message in message_collection:
            point = message_to_point(message)
            self._db.write_points([point])
