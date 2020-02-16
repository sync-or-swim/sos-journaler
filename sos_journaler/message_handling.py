import pyfixm as fixm
from influxdb import InfluxDBClient

from .transfer import HPPoint, THPoint


class FIXMMessageHandler:
    """Saves received FIXM data to InfluxDB."""

    def __init__(self, db: InfluxDBClient):
        self._db = db

    def on_message(self, _channel, _method, _properties, body):
        message_collection = fixm.parseString(body, silence=True)

        for message in message_collection.message:
            if isinstance(message, fixm.FlightMessageType):
                if message.flight.source == "HP":
                    point = HPPoint(message)
                    self._db.write_points([point.to_dict()])
                elif message.flight.source == "TH":
                    point = THPoint(message)
                    self._db.write_points([point.to_dict()])
