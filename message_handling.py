import pyfixm as fixm
from influxdb import InfluxDBClient

import transfer


class FIXMMessageHandler:
    """Saves received FIXM data to InfluxDB."""

    def __init__(self, db: InfluxDBClient):
        self._db = db

    def on_message(self, _channel, _method, _properties, body):
        message_collection = fixm.parseString(body, silence=True)

        for message in message_collection.message:
            if isinstance(message, fixm.FlightMessageType):
                if message.flight.source == "HP":
                    point = transfer.hp_message(message)
                    self._db.write_points([point.to_dict()])
                elif message.flight.source == "TH":
                    point = transfer.th_message(message)
                    self._db.write_points([point.to_dict()])
