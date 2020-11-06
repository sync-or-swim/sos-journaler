from collections import defaultdict
from typing import Callable, Dict, List

import geohash

_hooks: Dict[str, List[Callable[[dict], None]]] = defaultdict(list)


def run_hooks(point: dict) -> None:
    message_type = point["measurement"]

    for hook in _hooks[message_type]:
        hook(point)


def _hook(message_type: str) -> Callable[[Callable], Callable]:
    def decorator(func: Callable) -> Callable:
        _hooks[message_type].append(func)
        return func

    return decorator


@_hook("TH")
def add_geohash(point: dict):
    prefix = "flight.enRoute.position.position.location"

    latitude_longitude = point["fields"][f"{prefix}.pos"].split(" ")
    latitude, longitude = map(float, latitude_longitude)

    point["fields"]["latitude"] = latitude
    point["fields"]["longitude"] = longitude

    ghash = geohash.encode(latitude, longitude, precision=4)
    point["tags"]["geohash"] = ghash
    point["fields"]["geohash"] = ghash


@_hook("TH")
def altitude_to_float(point: dict):
    field_name = "flight.enRoute.position.altitude"
    if field_name in point["fields"]:
        point["fields"][field_name] = float(point["fields"][field_name])


@_hook("TH")
def add_aircraft_id_hook(point: dict):
    field = "flight.flightIdentification.aircraftIdentification"

    point["tags"]["aircraftIdentification"] = point["fields"][field]
