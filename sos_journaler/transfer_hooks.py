from typing import Callable, Dict, List
from collections import defaultdict

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
def add_(point: dict):
    prefix = "flight.enRoute.position.position.location"

    latitude_longitude = point["fields"][f"{prefix}.pos"].split(" ")
    latitude, longitude = map(float, latitude_longitude)

    point["fields"]["latitude"] = latitude
    point["fields"]["longitude"] = longitude

    point["tags"]["geohash"] = geohash.encode(
        latitude, longitude, precision=4)
