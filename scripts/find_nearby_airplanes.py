import itertools
from argparse import ArgumentParser
from datetime import datetime

import geohash
from geopy.distance import geodesic
from influxdb import InfluxDBClient


def main():
    parser = ArgumentParser(
        description="Writes some example FIXM data to InfluxDB")
    parser.add_argument("--hostname", type=str, default="localhost",
                        help="The hostname of InfluxDB")
    parser.add_argument("--port", type=int, default=8086,
                        help="The port to connect to InfluxDB on")
    parser.add_argument("--username", type=str, default="root",
                        help="The username to authenticate with InfluxDB")
    parser.add_argument("--password", type=str, default="root",
                        help="The password to authenticate with InfluxDB")
    parser.add_argument("--database", type=str, default="fixm",
                        help="The name of the database to write to")
    parser.add_argument("--minimum-distance", type=float, default=10.0,
                        help="The minimum allowed distance between aircraft, "
                             "in miles")
    parser.add_argument("--maximum-time-difference", type=float, default=5.0,
                        help="The maximum allowed time difference between "
                             "comparable records")
    parser.add_argument("--duration", type=str, default="1m",
                        help="How far back to look for nearby aircraft. "
                             "Format the duration as a number followed by 'h' "
                             "for hours, 'm' for minutes, or 's' for seconds.")
    args = parser.parse_args()

    # Establish a connection to InfluxDB
    client = InfluxDBClient(args.hostname, args.port,
                            args.username, args.password,
                            args.database)

    # Get all geohashes with aircraft currently in them
    result = client.query(f"SELECT geohash FROM fixm.autogen.TH "
                          f"WHERE time > now() - {args.duration}")
    result = result.get_points("TH")
    ghashes = {r["geohash"] for r in result}

    for ghash in ghashes:
        # Start the query by getting location and identification data from
        # TH records
        query = (
            "SELECT latitude, longitude, aircraftIdentification "
            "FROM fixm.autogen.TH WHERE ({geohash_conditions}) "
            "AND time > now() - {duration}"
        )

        # Narrow the query down to select only aircraft that are in this
        # geohash and all immediately adjacent geohashes
        nearby_ghashes = geohash.neighbors(ghash) + [ghash]
        geohash_conditions = " OR ".join(
            f"geohash = '{n}'" for n in nearby_ghashes)

        query = query.format(
            geohash_conditions=geohash_conditions,
            duration=args.duration,
        )

        # Get the query results
        result = client.query(query)
        result = result.get_points("TH")
        result = list(result)

        # Compare all aircraft in this area to all other nearby aircraft
        for aircraft1, aircraft2 in itertools.product(result, repeat=2):
            aircraft_id1 = aircraft1['aircraftIdentification']
            aircraft_id2 = aircraft2['aircraftIdentification']

            if aircraft_id1 != aircraft_id2:
                distance = get_distance(aircraft1, aircraft2)
                time_difference = get_time_difference(aircraft1, aircraft2)

                if distance < args.minimum_distance \
                        and time_difference <= args.maximum_time_difference:
                    print(f"Aircraft {aircraft_id1} and {aircraft_id2} are "
                          f"{distance} miles apart")


def get_distance(aircraft1: dict, aircraft2: dict) -> float:
    """
    :param aircraft1: TH message for the first aircraft
    :param aircraft2: TH message for the second aircraft
    :return: The distance between the aircraft in miles
    """
    aircraft1_location = aircraft1["latitude"], aircraft1["longitude"]
    aircraft2_location = aircraft2["latitude"], aircraft2["longitude"]
    distance = geodesic(aircraft1_location, aircraft2_location).miles

    return distance


def get_time_difference(aircraft1: dict, aircraft2: dict) -> float:
    """
    :param aircraft1: TH message for the first aircraft
    :param aircraft2: TH message for the second aircraft
    :return: The difference in reporting time between the two messages, in
        seconds
    """
    time1 = datetime.strptime(aircraft1["time"], "%Y-%m-%dT%H:%M:%S.%fZ")
    time2 = datetime.strptime(aircraft2["time"], "%Y-%m-%dT%H:%M:%S.%fZ")

    return (time1 - time2).seconds


if __name__ == "__main__":
    main()
