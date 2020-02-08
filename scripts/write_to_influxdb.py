from argparse import ArgumentParser

from influxdb import InfluxDBClient
import datetime
import geohash


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
    args = parser.parse_args()

    client = InfluxDBClient(args.hostname, args.port,
                            args.username, args.password,
                            args.database)

    client.create_database(args.database)

    latitude = 33.626675
    longitude = -112.1024746
    current_time = datetime.datetime.now()

    for i in range(10):
        # Generate a Geohash given the coordinates of the aircraft. A geohash
        # allows us to do a "fuzzy search" of all aircraft within the same
        # grid cell. As the geohash precision increases, the size of the grid
        # cells become smaller. I chose 4 here because that allows for a
        # precision of +/- 20 km. That's the closest precision we can get to
        # 9 km, which I think is the closest airplanes are allowed to fly to
        # each other based on a quick search.
        ghash = geohash.encode(latitude, longitude, precision=4)

        # A point is a single row of data in a measurement
        points = [
            {
                # The measurement is analogous to a table in SQL. It's the
                # type of data we're writing.
                "measurement": "location",
                # Tags are fields of the point that are indexed. They're the
                # data we expect to look points up by.
                "tags": {
                    "centre": "ZLA",
                    "flight_number": "N1220W",
                    "geohash": ghash,
                },
                # Time is the time that this point was recorded at
                "time": current_time.isoformat(),
                # Fields are like SQL fields, but unlike tags they are not
                # indexed. This is where the real meat and potatoes of data
                # goes.
                "fields": {
                    "latitude": latitude,
                    "longitude": longitude,
                },
            },
        ]
        client.write_points(points)

        latitude += 0.01
        current_time += datetime.timedelta(0, 1)

    result = client.query("SELECT * FROM location")

    print(f"Result: {result}")


if __name__ == "__main__":
    main()
