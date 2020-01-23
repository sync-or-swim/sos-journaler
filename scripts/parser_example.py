import xml.etree.ElementTree as ET
from pathlib import Path
from argparse import ArgumentParser


def main():
    parser = ArgumentParser(
        description="An example script demonstrating how to parse a few "
                    "values out of a FIXM XML file.")
    parser.add_argument("xml_file",
                        type=Path,
                        help="The XML file to parse")

    args = parser.parse_args()

    tree = ET.parse(args.xml_file)
    message_collection = tree.getroot()

    for message in message_collection:
        for flight in message:
            center = flight.attrib["centre"]
            flight_identification = flight.find("flightIdentification")
            flight_number = flight_identification.attrib[
                "aircraftIdentification"]
            timestamp_str = flight.attrib["timestamp"]

            print(f"Center: {center}\n"
                  f"Flight Number: {flight_number}\n"
                  f"Timestamp: {timestamp_str}")

            en_route = flight.find("enRoute")
            if en_route is None:
                print("Data does not have en-route information")
            else:
                pos = (en_route
                       .find("position")
                       .find("position")
                       .find("location")
                       .find("pos"))
                latitude, longitude = pos.text.split(" ")
                print(f"    Lat: {latitude}, Long: {longitude}")


if __name__ == "__main__":
    main()
