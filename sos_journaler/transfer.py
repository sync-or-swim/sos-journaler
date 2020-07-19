import xml.etree.ElementTree as ET
import re
from typing import List

import geohash
import pyfixm as fixm

_NAMESPACE_PATTERN = re.compile(r"\{.*\}")
"""Matches to XML namespaces as they are formatted by the ElementTree parser"""


def message_to_point(message: ET.Element) -> dict:
    """
    :param message: A single FIXM message from a MessageCollection
    :return: An InfluxDB point
    """
    tags = {}
    fields = {}

    def add_to_output(prefix, value):
        name = ".".join(prefix)

        if name in fields:
            print(fields)
            raise RuntimeError(f"Oh no! Non-unique name {name}")

        fields[name] = value

    def add_item(item: ET.Element, prefix: List[str]):
        # Remove the namespace from the tag name
        item.tag = _NAMESPACE_PATTERN.sub("", item.tag)

        # Add the body of the tag
        if item.text is not None and item.text.strip() != "":
            add_to_output(prefix, item.text)

        # Add all the tag's attributes
        for name, value in item.attrib.items():
            name = _NAMESPACE_PATTERN.sub("", name)
            add_to_output(prefix + [name], value)

        children = {}
        # Add a number to any children with duplicate names
        for child in item:
            if child.tag in children:
                duplicate_count = 0
                while child.tag + str(duplicate_count) in children:
                    duplicate_count += 1
                child.tag += str(duplicate_count)

            children[child.tag] = child

        # Recursively add all children of the tag
        for name, child in children.items():
            add_item(child, prefix + [name])

    message.tag = _NAMESPACE_PATTERN.sub("", message.tag)
    add_item(message, [])

    return {
        "measurements": fields["flight.source"],
        "tags": tags,
        "time": fields["flight.timestamp"],
        "fields": fields,
    }


class FIXMPoint:
    """Holds FIXM data formatted to be written to InfluxDB."""

    def __init__(self, measurement: str, time: str):
        """
        :param measurement: Which measurement (table in SQL terms) this should
            be saved to
        :param time: The time this data took place at, in ISO 8601 format
        """
        self.measurement = measurement
        self.time = time
        self.tags = {}
        self.fields = {}

    def to_dict(self):
        """Converts to a format that InfluxDB can save."""
        return {
            "measurement": self.measurement,
            "tags": self.tags,
            "time": self.time,
            "fields": self.fields,
        }

    def check_types(self):
        """Checks to make sure all fields and tags are of a format InfluxDB can
        save.
        """
        all_items = list(self.fields.items()) + list(self.tags.items())
        for name, val in all_items:
            if not isinstance(val, (str, float, int)):
                raise ValueError(f"Item {name} is of invalid type {type(val)}")

    def _set_arrival(self, obj: fixm.NasArrivalType):
        self.fields["arrival.point"] = obj.arrivalPoint
        if obj.runwayPositionAndTime is not None:
            self._set_runway_position_and_time(
                obj=obj.runwayPositionAndTime,
                name="arrival")

    def _set_runway_position_and_time(
            self, obj: fixm.RunwayPositionAndTimeType, name: str):
        if obj.runwayTime.actual is not None:
            self.fields[f"{name}.type"] = "actual"
            self.fields[f"{name}.time"] = \
                obj.runwayTime.actual.time.isoformat()
        elif obj.runwayTime.estimated is not None:
            self.fields[f"{name}.type"] = "estimated"
            self.fields[f"{name}.time"] = \
                obj.runwayTime.estimated.time.isoformat()

    def _set_flight_identification(
            self, obj: fixm.NasFlightIdentificationType):
        self.fields["flightIdentification.computerId"] = obj.computerId
        self.fields["flightIdentification.siteSpecificPlanId"] = \
            obj.siteSpecificPlanId
        self.fields["flightIdentification.aircraftIdentification"] = \
            obj.aircraftIdentification

    def _set_supplemental_data(self, obj: fixm.NasSupplementalDataType):
        namespace = "supplementalData.additionalFlightInformation"

        for name_value in obj.additionalFlightInformation.nameValue:
            if name_value.name == "FDPS_GUFI":
                self.fields[f"{namespace}.FDPS_GUFI"] = name_value.value
            elif name_value.name == "FLIGHT_PLAN_SEQ_NO":
                self.fields[f"{namespace}.FLIGHT_PLAN_SEQ_NO"] = \
                    name_value.value


class HPPoint(FIXMPoint):
    """Converts a FIXM message from the HP source to an InfluxDB point."""

    def __init__(self, message: fixm.FlightMessageType):
        flight: fixm.NasFlightType = message.flight
        super().__init__(measurement="HP", time=flight.timestamp.isoformat())

        self.tags["centre"] = flight.centre
        # Effectively an alias for "centre"
        self.tags["center"] = flight.centre

        if flight.arrival is not None:
            self._set_arrival(flight.arrival)

        if flight.flightIdentification is not None:
            self._set_flight_identification(flight.flightIdentification)

        self.tags["flightStatus"] = flight.flightStatus.fdpsFlightStatus
        self.fields["gufi"] = flight.gufi.valueOf_

        if flight.supplementalData is not None:
            self._set_supplemental_data(flight.supplementalData,)

        self._set_coordination(flight.coordination)

        # In accordance with the requirements doc, we're renaming this field
        self.fields["ERAM_GUFI"] = flight.flightPlan.identifier

        self.check_types()

    def _set_coordination(self, obj: fixm.NasCoordinationType):
        self.fields["coordination.coordinationTime"] = \
            obj.coordinationTime.isoformat()
        self.fields["coordination.coordinationTimeHandling"] = \
            obj.coordinationTimeHandling
        if isinstance(obj.coordinationFix, fixm.LocationPointType):
            self.fields["coordination.coordinationFix.latitude"] = \
                obj.coordinationFix.location.pos[0]
            self.fields["coordination.coordinationFix.longitude"] = \
                obj.coordinationFix.location.pos[1]
        elif isinstance(obj.coordinationFix, fixm.RelativePointType):
            # TODO: Do we want this information?
            pass


class THPoint(FIXMPoint):
    """Converts a FIXM message from the TH source to an InfluxDB point."""

    def __init__(self, message: fixm.FlightMessageType):
        flight: fixm.NasFlightType = message.flight
        super().__init__(measurement="TH", time=flight.timestamp.isoformat())

        self.tags["centre"] = flight.centre
        # Effectively an alias for "centre"
        self.tags["center"] = flight.centre

        if flight.arrival is not None:
            self._set_arrival(flight.arrival)

        if flight.controllingUnit is not None:
            self.fields["controllingUnit.unitIdentifier"] = \
                flight.controllingUnit.unitIdentifier
            self.fields["controllingUnit.sectorIdentifier"] = \
                flight.controllingUnit.sectorIdentifier

        if flight.departure is not None:
            if flight.departure.departurePoint is not None:
                self.fields["departure.point"] = \
                    flight.departure.departurePoint
            if flight.departure.runwayPositionAndTime is not None:
                self._set_runway_position_and_time(
                    obj=flight.departure.runwayPositionAndTime,
                    name="departure")

        if flight.enRoute is not None:
            self._set_en_route(flight.enRoute)

        if flight.flightIdentification is not None:
            self._set_flight_identification(flight.flightIdentification)

        self.tags["flightStatus"] = flight.flightStatus.fdpsFlightStatus
        self.fields["gufi"] = flight.gufi.valueOf_
        if flight.operator is not None:
            self.tags["operator"] = \
                flight.operator.operatingOrganization.organization.name

        if flight.supplementalData is not None:
            self._set_supplemental_data(flight.supplementalData)

        if flight.assignedAltitude is not None \
                and flight.assignedAltitude.simple is not None:
            self.fields["assignedAltitude"] = \
                float(flight.assignedAltitude.simple.valueOf_)

        # In accordance with the requirements doc, we're renaming this field
        self.fields["ERAM_GUFI"] = flight.flightPlan.identifier

        self.check_types()

    def _set_en_route(self, obj: fixm.NasEnRouteType):
        latitude = float(obj.position.position.location.pos[0])
        longitude = float(obj.position.position.location.pos[1])
        ghash = geohash.encode(latitude, longitude, precision=4)

        self.fields["enRoute.latitude"] = latitude
        self.fields["enRoute.longitude"] = longitude
        self.tags["enRoute.geohash"] = ghash

        self.fields["enRoute.reportSource"] = obj.position.reportSource
        if obj.position.targetPosition is not None:
            self.fields["enRoute.targetLatitude"] = \
                obj.position.targetPosition.pos[0]
            self.fields["enRoute.targetLongitude"] = \
                obj.position.targetPosition.pos[1]
        if obj.position.altitude is not None:
            self.fields["enRoute.altitude"] = \
                float(obj.position.altitude.valueOf_)
        if obj.position.targetAltitude is not None:
            self.fields["enRoute.targetAltitude"] = \
                float(obj.position.targetAltitude.valueOf_)
        if obj.position.actualSpeed is not None:
            self.fields["enRoute.actualSpeed"] = \
                float(obj.position.actualSpeed.surveillance.valueOf_)
        if obj.position.targetPositionTime is not None:
            self.fields["enRoute.targetPositionTime"] = \
                obj.position.targetPositionTime.isoformat()
        if obj.position.positionTime is not None:
            self.fields["enRoute.positionTime"] = \
                obj.position.positionTime.isoformat()
        if obj.position.trackVelocity is not None:
            self.fields["enRoute.trackVelocity.x"] = \
                float(obj.position.trackVelocity.x.valueOf_)
            self.fields["enRoute.trackVelocity.y"] = \
                float(obj.position.trackVelocity.y.valueOf_)
