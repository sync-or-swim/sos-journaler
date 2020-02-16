import geohash
import pyfixm as fixm

from influxdb_utils import Point


def hp_message(message) -> Point:
    """Converts a FIXM message from the HP source to an InfluxDB point."""
    flight = message.flight

    point = Point(measurement="HP", time=flight.timestamp.isoformat())

    point.tags["centre"] = flight.centre
    # Effectively an alias for "centre"
    point.tags["center"] = flight.centre

    if flight.arrival is not None:
        _arrival(flight.arrival, point)

    if flight.flightIdentification is not None:
        _flight_identification(
            flight.flightIdentification,
            point)

    point.tags["flightStatus"] = flight.flightStatus.fdpsFlightStatus
    point.fields["gufi"] = flight.gufi.valueOf_

    if flight.supplementalData is not None:
        _supplemental_data(flight.supplementalData, point)

    _coordination(flight.coordination, point)

    # In accordance with the requirements doc, we're renaming this field
    point.fields["ERAM_GUFI"] = flight.flightPlan.identifier

    point.check_types()
    return point


def th_message(message):
    """Converts a FIXM message from the TH source to an InfluxDB point."""
    flight = message.flight

    point = Point(measurement="TH", time=flight.timestamp.isoformat())

    point.tags["centre"] = flight.centre
    # Effectively an alias for "centre"
    point.tags["center"] = flight.centre

    if flight.arrival is not None:
        _arrival(flight.arrival, point)

    if flight.controllingUnit is not None:
        point.fields["controllingUnit.unitIdentifier"] = \
            flight.controllingUnit.unitIdentifier
        point.fields["controllingUnit.sectorIdentifier"] = \
            flight.controllingUnit.sectorIdentifier

    if flight.departure is not None:
        point.fields["departure.point"] = flight.departure.departurePoint
        if flight.departure.runwayPositionAndTime is not None:
            _runway_position_and_time(
                obj=flight.departure.runwayPositionAndTime,
                point=point,
                name="departure")

    if flight.enRoute is not None:
        _en_route(flight.enRoute, point)

    if flight.flightIdentification is not None:
        _flight_identification(
            flight.flightIdentification,
            point)

    point.tags["flightStatus"] = flight.flightStatus.fdpsFlightStatus
    point.fields["gufi"] = flight.gufi.valueOf_
    if flight.operator is not None:
        point.tags["operator"] = \
            flight.operator.operatingOrganization.organization.name

    if flight.supplementalData is not None:
        _supplemental_data(flight.supplementalData, point)

    if flight.assignedAltitude is not None \
            and flight.assignedAltitude.simple is not None:
        point.fields["assignedAltitude"] = \
            float(flight.assignedAltitude.simple.valueOf_)

    # In accordance with the requirements doc, we're renaming this field
    point.fields["ERAM_GUFI"] = flight.flightPlan.identifier

    point.check_types()
    return point


def _arrival(obj, point):
    point.fields["arrival.point"] = obj.arrivalPoint
    if obj.runwayPositionAndTime is not None:
        _runway_position_and_time(
            obj=obj.runwayPositionAndTime,
            point=point,
            name="arrival")


def _en_route(obj, point):
    latitude = float(obj.position.position.location.pos[0])
    longitude = float(obj.position.position.location.pos[1])
    ghash = geohash.encode(latitude, longitude, precision=4)

    point.fields["enRoute.latitude"] = latitude
    point.fields["enRoute.longitude"] = longitude
    point.tags["enRoute.geohash"] = ghash

    point.fields["enRoute.reportSource"] = obj.position.reportSource
    if obj.position.targetPosition is not None:
        point.fields["enRoute.targetLatitude"] = \
            obj.position.targetPosition.pos[0]
        point.fields["enRoute.targetLongitude"] = \
            obj.position.targetPosition.pos[1]
    if obj.position.altitude is not None:
        point.fields["enRoute.altitude"] = \
            float(obj.position.altitude.valueOf_)
    if obj.position.targetAltitude is not None:
        point.fields["enRoute.targetAltitude"] = \
            float(obj.position.targetAltitude.valueOf_)
    if obj.position.actualSpeed is not None:
        point.fields["enRoute.actualSpeed"] = \
            float(obj.position.actualSpeed.surveillance.valueOf_)
    if obj.position.targetPositionTime is not None:
        point.fields["enRoute.targetPositionTime"] = \
            obj.position.targetPositionTime.isoformat()
    if obj.position.positionTime is not None:
        point.fields["enRoute.positionTime"] = \
            obj.position.positionTime.isoformat()
    if obj.position.trackVelocity is not None:
        point.fields["enRoute.trackVelocity.x"] = \
            float(obj.position.trackVelocity.x.valueOf_)
        point.fields["enRoute.trackVelocity.y"] = \
            float(obj.position.trackVelocity.y.valueOf_)


def _runway_position_and_time(obj, point, name):
    if obj.runwayTime.actual is not None:
        point.fields[f"{name}.type"] = "actual"
        point.fields[f"{name}.time"] = \
            obj.runwayTime.actual.time.isoformat()
    elif obj.runwayTime.estimated is not None:
        point.fields[f"{name}.type"] = "estimated"
        point.fields[f"{name}.time"] = \
            obj.runwayTime.estimated.time.isoformat()


def _flight_identification(obj, point):
    point.fields["flightIdentification.computerId"] = obj.computerId
    point.fields["flightIdentification.siteSpecificPlanId"] = \
        obj.siteSpecificPlanId
    point.fields["flightIdentification.aircraftIdentification"] = \
        obj.aircraftIdentification


def _supplemental_data(obj, point):
    namespace = "supplementalData.additionalFlightInformation"

    for name_value in obj.additionalFlightInformation.nameValue:
        if name_value.name == "FDPS_GUFI":
            point.fields[f"{namespace}.FDPS_GUFI"] = name_value.value
        elif name_value.name == "FLIGHT_PLAN_SEQ_NO":
            point.fields[f"{namespace}.FLIGHT_PLAN_SEQ_NO"] = \
                name_value.value


def _coordination(obj, point):
    point.fields["coordination.coordinationTime"] = \
        obj.coordinationTime.isoformat()
    point.fields["coordination.coordinationTimeHandling"] = \
        obj.coordinationTimeHandling
    if type(obj.coordinationFix) is fixm.LocationPointType:
        point.fields["coordination.coordinationFix.latitude"] = \
            obj.coordinationFix.location.pos[0]
        point.fields["coordination.coordinationFix.longitude"] = \
            obj.coordinationFix.location.pos[1]
    elif type(obj.coordinationFix) == fixm.RelativePointType:
        # TODO: Do we want this information?
        pass
