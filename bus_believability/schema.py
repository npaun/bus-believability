import dataclasses
from dataclasses import dataclass
from enum import Enum


@dataclass
class VehicleState:
    # Identify trip
    start_date: str
    trip_id: str

    # Redundant with trip but useful for some cases
    route_id: str
    direction_id: int

    #  Position
    lat: float
    lon: float
    speed: float # km/h

    # Current heading
    stop_sequence: int
    stop_id: str

    # Vehicle
    vehicle_id: str
    vehicle_status: int

    # Observation
    observed_at: int

    def astuple(self):
        return dataclasses.astuple(self)

    @classmethod
    def asplaceholder(cls):
        n_fields = len(dataclasses.fields(cls))
        placeholders = ','.join('?'*n_fields)
        return f'({placeholders})'

    @staticmethod
    def fromsql(cur, row):
        row_dict = {k[0]: v for k, v in zip(cur.description, row)}
        return VehicleState(**row_dict)


class TripPrediction(str, Enum):
    # Scheduled to run; awaiting further information
    SCHEDULED = "SCHEDULED"

    # At originating bus stop
    WAITING = "WAITING"

    # Left originating bus stop
    DEPARTED = "DEPARTED"

    # Arrived at terminus
    ARRIVED = "ARRIVED"

    # No information available, should be running 
    MISSING = "MISSING"

    # Did not operate
    MISSED = "MISSED"

    # Cancelled by agency
    CANCELLED = "CANCELLED"

    # An earlier trip operated as scheduled
    BLOCK_IN_SERVICE = "BLOCK_IN_SERVICE"

    # An earlier trip did not run 
    BLOCK_MISSED = "BLOCK_MISSED"

    # An earlier trip was cancelled by agency
    BLOCK_LIKELY_CANCELLED = "BLOCK_LIKELY_CANCELLED"

    # Don't show block-based prediction; live status is best
    IGNORE_BLOCK = "IGNORE_BLOCK"




