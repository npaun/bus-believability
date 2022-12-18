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


class TripCompletion(Enum):
    SCHEDULED = 0
    DEPARTED = 1
    ARRIVED = 2
    MISSED = 3
    BLOCK_IN_SERVICE = 4
    BLOCK_MISSED = 5

class TripPerformance(Enum):
    UNKNOWN = 0
    EARLY = 1
    ON_TIME = 2
    LATE = 3


@dataclass
class TripStatus:
    completion: TripCompletion = TripCompletion.SCHEDULED
    performance: TripPerformance = TripPerformance.UNKNOWN

