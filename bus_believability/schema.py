import datetime
import dataclasses
from dataclasses import dataclass
from enum import Enum
from pathlib import Path


class SQLAdapter:
    def astuple(self):
        return dataclasses.astuple(self)

    @classmethod
    def asplaceholder(cls):
        n_fields = len(dataclasses.fields(cls))
        placeholders = ','.join('?'*n_fields)
        return f'({placeholders})'

    @classmethod
    def fromsql(cls, cur, row):
        row_dict = {k[0]: v for k, v in zip(cur.description, row)}
        return cls(**row_dict)


@dataclass
class VehicleState(SQLAdapter):
    # Identify trip
    start_date: str
    trip_id: str

    # Redundant with trip but useful for some cases
    route_id: str
    direction_id: int

    #  Position
    lat: float
    lon: float
    speed: float  # km/h

    # Current heading
    stop_sequence: int
    stop_id: str

    # Vehicle
    vehicle_id: str
    vehicle_status: int

    # Observation
    observed_at: int


class Meridiem(Enum):
    Ambiguous = -1
    AM = 1
    PM = 2


class OrdinalSeries(Enum):
    Ambiguous = -1
    TH = 0
    ST = 1
    ND = 2
    RD = 3


@dataclass
class NamedTime:
    hours: int
    minutes: int
    ampm: Meridiem


@dataclass
class NamedDate:
    month: int
    day: int
    ordinal: OrdinalSeries


@dataclass
class NamedEntities:
    times: list[datetime.time]
    dates: list[datetime.date]

@dataclass
class Alert(SQLAdapter):
    alert_id: int
    route_id: str
    title: str  # The raw text
    status: str
    start_date: int  # Unix time
    first_seen: int  # Unix time
    last_seen: int  # Unix time


@dataclass
class RecognizedAlert:
    alert: Alert
    dt: list[datetime.datetime]

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


def apply_schema(con):
    with open(Path(__file__).parent / 'schema.sql') as fp:
        con.executescript(fp.read())
