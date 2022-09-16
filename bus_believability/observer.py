#!/usr/bin/env pypy3
import urllib.request
import gtfs_realtime_pb2 as rt
import time
import dataclasses
from dataclasses import dataclass
import pprint
import sqlite3


VEHICLE_UPDATES_URL='https://bct.tmix.se/gtfs-realtime/vehicleupdates.pb?operatorIds=20'


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



def update_vehicle_positions(gtfs, con, url=VEHICLE_UPDATES_URL):
    with urllib.request.urlopen(url) as res:
        cur = con.cursor()
        vp = rt.FeedMessage()
        vp.ParseFromString(res.read())
        for entity in vp.entity:
            vehicle = entity.vehicle
            vs = VehicleState(
                    start_date=int(vehicle.trip.start_date) if vehicle.trip.start_date else None,
                    trip_id=vehicle.trip.trip_id,
                    route_id=vehicle.trip.route_id,
                    direction_id=vehicle.trip.direction_id,
                    lat=vehicle.position.latitude,
                    lon=vehicle.position.longitude,
                    speed=3.6*vehicle.position.speed,
                    stop_sequence=vehicle.current_stop_sequence,
                    stop_id=vehicle.stop_id,
                    vehicle_id=vehicle.vehicle.id[-4:],
                    vehicle_status=vehicle.current_status,
                    observed_at=int(time.time())
            )

            cur.execute(f"INSERT INTO vehicle_updates VALUES {vs.asplaceholder()} ON CONFLICT (start_date,trip_id,stop_sequence) DO UPDATE SET lat=excluded.lat, lon=excluded.lon, speed=excluded.speed, vehicle_status=excluded.vehicle_status, observed_at=excluded.observed_at;", vs.astuple())
            con.commit()

def main():
    con = sqlite3.connect("vehicles.db")
    while True:
        update_vehicle_positions(con)
        time.sleep(15)


if __name__ == '__main__':
    main()
