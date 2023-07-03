#!/usr/bin/env pypy3
from . import gtfs_realtime_pb2 as rt
import time
import datetime
import sqlite3
import requests
import argparse
import subprocess
from dataclasses import dataclass
from typing import Optional
from pathlib import Path
from ..schema import VehicleState, apply_schema


VEHICLE_UPDATES_URL = 'https://bct.tmix.se/gtfs-realtime/vehicleupdates.pb?operatorIds=20'
ROOT = Path(__file__).parent.parent


def update_vehicle_positions(sess, con, url=VEHICLE_UPDATES_URL):
    res = sess.get(url)
    cur = con.cursor()
    vp = rt.FeedMessage()
    vp.ParseFromString(res.content)
    vehicles_observed = set()
    for entity in vp.entity:
        vehicle = entity.vehicle
        vs = VehicleState(
            start_date=int(
                vehicle.trip.start_date) if vehicle.trip.start_date else None,
            trip_id=vehicle.trip.trip_id,
            route_id=vehicle.trip.route_id,
            direction_id=vehicle.trip.direction_id,
            lat=vehicle.position.latitude,
            lon=vehicle.position.longitude,
            speed=3.6*vehicle.position.speed,
            stop_sequence=vehicle.current_stop_sequence,
            stop_id=vehicle.stop_id,
            vehicle_id=vehicle.vehicle.id,
            vehicle_status=vehicle.current_status,
            observed_at=int(time.time())
        )
        vehicles_observed.add(vs.vehicle_id)
        cur.execute(f"""
        INSERT INTO vehicle_updates
        VALUES {vs.asplaceholder()}
        ON CONFLICT (start_date, trip_id, stop_sequence)
        DO UPDATE SET
            lat=excluded.lat,
            lon=excluded.lon,
            speed=excluded.speed,
            vehicle_status=excluded.vehicle_status,
            observed_at=excluded.observed_at;
        """, vs.astuple())
        con.commit()

    print(f'Updated {vehicles_observed}')


def main():
    cmd = argparse.ArgumentParser()
    cmd.add_argument(
        '--dir', help='Directory to write database files', type=Path)
    cmd.add_argument('--bucket', help='Bucket for archived database files')
    args = cmd.parse_args()
    db_rotator = DBRotator(args.dir, args.bucket)
    loop(db_rotator)


def sync_data(db_dir, bucket_url):
    print('Archiving data...')
    subprocess.run(['gsutil', '-m', 'rsync', db_dir, bucket_url])





@dataclass
class DBRotator:
    db_dir: Path
    bucket_url: str
    date: Optional[datetime.date] = None
    con: Optional[sqlite3.Connection] = None

    def connect(self):
        now = datetime.date.today()
        if self.date == now:
            return self.con

        print(f'Rotating database {self.date} -> {now}')
        self.date = now

        sync_data(self.db_dir, self.bucket_url)
        db_file = self.db_dir / f"{self.date.strftime('%Y%m%d')}.db"
        self.con = sqlite3.connect(db_file)
        apply_schema(self.con)
        return self.con


def loop(db_rotator):
    print('Started @', datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

    sess = requests.Session()
    while True:
        try:
            con = db_rotator.connect()
            update_vehicle_positions(sess, con)
        except Exception as exc:
            print(exc)

        time.sleep(20)


if __name__ == '__main__':
    main()
