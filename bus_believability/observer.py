#!/usr/bin/env pypy3
from . import gtfs_realtime_pb2 as rt
import time
import datetime
import pprint
import sqlite3
import requests
import argparse
import subprocess
from pathlib import Path
from .schema import *


VEHICLE_UPDATES_URL='https://bct.tmix.se/gtfs-realtime/vehicleupdates.pb?operatorIds=20'
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
        vehicles_observed.add(vs.vehicle_id)
        cur.execute(f"INSERT INTO vehicle_updates VALUES {vs.asplaceholder()} ON CONFLICT (start_date,trip_id,stop_sequence) DO UPDATE SET lat=excluded.lat, lon=excluded.lon, speed=excluded.speed, vehicle_status=excluded.vehicle_status, observed_at=excluded.observed_at;", vs.astuple())
        con.commit()

    print(f'Updated {vehicles_observed}')


def main():
    cmd = argparse.ArgumentParser()
    cmd.add_argument('--dir', help='Directory to write database files', type=Path)
    cmd.add_argument('--bucket', help='Bucket for archived database files')
    args = cmd.parse_args()

    sync_data(args.dir, args.bucket)
    db_file = args.dir / f"{datetime.datetime.now().strftime('%Y%m%d')}.db"
    con = sqlite3.connect(db_file)

    apply_schema(con)
    loop(con)

def sync_data(db_dir, bucket_url):
    print('Archiving data...')
    subprocess.run(['gsutil', '-m', 'rsync', db_dir, bucket_url]) 


def apply_schema(con):
    with open(Path(__file__).parent / 'schema.sql') as fp:
        con.execute(fp.read())


def loop(con):
    print('Started @', datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

    sess = requests.Session()
    while True:
        try:
            update_vehicle_positions(sess, con)
        except Exception as exc:
            print(exc)

        time.sleep(20)


if __name__ == '__main__':
    main()
