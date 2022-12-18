#!/usr/bin/env pypy3
import sqlite3
import gtfs_loader
import argparse
import datetime
import math
from .schema import *
from blocks_to_transfers.service_days import ServiceDays
import pprint

NOW = datetime.datetime.now()

def main():
    today = datetime.datetime.now().strftime('%Y%m%d')

    cmd = argparse.ArgumentParser(description='Predict likelihood of a trip running based on RT data')
    cmd.add_argument('--gtfs', help='Directory containing GTFS static feed', required=True)
    cmd.add_argument('--db', help='A SQLite database containing GTFS-RT observations', required=True)
    cmd.add_argument('--date', help='Service date in yyyymmdd format', default=today)

    args = cmd.parse_args()

    gtfs = gtfs_loader.load(args.gtfs)
    con = sqlite3.connect(args.db)
    con.row_factory = VehicleState.fromsql

    process(gtfs, con, args.date)


def process(gtfs, con, raw_service_date):
    trip_observations = get_observations(con, raw_service_date)
    service_date = datetime.datetime.strptime(raw_service_date, '%Y%m%d')
    active_services = get_active_services(gtfs, service_date)
    get_predictions(gtfs, trip_observations, service_date, active_services)

def get_active_services(gtfs, service_date):
    service_days = ServiceDays(gtfs)
    today_index = (service_date - service_days.epoch).days
    return {service_id for service_id, days in service_days.days_by_service.items() if days[today_index]}


def get_expected_start(service_date, trip):
    return service_date + datetime.timedelta(seconds=trip.first_departure)


def get_predictions(gtfs, trip_observations, service_date, active_services):
    res = {}

    trips_by_block = {}
    for trip in sorted(gtfs.trips.values(), key=lambda trip: trip.first_departure):
        if trip.service_id not in active_services:
            continue

        trips_by_block.setdefault(trip.block_id, []).append(trip)


    for block_id, trips in trips_by_block.items():
        print(f'Block {block_id}')
        previous_status = TripCompletion.SCHEDULED

        for trip in trips:
            observation = trip_observations.get(trip.trip_id)
            status = predict_unobserved(service_date, trip, observation)
            status = predict_block(status, previous_status)
            status = predict_observed(status, service_date, trip, observation)

            previous_status = status
            print(f'{map_status(status):10} {str(trip.first_departure)[:-3]} - {str(trip.last_arrival)[:-3]} [{trip.route.route_short_name:>3}-{trip.direction_id}] {trip.trip_headsign}')


        print('')



def map_status(status):
    if status == TripCompletion.SCHEDULED:
        return '⏳'

    if status == TripCompletion.DEPARTED:
        return '🚍'

    if status == TripCompletion.ARRIVED:
        return '✅'

    if status == TripCompletion.MISSED:
        return '❌'

    if status == TripCompletion.BLOCK_MISSED:
        return 'Block ⚠️'

    if status == TripCompletion.BLOCK_IN_SERVICE:
        return 'Block ✅'



def predict_unobserved(service_date, trip, observation):
    if observation:
        return TripCompletion.SCHEDULED

    expected_start = get_expected_start(service_date, trip)
    delay = (NOW - expected_start).total_seconds()
    if delay < 240:
        return TripCompletion.SCHEDULED
    else:
        return TripCompletion.MISSED


def predict_block(status, previous_status):
    if status != TripCompletion.SCHEDULED:
        return status

    if previous_status in {TripCompletion.BLOCK_IN_SERVICE, TripCompletion.DEPARTED, TripCompletion.ARRIVED}:
        return TripCompletion.BLOCK_IN_SERVICE

    if previous_status in {TripCompletion.BLOCK_MISSED, TripCompletion.MISSED}:
        return TripCompletion.BLOCK_MISSED

    return TripCompletion.SCHEDULED


def predict_observed(status, service_date, trip, observation):
    if not observation:
        return status

    last_seq = trip.last_stop_time.stop_sequence
    last_event = max(observation.values(), key=lambda event: event.stop_sequence)

    if last_event.stop_sequence == last_seq and last_event.vehicle_status in {1, 2}:
        # At the end of the journey for sure
        return TripCompletion.ARRIVED

    # Due to refresh rates we might not have an event for the last stop,
    # so figure out if the trip would've ended based on the last observation
    # we do have
    if NOW > get_predicted_end(service_date, trip, last_event):
        return TripCompletion.ARRIVED


    return TripCompletion.DEPARTED


def get_predicted_end(service_date, trip, last_event):
    seq_ofs = {st.stop_sequence: i for i, st in enumerate(trip._gtfs.stop_times[trip.trip_id])}
    expected_ofs = seq_ofs[last_event.stop_sequence]
    expected_st = trip._gtfs.stop_times[trip.trip_id][expected_ofs]
    expected_arrival = service_date + datetime.timedelta(seconds=expected_st.arrival_time)
    actual_arrival = datetime.datetime.fromtimestamp(last_event.observed_at)
    delay = int((actual_arrival - expected_arrival).total_seconds())
    expected_end = service_date + datetime.timedelta(seconds=delay) + datetime.timedelta(seconds=trip.last_arrival)
    return expected_end



def get_observations(con, service_date):
    cur = con.cursor()
    trip_observations = {}
    query = 'SELECT * from vehicle_updates WHERE start_date = ?;'
    for row in cur.execute(query, (service_date,)):
        trip_observations.setdefault(row.trip_id, {})[row.stop_sequence] = row

    return trip_observations



if __name__ == '__main__':
    main()

