#!/usr/bin/env pypy3
import sqlite3
import gtfs_loader
import argparse
import datetime
import math
import json
from .schema import *
from dataclasses import dataclass
from blocks_to_transfers.service_days import ServiceDays
import pprint

NOW = datetime.datetime.now()
MISSING_THRESHOLD = 360 # Mark as missing if more than 6 minutes late
VEHICLE_STATUS_STR = ["INCOMING_AT", "STOPPED_AT", "IN_TRANSIT_TO"]

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

    results = process(gtfs, con, args.date)
    print(json.dumps(results, indent=2))


def process(gtfs, con, raw_service_date):
    trip_observations = get_observations(con, raw_service_date)
    service_date = datetime.datetime.strptime(raw_service_date, '%Y%m%d')
    active_services = get_active_services(gtfs, service_date)
    return get_predictions(gtfs, trip_observations, service_date, active_services)

def get_active_services(gtfs, service_date):
    service_days = ServiceDays(gtfs)
    today_index = (service_date - service_days.epoch).days
    return {service_id for service_id, days in service_days.days_by_service.items() if days[today_index]}


def get_expected_start(service_date, trip):
    return service_date + datetime.timedelta(seconds=trip.first_departure)

def get_trips_by_block(gtfs, active_services):
    trips_by_block = {}
    for trip in sorted(gtfs.trips.values(), key=lambda trip: trip.first_departure):
        if trip.service_id not in active_services:
            continue

        trips_by_block.setdefault(trip.block_id, []).append(trip)

    return trips_by_block




def get_predictions(gtfs, trip_observations, service_date, active_services):
    all_results = {}
    trips_by_block = get_trips_by_block(gtfs, active_services)
    for block_id, trips in trips_by_block.items():
        block_results = all_results[block_id] = {}

        block_status = TripPrediction.SCHEDULED
        for trip in trips:
            observation = trip_observations.get(trip.trip_id, {})
            if observation:
                live_status = predict_from_observation(service_date, trip, observation)
            else:
                live_status = predict_from_schedule(service_date, trip)


            block_status = predict_from_previous_trips(block_status, live_status)

            last_event = max(observation.values(), key=lambda event: event.stop_sequence) if observation else None

            block_results[trip.trip_id] = dict(
                live_status=live_status,
                block_status=block_status,
                route_short_name=trip.route.route_short_name,
                trip_headsign=trip.trip_headsign,
                direction_id=trip.direction_id,
                scheduled_departure=str(trip.first_departure),
                scheduled_arrival=str(trip.last_arrival),
                last_event=describe_last_event(trip, last_event),
                route_color=trip.route.route_color,
                route_text_color=trip.route.route_text_color
            )

    return all_results

def describe_last_event(trip, last_event):
    if not last_event:
        return None   
    seq_ofs = {st.stop_sequence: i for i, st in enumerate(trip._gtfs.stop_times[trip.trip_id])}
    expected_ofs = seq_ofs[last_event.stop_sequence]
    expected_st = trip._gtfs.stop_times[trip.trip_id][expected_ofs]
    return dict(
            stop_name=expected_st.stop.stop_name,
            observed_at=last_event.observed_at,
            vehicle_status=VEHICLE_STATUS_STR[last_event.vehicle_status],
            speed=last_event.speed,
            vehicle_id=last_event.vehicle_id
            )


    

def predict_from_schedule(service_date, trip):
    expected_start = get_expected_start(service_date, trip)
    delay = (NOW - expected_start).total_seconds()
    if delay < MISSING_THRESHOLD:
        return TripPrediction.SCHEDULED
    elif NOW > service_date + datetime.timedelta(seconds=trip.last_arrival):
        return TripPrediction.MISSED
    else:
        return TripPrediction.MISSING


def predict_from_observation(service_date, trip, observation):
    start_seq = trip.first_stop_time.stop_sequence
    end_seq = trip.last_stop_time.stop_sequence
    last_event = max(observation.values(), key=lambda event: event.stop_sequence)

    if last_event.stop_sequence == end_seq and last_event.vehicle_status in {0, 1}:
        # At the end of the journey for sure
        return TripPrediction.ARRIVED

    # Due to refresh rates we might not have an event for the last stop,
    # so figure out if the trip would've ended based on the last observation
    # we do have
    if NOW > get_predicted_end(service_date, trip, last_event):
        return TripPrediction.ARRIVED

    if last_event.stop_sequence == start_seq and last_event.vehicle_status == 1:
        return TripPrediction.WAITING

    return TripPrediction.DEPARTED


def predict_from_previous_trips(block_status, live_status):
    if live_status in {TripPrediction.DEPARTED, TripPrediction.ARRIVED}:
        return TripPrediction.BLOCK_IN_SERVICE
    elif live_status in {TripPrediction.MISSED, TripPrediction.MISSING}:
        return TripPrediction.BLOCK_MISSED
    else:
        return block_status


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

