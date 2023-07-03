#!/usr/bin/env pypy3
import sqlite3
import gtfs_loader
import argparse
import datetime
import math
import json
from .schema import *
from . import aggregate
from . import alerts
from typing import Optional
from dataclasses import dataclass
from functools import cached_property
from blocks_to_transfers.service_days import ServiceDays
import pprint

MISSING_THRESHOLD = 360 # Mark as missing if more than 6 minutes late
VEHICLE_STATUS_STR = ["INCOMING_AT", "STOPPED_AT", "IN_TRANSIT_TO"]

def main():
    today = datetime.datetime.now().strftime('%Y%m%d')

    cmd = argparse.ArgumentParser(description='Predict likelihood of a trip running based on RT data')
    cmd.add_argument('--gtfs', help='Directory containing GTFS static feed', required=True)
    cmd.add_argument('--db', help='A SQLite database containing GTFS-RT observations', required=True)
    cmd.add_argument('--alerts', help='A SQLite database containing BCTransit-proprietary alerts', required=True)
    cmd.add_argument('--date', help='Service date in yyyymmdd format', default=today)

    args = cmd.parse_args()
    predictor = Predictor(args.gtfs, args.db, args.alerts, datetime.datetime.strptime(args.date, '%Y%m%d'))
    predictor.update() 
    print(json.dumps(predictor.get_all_blocks(), indent=2))
    #print(json.dumps(predictor.get_departures('20', '1', '160376'), indent=2))

class Predictor:
    def __init__(self, gtfs_path, db_path, alerts_db_path, service_date):
        self.service_date = service_date
        self.gtfs = gtfs_loader.load(gtfs_path)
        self.itineraries = aggregate.get_itineraries(self.gtfs)
        self.stop_index = aggregate.get_stop_index(self.gtfs, self.itineraries)
        self.alerts = alerts.recognize_alerts(alerts_db_path)
        self.cancelled_trips = alerts.link_alerts(self.trips_by_route, self.service_date, self.alerts)
        self.con = sqlite3.connect(db_path)
        self.con.row_factory = VehicleState.fromsql
        self.results = {}

    @cached_property
    def active_services(self):
        service_days = ServiceDays(self.gtfs)
        today_index = (self.service_date - service_days.epoch).days
        return {service_id for service_id, days 
                in service_days.days_by_service.items() if days[today_index]}

    @cached_property
    def active_trips(self):
        active_trips = []
        for trip in sorted(self.gtfs.trips.values(), key=lambda trip: trip.first_departure):
            if trip.service_id not in self.active_services:
                active_trips.append(trip)

        return active_trips


    @cached_property
    def trips_by_block(self):
        trips_by_block = {}
        for trip in self.active_trips:
            trips_by_block.setdefault(trip.block_id, []).append(trip)

        return trips_by_block


    @cached_property
    def trips_by_route(self):
        trips_by_route = {}
        for trip in self.active_trips: 
            trips_by_route.setdefault(trip.route.route_short_name, []).append(trip)

        return trips_by_route


    def fetch_observations(self):
        cur = self.con.cursor()
        trip_observations = {}
        query = 'SELECT * from vehicle_updates WHERE start_date = ?;'
        for row in cur.execute(query, (self.service_date.strftime('%Y%m%d'),)):
            trip_observations.setdefault(row.trip_id, {})[row.stop_sequence] = row

        return trip_observations

    def update(self):
        trip_observations = self.fetch_observations()
        now = datetime.datetime.now()
        self.results.clear()

        for block_id, trips in self.trips_by_block.items():
            block_status = TripPrediction.SCHEDULED
            for trip in trips:
                observations = trip_observations.get(trip.trip_id, {})
                latest_event = (max(observations.values(), key=lambda event: event.stop_sequence) 
                                if observations else None)
                trip_predictor = TripPredictor(trip, self.service_date, latest_event)
                live_status = trip_predictor.live_status
                if trip.trip_id in self.cancelled_trips:
                    live_status = TripPrediction.CANCELLED

                block_status = self._predict_from_previous_trips(block_status, live_status)
                self.results[trip.trip_id] = trip_predictor.get_trip_desc(now, block_status) 

    def get_all_blocks(self):
        all_results = {}
        for block_id, trips in self.trips_by_block.items():
            block_results = all_results[block_id] = {}
            for trip in trips:
                block_results[trip.trip_id] = self.results[trip.trip_id]

        return all_results

    def get_block(self, block_id):
        pass

    def get_departures(self, route_id, direction_id, stop_id):
        departures = []
        for loc in self.stop_index[stop_id]:
            if loc.itin.route != route_id or loc.itin.direction != direction_id:
                continue

            for trip in self.itineraries[loc.itin]:
                if result := self.results.get(trip.trip_id):
                    departures.append(result)


        return sorted(departures, key=lambda t: t['scheduled_departure'])




    def _predict_from_previous_trips(self, block_status, live_status):
        if live_status in {TripPrediction.DEPARTED, TripPrediction.ARRIVED}:
            return TripPrediction.BLOCK_IN_SERVICE
        elif live_status in {TripPrediction.MISSED, TripPrediction.MISSING}:
            return TripPrediction.BLOCK_MISSED
        else:
            return block_status

@dataclass
class TripPredictor:
    trip: any
    service_date: datetime.date
    latest_event: Optional[VehicleState]

    @cached_property
    def scheduled_end(self):
        return self.service_date + datetime.timedelta(seconds=self.trip.last_arrival)

    @cached_property
    def scheduled_start(self):
        return self.service_date + datetime.timedelta(seconds=self.trip.first_departure)

    def status(self, now):
        return self.live_status(now) if self.latest_event else self.scheduled_status(now)

    def scheduled_status(self, now):
        delay = (now - self.scheduled_start).total_seconds()
        if delay < MISSING_THRESHOLD:
            return TripPrediction.SCHEDULED
        elif now > self.scheduled_end:
            return TripPrediction.MISSED

        return TripPrediction.MISSED

    def live_status(self, now): 
        start_seq = self.trip.first_stop_time.stop_sequence
        end_seq = self.trip.last_stop_time.stop_sequence

        if self.latest_event.stop_sequence == end_seq and self.latest_event.vehicle_status in {0, 1}:
            # At the end of the journey for sure
            return TripPrediction.ARRIVED

        # Due to refresh rates we might not have an event for the last stop,
        # so figure out if the trip would've ended based on the last observation
        # we do have
        if now > self.live_end:
            return TripPrediction.ARRIVED

        if self.latest_event.stop_sequence == start_seq and self.latest_event.vehicle_status == 1:
            return TripPrediction.WAITING

        return TripPrediction.DEPARTED

    def get_latest_event_desc(self):
        if not self.latest_event:
            return None

        return dict(
                stop_name=self.latest_stop_time.stop.stop_name,
                observed_at=self.latest_event.observed_at,
                vehicle_status=VEHICLE_STATUS_STR[self.latest_event.vehicle_status],
                speed=self.latest_event.speed,
                vehicle_id=self.latest_event.vehicle_id
        )

    def get_trip_desc(self, now, block_status):
        return dict(
                    live_status=self.status(now),
                    block_status=block_status,
                    route_short_name=self.trip.route.route_short_name,
                    trip_headsign=self.trip.trip_headsign,
                    direction_id=self.trip.direction_id,
                    scheduled_departure=str(self.trip.first_departure),
                    scheduled_arrival=str(self.trip.last_arrival),
                    latest_event=self.get_latest_event_desc(),
                    route_color=self.trip.route.route_color,
                    route_text_color=self.trip.route.route_text_color,
                    block_id=self.trip.block_id,
                    trip_id=self.trip.trip_id,
                )

    @cached_property
    def live_delay(self):
        next_scheduled = self.service_date + datetime.timedelta(seconds=self.latest_stop_time.arrival_time)
        observed_at = datetime.datetime.fromtimestamp(self.latest_event.observed_at)
        delay = (observed_at - next_scheduled).total_seconds()
        return max(0, delay) # Model isn't very good for early arrivals; would need to look at earlier events

    @cached_property
    def live_end(self):
        return self.scheduled_end + datetime.timedelta(seconds=self.live_delay)

    @cached_property
    def stop_times_by_seq(self):
        return {st.stop_sequence: st 
                for st in self.trip._gtfs.stop_times[self.trip.trip_id]}

    @cached_property
    def latest_stop_time(self):
        return self.stop_times_by_seq[self.latest_event.stop_sequence]

if __name__ == '__main__':
    main()

