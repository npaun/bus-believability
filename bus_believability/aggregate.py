import pprint
import collections
from dataclasses import dataclass

@dataclass(frozen=True)
class ItineraryIndex:
    route: str
    direction: str
    counter: int


@dataclass(frozen=True)
class StopLocator:
    itin: ItineraryIndex
    ofs: int


def get_itineraries(gtfs):
    cell_cols = gtfs.stop_times._resolved_fields.keys() - {'trip_id', 'arrival_time', 'departure_time'}
    patterns = {}
    for trip_id, stop_times in gtfs.stop_times.items():
        cells = tuple(tuple(st[col] for col in cell_cols) 
                      for st in stop_times)

        patterns.setdefault(cells, []).append(gtfs.trips[trip_id])

    itineraries = {}
    n_by_route_and_direction = collections.Counter()
    for trips in patterns.values():
        sample_trip = trips[0] 
        route = sample_trip.route.route_short_name
        direction = sample_trip.direction_id
        n_by_route_and_direction[(route, direction)] += 1
        counter = n_by_route_and_direction[(route, direction)]
        itineraries[ItineraryIndex(route, direction, counter)] = sorted(trips, key=lambda t: t.first_departure)
    
    return itineraries

def get_stop_index(gtfs, itineraries):
    stop_index = {}
    for itinerary_id, trips in itineraries.items():
        sample_trip = trips[0]
        for ofs, cell in enumerate(gtfs.stop_times[sample_trip.trip_id]):
            stop_index.setdefault(cell.stop_id, []).append(StopLocator(itinerary_id, ofs))

    return stop_index

