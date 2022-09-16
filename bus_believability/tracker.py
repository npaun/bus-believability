import urllib.request
import gtfs_realtime_pb2 as rt
import gtfs_loader

VEHICLE_UPDATES_URL='https://bct.tmix.se/gtfs-realtime/vehicleupdates.pb?operatorIds=20'

def get_vehicle_positions(url=VEHICLE_UPDATES_URL):
    gtfs = gtfs_loader.load('../static')
    with urllib.request.urlopen(url) as res:
        vp = rt.FeedMessage()
        vp.ParseFromString(res.read())
        for entity in vp.entity:
            vehicle = entity.vehicle
            print(f'{vehicle.trip.route_id}-{vehicle.trip.direction_id}')
            print(vehicle.trip.trip_id)
            print(vehicle.trip.start_date)
            print(f'{vehicle.position.latitude}, {vehicle.position.longitude}')
            print(f'{3.6*vehicle.position.speed} km/h')
            print(f'#{vehicle.current_stop_sequence}')

            if vehicle.trip.trip_id and vehicle.current_stop_sequence:
                if vehicle.trip.trip_id not in gtfs.trips:
                    print('<unknown trip>')
                else:
                    trip = gtfs.trips[vehicle.trip.trip_id]
                    print(trip.trip_headsign)
                    st = gtfs.stop_times[vehicle.trip.trip_id][vehicle.current_stop_sequence-1]
                    print(st.stop.stop_name)

            if vehicle.stop_id:
                print(gtfs.stops[vehicle.stop_id].stop_name, vehicle.stop_id)

            print(rt.VehiclePosition.VehicleStopStatus.Name(vehicle.current_status))
            print(vehicle.vehicle)
            print('')

get_vehicle_positions()
