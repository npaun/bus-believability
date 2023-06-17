CREATE TABLE IF NOT EXISTS vehicle_updates (start_date int, trip_id text, route_id text, direction_id int, lat real, lon real, speed real, stop_sequence int, stop_id text, vehicle_id text, vehicle_status int, observed_at int, unique(start_date, trip_id, stop_sequence));
