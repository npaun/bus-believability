import argparse
import re
import datetime
import sqlite3
from .schema import Alert, NamedEntities, OrdinalSeries, NamedDate, NamedTime, Meridiem, RecognizedAlert

NOW = datetime.datetime.now()

def fake_ner(alert):
    return NamedEntities(
            times=extract_time(alert),
            dates=extract_date(alert),
    )


def extract_time(alert):
    time_strs = re.findall('((\d:?\d:?\d\d?) *(am|pm)?)', alert)
    entities = []
    for time_str in time_strs:
        value, ampm = time_str[1:3]

        clean_value = re.sub('[: ]+', '', value)
        hours = int(clean_value[:-2])
        minutes = int(clean_value[-2:])
        meridiem = Meridiem[ampm.upper()] if ampm else Meridiem.Ambiguous
        entities.extend(expand_time(NamedTime(hours, minutes, meridiem)))

    return entities


MONTHS = {
        'JAN': 1,
        'FEB': 2, 
        'MAR': 3, 
        'APR': 4, 
        'MAY': 5, 
        'JUN': 6, 
        'JUL': 7, 
        'AUG': 8, 
        'SEP': 9, 
        'OCT': 10, 
        'NOV': 11, 
        'DEC': 12,
}

def extract_date(alert):
    date_strs = re.findall('(Jan|January|Feb|February|Mar|March|Apr|April|May|Jun|June|Jul|July|Aug|August|Sep|Sept|September|Oct|October|Nov|November|Dec|December) *(\d\d?) *(st|nd|rd|th)?', alert)
    entities = []

    for date_str in date_strs:
        month, day, ordinal = date_str[0:3]
        int_month = MONTHS.get(month[:3].upper())
        if not int_month:
            continue

        ordinal = OrdinalSeries[ordinal.upper()] if ordinal else OrdinalSeries.Ambiguous
        entities.append(expand_date(NamedDate(
            month=int_month,
            day=int(day),
            ordinal=ordinal
        )))

    return entities


def expand_date(date):
    return datetime.date(
            year=NOW.year if date.month >= (NOW.month - 1) else NOW.year + 1,
            month=date.month,
            day=date.day
    )


def expand_time(time):
    if time.ampm == Meridiem.AM:
        return [datetime.time(time.hours, time.minutes)]
    elif time.ampm == Meridiem.PM:
        return [datetime.time(time.hours + 12 if time.hours != 12 else time.hours, time.minutes)]
    else:
        return [datetime.time(time.hours if time.hours != 12 else 0, time.minutes, 0), datetime.time(time.hours + 12, time.minutes, 0)]


def expand_datetime(entities):
    for date in entities.dates:
        for time in entities.times:
            yield datetime.datetime.combine(date, time)

def recognize_alerts(db_file):
    con = sqlite3.connect(db_file)
    con.row_factory = Alert.fromsql
    alerts = []
    for row in con.execute('SELECT * FROM alerts;'):
        alerts.append(RecognizedAlert(
            alert=row,
            dt=list(expand_datetime(fake_ner(row.title)))
        ))

    return alerts


def link_alerts(trips_by_route, service_date, alerts):
    cancelled_trips = set()
    for alert in alerts:
        trips_for_route = trips_by_route.get(alert.alert.route_id)
        if not trips_for_route or not alert.dt:
            continue

        for dt in alert.dt:
            if dt.date() != service_date.date():
                continue

            for trip in trips_for_route:
                scheduled_dep = datetime.datetime.combine(dt.date(), datetime.time.min) + datetime.timedelta(seconds=trip.first_departure)
                print(scheduled_dep, dt)
                if scheduled_dep == dt:
                    cancelled_trips.add(trip.trip_id)

    return cancelled_trips
