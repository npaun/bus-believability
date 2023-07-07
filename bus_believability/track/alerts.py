import requests
import datetime
import time
import sqlite3
import argparse
from ..schema import Alert, apply_schema


ALERT_URL = 'https://www.bctransit.com/sites/REST/controller/ServiceAlert/get-alert-list?micrositeid=1520526315921&timezone=Canada/Pacific'
REFRESH = 1800


def main():
    cmd = argparse.ArgumentParser()
    cmd.add_argument('--db', help='Location of the alets database')
    args = cmd.parse_args()
    track_alerts(args.db)


def track_alerts(db_file):
    sess = requests.Session()
    con = sqlite3.connect(db_file)
    apply_schema(con)

    while True:
        try:
            process_alerts(sess, con)
        except Exception as exc:
            print(exc)
    
        time.sleep(REFRESH)

def fetch_alerts(sess):
    res = sess.get(ALERT_URL)
    return res.json()


def process_alerts(sess, con):
    cur = con.cursor()
    raw_alerts = fetch_alerts(sess)
    alerts = []
    for raw_alert in raw_alerts:
        alerts.extend(parse_alert(raw_alert))

    for alert in alerts:
        insert_alert(cur, alert)

    print(f'Update {len(alerts)} alerts')

    con.commit()


def parse_alert(raw_alert):
    now = int(time.time())
    alerts = []
    for route in raw_alert['Routes']:
        alerts.append(Alert(
            status=raw_alert['AlertStatus'],
            route_id=route,
            start_date=parse_start_date(raw_alert['StartDateFormatted']),
            title=raw_alert['Title'],
            alert_id=raw_alert['id'],
            last_seen=now,
            first_seen=now,
        ))

    return alerts


def parse_start_date(start_date):
    dt = datetime.datetime.strptime(start_date, '%B %d, %Y %I:%M %p')
    return int(dt.timestamp())


def insert_alert(cur, alert):
    cur.execute(f"""
    INSERT INTO alerts
    VALUES {alert.asplaceholder()}
    ON CONFLICT (alert_id, route_id, title)
    DO UPDATE SET
        last_seen=excluded.last_seen;
    """, alert.astuple())


if __name__ == '__main__':
    main()
