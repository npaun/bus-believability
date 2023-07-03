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
        entities.append(NamedTime(hours, minutes, meridiem))

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
        entities.append(NamedDate(
            month=int_month,
            day=int(day),
            ordinal=ordinal
        ))

    return entities


def resolve_alert(alert):
    for route in alert.routes:
        for date in alert.entities.dates:
            res_date = datetime.date(
                    year=NOW.year if date.month >= (NOW.month - 1) else NOW.year + 1,
                    month=date.month,
                    day=date.day
            )
            for time in alert.entities.times:
                res_times = expand_time(time)
                for res_time in res_times:
                    print('Guess', route, res_date, res_time)


def expand_time(time):
    if time.ampm == Meridiem.AM:
        return [datetime.time(time.hours, time.minutes)]
    elif time.ampm == Meridiem.PM:
        return [datetime.time(time.hours + 12 if time.hours != 12 else time.hours, time.minutes)]
    else:
        return [datetime.time(time.hours if time.hours != 12 else 0, time.minutes, 0), datetime.time(time.hours + 12, time.minutes, 0)]


def parse_alert(alert):
    return Alert(
            status=alert['AlertStatus'],
            routes=alert['Routes'],
            start_date=alert['StartDateFormatted'],
            title=alert['Title'],
            entities=fake_ner(alert['Title']),
            alert_id=alert['id']
    )


