from datetime import datetime, timedelta, UTC
from itertools import islice

ONE_HOUR = timedelta(hours=1)
ONE_SECOND = timedelta(seconds=1)
FIVE_MIN = timedelta(minutes=5)
ONE_DAY = timedelta(days=1)
ALMOST_DAY = timedelta(hours=23, minutes=59, seconds=59)
ONE_HOUR = timedelta(hours=1)
SEVEN_DAYS = timedelta(days=7)
THREE_MONTHS = timedelta(days=31*3)
SIX_MONTHS = timedelta(days=31*6)
ONE_YEAR = timedelta(days=365)

def next_month(d):
    if d.month==12:
        return d.replace(month=1, year=d.year+1)
    return d.replace(month=d.month+1)

def prev_month(d):
    if d.month==1:
        return d.replace(month=12, year=d.year-1)
    return d.replace(month=d.month-1)

def start_of_day(d):
    return d.replace(hour=0, minute=0, second=0, microsecond=0)

def start_of_hour(d):
    return d.replace(minute=0, second=0, microsecond=0)

def start_of_5_minutes(d):
    return d.replace(minute=5*(d.minute//5), second=0, microsecond=0)

def from_now(delta):
    return datetime.utcnow() + delta

def from_now_5_minutes(delta):
    return start_of_5_minutes(datetime.utcnow() + delta)

def start_of_week(d):
    return d - ONE_DAY * d.weekday()

def start_of_month(d):
    return d.replace(day=1)

def ts_range(start, end, delta):
    _start = int(start.replace(tzinfo=UTC).timestamp())
    _end = int(end.replace(tzinfo=UTC).timestamp())
    _delta = int(delta.total_seconds())
    return range(_start, _end, _delta)

def month_range_reverse(start, end, max_count=None):
    current = start_of_month(end)
    cnt = 0
    while (current >= start) if max_count is None else (cnt < max_count):
        yield current
        current = prev_month(current)
        cnt +=1

def week_range_reverse(start, end, max_count=None):
    current = start_of_week(end)
    cnt = 0
    while (current >= start) if max_count is None else (cnt < max_count):
        yield current
        current -= SEVEN_DAYS
        cnt +=1


def day_range_reverse(start, end, max_count=None):
    current = start_of_day(end)
    cnt = 0
    while (current >= start) if max_count is None else (cnt < max_count):
        yield current
        current -= ONE_DAY
        cnt +=1

def hour_range_reverse(start, end, max_count=None):
    current = start_of_hour(end)
    cnt = 0
    while (current >= start) if max_count is None else (cnt < max_count):
        yield current
        current -= ONE_HOUR
        cnt +=1

def five_min_range_reverse(start, end, max_count=None):
    current = start_of_5_minutes(end)
    cnt = 0
    while (current >= start) if max_count is None else (cnt < max_count):
        yield current
        current -= FIVE_MIN
        cnt +=1

def day_range(start, end):
    current = start_of_day(start)
    while current <= end:
        yield current
        current += ONE_DAY

def day_range_limit(start, end):
    return islice(day_range(start, end), 500)

def five_min_range(start, end):
    current = start_of_5_minutes(start)
    while current <= end:
        yield current
        current += FIVE_MIN

def hour_range(start, end):
    current = start_of_hour(start)
    while current <= end:
        yield current
        current += ONE_HOUR

def hour_range_limit(start, end):
    return islice(hour_range(start, end), 500)

def week_range(start, end):
    current = start_of_week(start)
    while current <= end:
        yield current
        current += SEVEN_DAYS

def week_range_limit(start, end):
    return islice(week_range(start, end), 100)

def month_range(start, end):
    current = start_of_month(start)
    while current <= end:
        yield current
        current = next_month(current)

def month_range_limit(start, end):
    return islice(month_range(start, end), 50)
