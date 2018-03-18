from datetime import datetime, timedelta

def date_to_iso8601(date):
    return '{year}-{month:02d}-{day:02d}T{hour:02d}:{minute:02d}:{second:02d}'.format(
      year=date.year,
      month=date.month,
      day=date.day,
      hour=date.hour,
      minute=date.minute,
      second=date.second)

def date_to_datestring(date):
  return '{year}-{month:02d}-{day:02d}'.format(
    year=date.year,
    month=date.month,
    day=date.day)

def date_to_interval(dt, interval):
    rounding = interval // 60
    hourly = dt.hour // rounding * rounding
    time_period = datetime(dt.year, dt.month, dt.day, hourly, 0)
    return time_period

# WIP code to make the date_to_interval function work for all types of intervals
def date_to_interval_2(dt, interval): # Round a datetime DOWN to the nearest interval
    round_to = timedelta(minutes=interval).total_seconds()
    seconds = (dt - dt.min).seconds
    rounding = seconds // round_to * round_to

    return dt + timedelta(0, rounding - seconds, -dt.microsecond)
