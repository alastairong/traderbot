def date_to_iso8601(date):
    return '{year}-{month:02d}-{day:02d}T{hour:02d}:{minute:02d}:{second:02d}'.format(
      year=date.year,
      month=date.month,
      day=date.day,
      hour=date.hour,
      minute=date.minute,
      second=date.second)

def date_to_interval(dt, interval): # Round a datetime DOWN to the nearest interval
    interval = datetime(dt.year, dt.month, dt.day, dt.hour, dt.minute // interval * interval)
    return interval
