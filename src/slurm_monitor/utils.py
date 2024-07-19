import datetime as dt


def utcnow():
    return dt.datetime.now(tz=dt.timezone.utc)
