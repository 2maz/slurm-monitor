import datetime as dt

def utcnow() -> dt.datetime:
    return dt.datetime.now(tz=dt.timezone.utc)

def fromtimestamp(time_in_s: float) -> dt.datetime:
    return dt.datetime.fromtimestamp(time_in_s, tz=dt.timezone.utc)

def ensure_utc(date: dt.datetime) -> dt.datetime:
    """
    Prepare plain time in utc, but with tzinfo being set
    """
    if date.tzinfo is not None:
        return date.astimezone(dt.timezone.utc).replace(tzinfo=None)
    return date



def ensure_float(value: dict[str, any], key: str, default_value: float) -> float:
    try:
        return float(value[key])
    except (KeyError, ValueError):
        return default_value
