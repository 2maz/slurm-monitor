import datetime as dt
import pytz

def utcnow():
    return dt.datetime.now(tz=dt.timezone.utc)

def ensure_utc(date: dt.datetime):
    """
    Prepare plain time in utc, but with tzinfo being set
    """
    if date.tzinfo is not None:
        return date.astimezone(pytz.utc).replace(tzinfo=None)
    return date



def ensure_float(value: dict[str, any], key: str, default_value: float) -> float:
    try:
        return float(value[key])
    except (KeyError, ValueError):
        return default_value
