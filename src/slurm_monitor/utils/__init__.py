import datetime as dt

def utcnow():
    return dt.datetime.now(tz=dt.timezone.utc)


def ensure_float(value: dict[str, any], key: str, default_value: float) -> float:
    try:
        return float(value[key])
    except (KeyError, ValueError):
        return default_value
