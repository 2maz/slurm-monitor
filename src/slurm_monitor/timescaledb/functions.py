from sqlalchemy.sql.functions import GenericFunction


class first(GenericFunction):
    identifier = "first"
    inherit_cache = True


class last(GenericFunction):
    identifier = "last"
    inherit_cache = True


# https://docs.timescale.com/api/latest/hyperfunctions/time_bucket/
class time_bucket(GenericFunction):
    identifier = "time_bucket"
    inherit_cache = True
