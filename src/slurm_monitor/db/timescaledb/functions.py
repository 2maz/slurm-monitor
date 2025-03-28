from sqlalchemy.sql.functions import GenericFunction


class First(GenericFunction):
    identifier = 'first'
    inherit_cache = True


class Last(GenericFunction):
    identifier = 'last'
    inherit_cache = True

# https://docs.timescale.com/api/latest/hyperfunctions/time_bucket/
class time_bucket(GenericFunction):
    identifier = 'time_bucket'
    inherit_cache = True
