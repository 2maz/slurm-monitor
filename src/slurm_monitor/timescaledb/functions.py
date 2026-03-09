from sqlalchemy.sql.functions import GenericFunction
from sqlalchemy.ext.compiler import compiles
from sqlalchemy import TIMESTAMP

class first(GenericFunction):
    identifier = 'first'
    inherit_cache = True

class last(GenericFunction):
    identifier = 'last'
    inherit_cache = True

# https://docs.timescale.com/api/latest/hyperfunctions/time_bucket/
class time_bucket(GenericFunction):
    identifier = 'time_bucket'

    type = TIMESTAMP()
    inherit_cache = True

# For TimeScaledb
@compiles(time_bucket, 'timescaledb')
def compile_time_bucket_timescaledb(expr, compiler, **kwargs):
    time_window = expr.clauses.clauses[0].value
    time_column = f"{compiler.process(expr.clauses.clauses[1], **kwargs)}"

    if type(time_window) is int:
        return f"time_bucket('{time_window} seconds',{time_column})"
    else:
        return f"time_bucket('{time_window}',{time_column}"
