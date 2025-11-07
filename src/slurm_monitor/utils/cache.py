from cachetools import TTLCache
from cachetools.keys import hashkey
import functools

def hashkey_generator(*args, **kwargs):
    """
    Generate a hash key that handles list and set
    while ignoring *self*
    """
    updated_args = []

    # Make the result instance independant
    # so ignore (self) in the hashing
    for x in args[1:]:
        if type(x) is list or type(x) is set:
            updated_args.append(hash(tuple(sorted(x))))
        else:
            updated_args.append(x)

    updated_kwargs = {}
    for k,v in kwargs.items():
        if type(v) is list or type(v) is set:
            updated_kwargs[k] = hash(tuple(sorted(v)))
        else:
            updated_kwargs[k] = v

    return hashkey(*updated_args, **updated_kwargs)


def ttl_cache_async(*, ttl, maxsize: int = 128):
    """
    Enable a timeout cache for an async function

    At the same time handle the limitation of cachetools to deal with list
    and set arguments
    """
    cache = TTLCache(maxsize=maxsize, ttl=ttl)

    def decorator(func):
        @functools.wraps(func)
        async def wrapped(*args, **kwargs):
            k = hashkey_generator(*args, **kwargs)

            try:
                return cache[k]
            except KeyError:
                pass  # key not found

            v = await func(*args, **kwargs)

            try:
                cache[k] = v
            except ValueError:
                pass  # value too large
            return v
        return wrapped
    return decorator
