"""Use Prefect's @task / @flow if Prefect is installed; otherwise fall back to no-op
decorators so every step can still be run and tested as plain Python."""
try:
    from prefect import task, flow  # noqa: F401
    HAS_PREFECT = True
except ImportError:
    HAS_PREFECT = False

    def _passthrough(*dargs, **dkwargs):
        if len(dargs) == 1 and callable(dargs[0]) and not dkwargs:
            return dargs[0]
        return lambda fn: fn

    task = flow = _passthrough
