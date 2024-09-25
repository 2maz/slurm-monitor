# Slurm Monitor

## Start

```bash
python3 -m uvicorn --reload slurm_monitor.main:app
```

## Testing
For testing one can run tox.
For the sqlite3 version use
```
    tox
```

for testing against a timescaledb docker will be required:
```
    tox -e timescaledb
```
