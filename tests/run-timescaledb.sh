docker run -d --rm --name some-timescaledb -p 7000:5432 -e POSTGRES_DB=ex3cluster -e POSTGRES_PASSWORD=test -e POSTGRES_USER=slurmuser timescale/timescaledb:latest-pg16
