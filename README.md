# pgbuffer
Buffer data in memory and bulk copy to postgres. Time is esspecially useful when using the timescaledb postresql extension for timeseries workloads.

# Features
- Flush to db based on time or row limit
- Custom column definition
- Multi-worker parallel COPY
- Utilizes copy instead of insert for greater performance.

## TODO
- create examples
- optional buffer to disk instead of memory
