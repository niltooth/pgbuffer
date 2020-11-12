# pgbuffer
Buffer data in memory and bulk copy to postgres. This is especially useful when using the timescaledb postresql extension for timeseries workloads.
Useful for any append only workloads such as timeseries streams.

# Features
- Public flush signaling for custom flush handling such as time based, or os signal based.
- Custom column definition
- Multi-worker concurrent COPY
- Utilizes COPY instead of insert for greater performance.

## TODO
- create examples
- write statistics handling
- optional buffer to disk instead of memory
