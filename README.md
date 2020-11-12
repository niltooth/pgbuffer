# pgbuffer
Buffer data in memory and bulk copy to postgres. This is especially useful when using the timescaledb postresql extension for timeseries workloads.
Useful for any append only workloads such as timeseries streams.

# Features
- Public flush signaling for custom flush handling such as time based, or os signal based.
- Custom column definition
- Multi-worker concurrent COPY
- Utilizes COPY instead of insert for greater performance.
## Installation
```shell
go get github.com/dev-mull/pgbuffer
```
## Basic Usage

```go

import 	(
    "github.com/dev-mull/pgbuffer"
    "database/sql"
    _ "github.com/lib/pq"

)

//Setup a new buffer
cfg := pgbuffer.Config{
    Limit: 100,
    Workers: 2,
    Tables: []*pgbuffer.BufferedData{
    	&pgbuffer.BufferedData{
    		Table: "test",
    		Columns: []string{"time","foo","bar"},
    	},
    },
}
//Connect to the db
db, err := sql.Open("postgres", dbUrl)

buff := pgbuffer.NewBuffer(db, cfg, logrus.New())

//Write some test data every second to the buffer.
//It will flush after 101 writes because the limit is set to 100
go func() {
    time.Sleep(time.Second * 1)
    buff.Write("test",time.Now(),"check","this")
}()

//Clean shutdown
sigs := make(chan os.Signal, 1)
signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
go func() {
    <-sigs
    buff.Stop()
}()

//Force a flush every minute
go func() {
    t := time.NewTicker(time.Minute)
    for {
        select {
        case <-t.C:
            buff.FlushAll()
        }   
    }
}()

//Block and run until finished
buff.Run()


```
## TODO
- create examples
- write statistics handling
- optional buffer to disk instead of memory
