ingen
=====

`ingen` is an InfluxDB data generation tool for testing query performance of InfluxDB and ifql.

`ingen` generates fully compacted `.tsm` data directly into existing InfluxDB directories and 
updates the metadata, adding the database, retention policy and shard definitions. When `influxd`
is started, the generated database will be immediately available for querying.

This tool is intended to generate data fast and in a deterministic state.

performance
-----------

Performance is expected to be considerably higher, given it bypasses the entire write path. 

The following examples create a database with a 24h shard group duration and data that spans 4 shards.

|   |   |
| --- | --- |
| Series            | 100,000     |
| Points per Series | 1000        |
| Shards            | 4           |
| Points per shard  | 25,000,000  |
| Total Points      | 100,000,000 |

### inch

```bash
$ influx -execute "drop database stress; create database stress with duration INF shard duration 24h"
$ inch -v -db="stress" -start-time="2017-11-01T00:00:00Z" -time=96h -t=1000,100 -p=1000 -c=2

Host: http://localhost:8086
Concurrency: 2
Measurements: 1
Tag cardinalities: [1000 100]
Points per series: 1000
Total series: 100000
Total points: 100000000
Total fields per point: 1
Batch Size: 5000
Database: stress (Shard duration: 7d)
Write Consistency: any
Start time: 2017-11-01 00:00:00 +0000 UTC
Approx End time: 2017-11-05 00:00:00 +0000 UTC

...

Total time: 180.7 seconds

inch -v -db="stress" -start-time="2017-11-01T00:00:00Z" -time=96h -t=1000,100  91.28s user 14.96s system 58% cpu 3:00.77 total
```

### ingen

```bash
$ bin/ingen -data-path ~/.influxdb/data -meta-path ~/.influxdb/meta -start-time="2017-11-01T00:00:00Z" -p=250 -t=1000,100 -shards=4 -c=2

Data Path: /Users/stuartcarnie/.influxdb/data
Meta Path: /Users/stuartcarnie/.influxdb/meta
Concurrency: 2
Tag cardinalities: [1000 100]
Points per series per shard: 250
Total points per shard: 25000000
Total series: 100000
Total points: 100000000
Total fields per point: 1
Shard Count: 4
Database: db (Shard duration: 24h0m0s)
Start time: 2017-11-01 00:00:00 +0000 UTC
End time: 2017-11-05 00:00:00 +0000 UTC

...

Total time: 4.0 seconds

bin/ingen -data-path ~/.influxdb/data -meta-path ~/.influxdb/meta  -p=250     8.06s user 0.12s system 201% cpu 4.069 total
```

TODOs
-----

* [ ] support TSI

[inch]: https://github.com/influxdata/inch