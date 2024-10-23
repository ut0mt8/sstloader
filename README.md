Go rewrite of scylla sstableloader for performance purpose

Implement partially sstable3 specification (3 types, text only partition and clustering key)

````
Usage:
  sstloader [OPTIONS]

Application Options:
  -d, --datafile=    sstable data file
  -s, --seeds=       cassandra seeds
  -k, --keyspace=    cassandra keyspace
  -t, --table=       cassandra table
  -r, --datacenter=  cassandra datacenter
  -u, --username=    cassandra username (default: cassandra)
  -p, --password=    cassandra password (default: cassandra)
  -c, --connections= number of connections by host (default: 20)
  -w, --workers=     workers numbers (default: 100)
  -i, --maxinflight= maximum in flight requests (default: 200)
      --dryrun       only decode sstable
      --printcsv     print CSV to stdout
      --ratelimit=   rate limit insert per second (default: 10000)
      --retries=     number of retry per query (default: 5)
      --timeout=     timeout of a query in ms (default: 5000)
      --debug        print debugging messages

Help Options:
  -h, --help         Show this help message
````