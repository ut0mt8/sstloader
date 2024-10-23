module sstloader

go 1.22.2

require (
	github.com/ghostiam/binstruct v1.4.0
	github.com/gocql/gocql v1.7.0
	github.com/jessevdk/go-flags v1.6.1
	github.com/pierrec/lz4 v2.6.1+incompatible
	go.uber.org/ratelimit v0.3.1
)

require (
	github.com/benbjohnson/clock v1.3.0 // indirect
	github.com/frankban/quicktest v1.14.6 // indirect
	github.com/golang/snappy v0.0.3 // indirect
	github.com/hailocab/go-hostpool v0.0.0-20160125115350-e80d13ce29ed // indirect
	golang.org/x/sys v0.21.0 // indirect
	gopkg.in/inf.v0 v0.9.1 // indirect
)

replace github.com/gocql/gocql => github.com/scylladb/gocql v1.14.3
