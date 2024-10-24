package main

import (
	"bytes"
	"fmt"
	"github.com/gocql/gocql"
	"github.com/jessevdk/go-flags"
	"go.uber.org/ratelimit"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"sstloader/pkg/sstable"
)

func main() {

	var opts struct {
		DataFile string `short:"d" long:"datafile" description:"sstable data file" required:"true"`
		Seeds    string `short:"s" long:"seeds" description:"cassandra seeds" required:"true"`
		KS       string `short:"k" long:"keyspace" description:"cassandra keyspace" required:"true"`
		Table    string `short:"t" long:"table" description:"cassandra table" required:"true"`
		DC       string `short:"r" long:"datacenter" description:"cassandra datacenter" required:"true"`
		Username string `short:"u" long:"username" description:"cassandra username" default:"cassandra"`
		Password string `short:"p" long:"password" description:"cassandra password" default:"cassandra"`
		Conns    int    `short:"c" long:"connections" description:"number of connections by host" default:"20"`
		Workers  int    `short:"w" long:"workers" description:"workers numbers" default:"100"`
		InFlight int    `short:"i" long:"maxinflight" description:"maximum in flight requests" default:"200"`
		Dry      bool   `long:"dryrun" description:"only decode sstable"`
		CSV      bool   `long:"printcsv" description:"print CSV to stdout"`
		Limit    int    `long:"ratelimit" description:"rate limit insert per second" default:"10000"`
		Retries  int    `long:"retries" description:"number of retry per query" default:"5"`
		Timeout  int    `long:"timeout" description:"timeout of a query in ms" default:"5000"`
		Sampling int    `long:"sample" description:"every how sample print rate message" default:"10000"`
		Debug    bool   `long:"debug" description:"print debugging messages"`
	}

	if _, err := flags.Parse(&opts); err != nil {
		switch flagsErr := err.(type) {
		case flags.ErrorType:
			if flagsErr == flags.ErrHelp {
				os.Exit(0)
			}
			os.Exit(1)
		default:
			os.Exit(1)
		}
	}

	statisticsFile := strings.Replace(opts.DataFile, "Data", "Statistics", 1)
	compressionFile := strings.Replace(opts.DataFile, "Data", "CompressionInfo", 1)

	// cassandra init
	var session *gocql.Session

	cluster := gocql.NewCluster(opts.Seeds)
	cluster.Keyspace = opts.KS
	cluster.Consistency = gocql.Any // we don't want to wait
	cluster.ProtoVersion = 4        // null handling
	cluster.Timeout = time.Duration(opts.Timeout) * time.Millisecond
	cluster.WriteTimeout = time.Duration(opts.Timeout) * time.Millisecond
	cluster.NumConns = opts.Conns // theoricitally handled by the scylla driver
	cluster.RetryPolicy = &gocql.SimpleRetryPolicy{NumRetries: opts.Retries}
	cluster.PoolConfig.HostSelectionPolicy = gocql.TokenAwareHostPolicy(gocql.DCAwareRoundRobinPolicy(opts.DC))

	cluster.Authenticator = gocql.PasswordAuthenticator{
		Username: opts.Username,
		Password: opts.Password,
	}

	if !opts.Dry {
		var err error
		session, err = cluster.CreateSession()
		if err != nil {
			fmt.Printf("(error) cassandra create session: %v\n", err)
			os.Exit(1)
		}
		defer session.Close()
	}

	// statistics file
	err := sstable.ReadStatistics(statisticsFile)
	if err != nil {
		fmt.Printf("(error) read statistics: %v\n", err)
		os.Exit(1)
	}

	// construct request

	// get partition and clustering key
	// TODO only text supported
	var (
		partition     string
		clustering    string
		cname         string
		kind          string
		regularColums string
		columsFill    string
		pkNumber      int
		compoundPK    bool
	)

	req := "SELECT column_name, kind FROM system_schema.columns where keyspace_name = '%s' and table_name = '%s'"
	iter := session.Query(fmt.Sprintf(req, opts.KS, opts.Table)).Consistency(gocql.LocalQuorum).Iter()

	for iter.Scan(&cname, &kind) {
		if kind == "partition_key" {
			partition = partition + cname + ","
			columsFill = columsFill + "?,"
			pkNumber++
		} else if kind == "clustering" {
			clustering = clustering + cname + ","
			columsFill = columsFill + "?,"
		}
	}

	// FIXME find better to pass it to partitionReader
	if pkNumber > 1 {
		compoundPK = true
	}

	// get columns from schemas
	for i := 0; i < len(sstable.Schema); i++ {
		regularColums = regularColums + sstable.Schema[i].Name + ","
		columsFill = columsFill + "?,"
	}

	regularColums = strings.Trim(regularColums, ",")
	columsFill = strings.Trim(columsFill, ",")

	// insert reqyest
	req = "INSERT INTO " + opts.KS + "." + opts.Table + " (" + partition + clustering + regularColums + ") VALUES (" + columsFill + ") "

	if opts.Debug {
		fmt.Printf("(debug) query: %s \n", req)
	}

	// read datafile and uncompress it
	var data []byte

	err = sstable.ReadData(opts.DataFile, compressionFile, &data)
	if err != nil {
		fmt.Printf("(error) read data: %v\n", err)
	}

	// insert workers
	ch := make(chan []any, opts.InFlight)
	wg := &sync.WaitGroup{}
	wg.Add(opts.Workers)

	var errors atomic.Uint64

	for i := 0; i < opts.Workers; i++ {
		go func() {
			defer wg.Done()
			for v := range ch {
				if !opts.Dry {
					err := session.Query(req, v...).Exec()
					if err != nil {
						// counting error if any
						errors.Add(1)
						fmt.Printf("(error) query error: %v\n", err)
					}
				}
			}
		}()
	}

	// do not count uncompress
	start := time.Now()

	// main reading worker
	queriesCount := 0
	rl := ratelimit.New(opts.Limit)

	wg.Add(1)
	go func() {

		defer func() {
			if r := recover(); r != nil {
				close(ch)
				wg.Done()
			}
		}()

		// readPartition(data, ch, rl)
		reader := bytes.NewReader(data)

		//main read loop
		for {
			partition := sstable.Partition{}
			partition.Read(reader, compoundPK)

			var pvalues []any

			for _, hk := range partition.HeaderKeys {
				pvalues = append(pvalues, hk.Value)
			}

			for _, r := range partition.Rows {

				var values []any
				values = append(pvalues, r.ClusteringValue)

				for _, c := range r.Cells {
					// Internal type
					switch c.TypeSize {
					case sstable.TextSize:
						if string(c.Value) == "" {
							values = append(values, &gocql.UnsetValue)
						} else {
							values = append(values, string(c.Value))
						}
					case sstable.Int32Size:
						if sstable.GetFlag(c.Flags, sstable.HAS_EMPTY_VALUE) {
							values = append(values, &gocql.UnsetValue)
						} else {
							values = append(values, sstable.Int32(c.Value))
						}
					case sstable.DoubleSize:
						if sstable.GetFlag(c.Flags, sstable.HAS_EMPTY_VALUE) {
							values = append(values, &gocql.UnsetValue)
						} else {
							values = append(values, sstable.Float64(c.Value))
						}
					}
				}

				// send to cql workers
				rl.Take()
				ch <- values
				queriesCount++

				if opts.Debug && queriesCount%opts.Sampling == 0 {
					fmt.Printf("(debug) inserted %d (%d)\n", queriesCount, len(ch))
				}
			}
		}
	}()

	wg.Wait()
	elapsed := time.Since(start)
	fmt.Printf("%d rows inserted in %s. (%d rows/s). %d error(s)\n", queriesCount, elapsed, queriesCount/int(elapsed.Seconds()), errors.Load())

}
