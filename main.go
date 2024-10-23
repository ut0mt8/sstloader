package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"github.com/ghostiam/binstruct"
	"github.com/gocql/gocql"
	"github.com/jessevdk/go-flags"
	"github.com/pierrec/lz4"
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
	statfile, err := os.Open(strings.Replace(opts.DataFile, "Data", "Statistics", 1))
	if err != nil {
		fmt.Printf("(error) statistics-file: %v\n", err)
		os.Exit(1)
	}

	// decode statistics info to struct
	stats := sstable.StatisticsInfo{}
	decoder := binstruct.NewDecoder(statfile, binary.BigEndian)
	err = decoder.Decode(&stats)
	if err != nil {
		fmt.Printf("(error) decode statistics-file: %v\n", err)
		os.Exit(1)
	}
	statfile.Close()

	// display some structure info
	if opts.Debug {
		fmt.Printf("(debug) partition-key %v\n", stats.Serialization.PartitionKeyTypeValue)

		for _, t := range stats.Serialization.ClusteringKey {
			fmt.Printf("(debug) clustering-key %s\n", t.Type)
		}
		for i, t := range stats.Serialization.RegularColumns {
			fmt.Printf("(debug) columns[%d] %s(%s)\n", i, t.Name, t.Type)
		}
	}

	// construct request

	// get partition and clustering key TODO only text supported
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

	// get schema info from stats file
	sstable.SchemaType = make([]uint64, stats.Serialization.RegularColumnsNumber)
	for i := 0; i < int(stats.Serialization.RegularColumnsNumber); i++ {
		sstable.SchemaType[i] = stats.Serialization.RegularColumns[i].TypeSize
		regularColums = regularColums + stats.Serialization.RegularColumns[i].Name + ","
		columsFill = columsFill + "?,"
	}

	regularColums = strings.Trim(regularColums, ",")
	columsFill = strings.Trim(columsFill, ",")

	// insert reqyest
	req = "INSERT INTO " + opts.KS + "." + opts.Table + " (" + partition + clustering + regularColums + ") VALUES (" + columsFill + ") "

	if opts.Debug {
		fmt.Printf("(debug) query: %s \n", req)
	}

	// data file
	dataf, err := os.Open(opts.DataFile)
	if err != nil {
		fmt.Printf("(error) data-file: %v\n", err)
		os.Exit(1)
	}
	defer dataf.Close()

	datafi, err := dataf.Stat()
	if err != nil {
		fmt.Printf("(error) statistics-file: %v\n", err)
		os.Exit(1)
	}

	// compression file
	comp, err := os.Open(strings.Replace(opts.DataFile, "Data", "CompressionInfo", 1))
	if err != nil {
		fmt.Printf("(error) compression-file: %v\n", err)
		os.Exit(1)
	}

	// decode compression info to struct
	info := sstable.CompressionInfo{}
	info.FileSize = datafi.Size()
	decoder = binstruct.NewDecoder(comp, binary.BigEndian)
	err = decoder.Decode(&info)
	if err != nil {
		fmt.Printf("(error) decode compression-file: %v\n", err)
		os.Exit(1)
	}
	comp.Close()

	if opts.Debug {
		fmt.Printf("(debug) compressor-name: %s\n", info.CompressorName.Value)
	}

	// do not count uncompress
	start := time.Now()

	// uncompress data chunk by chunk
	var data []byte

	for i := 0; i < int(info.ChunkCount); i++ {

		chunk := sstable.DataChunk{}
		chunk.CompressedLength = info.ChunkSizes[i]
		decoder := binstruct.NewDecoder(dataf, binary.BigEndian)
		err = decoder.Decode(&chunk)
		if err != nil {
			fmt.Printf("(error) decode data-chunk: %v\n", err)
			os.Exit(1)
		}

		UncompressedBytes := make([]byte, chunk.UncompressedLength)
		_, err := lz4.UncompressBlock(chunk.CompressedBytes, UncompressedBytes)
		if err != nil {
			fmt.Printf("(error) uncompress lz4 data-chunk: %v\n", err)
			os.Exit(1)
		}

		data = append(data, UncompressedBytes...)
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

	// main reading worker
	count := 0
	rl := ratelimit.New(opts.Limit)

	wg.Add(1)
	go func() {

		defer func() {
			if r := recover(); r != nil {
				close(ch)
				wg.Done()
			}
		}()

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
					case 0:
						if string(c.Value) == "" {
							values = append(values, &gocql.UnsetValue)
						} else {
							values = append(values, string(c.Value))
						}
					case 4:
						if sstable.GetFlag(c.Flags, sstable.HAS_EMPTY_VALUE) {
							values = append(values, &gocql.UnsetValue)
						} else {
							values = append(values, sstable.Int32(c.Value))
						}
					case 8:
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
				count++

				if opts.Debug && count%opts.Sampling == 0 {
					fmt.Printf("(debug) inserted %d (%d)\n", count, len(ch))
				}
			}
		}
	}()

	wg.Wait()
	elapsed := time.Since(start)
	fmt.Printf("%d rows inserted in %s. (%d rows/s). %d error(s)\n", count, elapsed, count/int(elapsed.Seconds()), errors.Load())

}
