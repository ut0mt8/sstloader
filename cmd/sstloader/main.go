package main

import (
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/jessevdk/go-flags"

	"sstloader/internal/cassandra"
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
		Workers  int    `short:"w" long:"workers" description:"workers numbers" default:"100"`
		InFlight int    `short:"i" long:"maxinflight" description:"maximum in flight requests" default:"200"`
		Conns    int    `long:"connections" description:"number of connections by host" default:"20"`
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

	// sstable init
	sst := sstable.New()
	sst.DataFile = opts.DataFile
	sst.StatisticsFile = strings.Replace(opts.DataFile, "Data", "Statistics", 1)
	sst.CompressionFile = strings.Replace(opts.DataFile, "Data", "CompressionInfo", 1)
	sst.Limit = opts.Limit
	sst.Sampling = opts.Sampling
	if opts.Debug {
		sst.Debug = true
	}

	// read statistics file
	err := sst.ReadStatistics()
	if err != nil {
		fmt.Printf("(error) read statistics: %v\n", err)
		os.Exit(1)
	}

	// cassandra loader init
	cl := cassandra.New()
	cl.Seeds = opts.Seeds
	cl.KS = opts.KS
	cl.Table = opts.Table
	cl.DC = opts.DC
	cl.Username = opts.Username
	cl.Password = opts.Password
	cl.Timeout = opts.Timeout
	cl.Retries = opts.Retries
	cl.Conns = opts.Conns
	if opts.Debug {
		cl.Debug = true
	}

	if !opts.Dry {
		err := cl.Prepare(sst)
		if err != nil {
			fmt.Printf("(error) cassandra loader prepare: %v\n", err)
			os.Exit(1)
		}
	}

	// read datafile and uncompress it in memory
	err = sst.ReadData()
	if err != nil {
		fmt.Printf("(error) read data: %v\n", err)
	}

	// loader workers
	ch := make(chan []any, opts.InFlight)
	wg := &sync.WaitGroup{}
	wg.Add(opts.Workers)
	for i := 0; i < opts.Workers; i++ {
		go func() {
			defer wg.Done()
			for v := range ch {
				if !opts.Dry {
					cl.Load(v)
				}
			}
		}()
	}

	// main reading worker
	start := time.Now()

	// wrap in go routine to handle recover
	wg.Add(1)
	go func() {
		defer func() {
			if r := recover(); r != nil {
				close(ch)
				wg.Done()
			}
		}()

		sst.ReadPartitions(ch)
	}()

	wg.Wait()
	elapsed := time.Since(start)
	fmt.Printf("%d rows inserted in %s. (%d rows/s). %d error(s)\n", sst.Queries, elapsed, sst.Queries/int(elapsed.Seconds()), cl.Errors.Load())
}
