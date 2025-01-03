package sstable

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"

	"github.com/ghostiam/binstruct"
	"github.com/gocql/gocql"
	"github.com/pierrec/lz4"
	"go.uber.org/ratelimit"
)

var Schema []SchemaEntry

const (
	TextSize   uint64 = 0
	Int32Size  uint64 = 4
	DoubleSize uint64 = 8
)

type SchemaEntry struct {
	Name string
	Size uint64
}

type SSTable struct {
	DataFile        string
	StatisticsFile  string
	CompressionFile string
	Debug           bool
	Compound        bool
	Sampling        int
	Limit           int
	Queries         int
	data            []byte
}

func New() *SSTable {
	return &SSTable{}
}

func (sst *SSTable) ReadStatistics() error {
	// statistics file
	file, err := os.Open(sst.StatisticsFile)
	if err != nil {
		return fmt.Errorf("open statistics-file: %w", err)
	}

	// decode statistics info to struct
	stats := StatisticsInfo{}
	decoder := binstruct.NewDecoder(file, binary.BigEndian)
	err = decoder.Decode(&stats)
	if err != nil {
		return fmt.Errorf("decode statistics-file: %w", err)
	}
	file.Close()

	// display some structure info
	if sst.Debug {
		fmt.Printf("(debug) partition-key %v\n", stats.Serialization.PartitionKeyTypeValue)

		for _, t := range stats.Serialization.ClusteringKey {
			fmt.Printf("(debug) clustering-key %s\n", t.Type)
		}
		for i, t := range stats.Serialization.RegularColumns {
			fmt.Printf("(debug) columns[%d] %s(%s)\n", i, t.Name, t.Type)
		}
	}

	// fill schema infos from stats file
	Schema = make([]SchemaEntry, stats.Serialization.RegularColumnsNumber)
	for i := 0; i < int(stats.Serialization.RegularColumnsNumber); i++ {
		Schema[i].Name = stats.Serialization.RegularColumns[i].Name
		Schema[i].Size = stats.Serialization.RegularColumns[i].TypeSize
	}

	return nil
}

func (sst *SSTable) ReadData() error {
	// data file
	dataf, err := os.Open(sst.DataFile)
	if err != nil {
		return fmt.Errorf("open data-file: %w", err)
	}
	defer dataf.Close()

	// get file size
	datafi, err := dataf.Stat()
	if err != nil {
		return fmt.Errorf("stat data-file: %w", err)
	}

	// compression file
	compf, err := os.Open(sst.CompressionFile)
	if err != nil {
		return fmt.Errorf("open compression-file: %w", err)
	}

	// decode compression info to struct
	cinfo := CompressionInfo{}
	cinfo.FileSize = datafi.Size()
	decoder := binstruct.NewDecoder(compf, binary.BigEndian)
	err = decoder.Decode(&cinfo)
	if err != nil {
		return fmt.Errorf("decode compression-file: %w", err)
	}
	compf.Close()

	if sst.Debug {
		fmt.Printf("(debug) compressor-name: %s\n", cinfo.CompressorName.Value)
	}

	// uncompress data chunk by chunk
	for i := 0; i < int(cinfo.ChunkCount); i++ {
		chunk := DataChunk{}
		chunk.CompressedLength = cinfo.ChunkSizes[i]
		decoder := binstruct.NewDecoder(dataf, binary.BigEndian)
		err = decoder.Decode(&chunk)
		if err != nil {
			return fmt.Errorf("decode data-chunk: %w", err)
		}

		uncompressedBytes := make([]byte, chunk.UncompressedLength)
		_, err := lz4.UncompressBlock(chunk.CompressedBytes, uncompressedBytes)
		if err != nil {
			return fmt.Errorf("uncompress lz4 data-chunk: %w", err)
		}

		sst.data = append(sst.data, uncompressedBytes...)
	}

	return nil
}

func (sst *SSTable) ReadPartitions(ch chan []any) {
	rl := ratelimit.New(sst.Limit)
	reader := bytes.NewReader(sst.data)

	// loop over partition
	for {
		partition := Partition{}
		err := partition.Read(reader, sst.Compound)
		if err != nil {
			break // we should have reach eof
		}

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
				case TextSize:
					if string(c.Value) == "" {
						values = append(values, &gocql.UnsetValue)
					} else {
						values = append(values, string(c.Value))
					}
				case Int32Size:
					if GetFlag(c.Flags, HasEmptyValue) {
						values = append(values, &gocql.UnsetValue)
					} else {
						values = append(values, Int32(c.Value))
					}
				case DoubleSize:
					if GetFlag(c.Flags, HasEmptyValue) {
						values = append(values, &gocql.UnsetValue)
					} else {
						values = append(values, Float64(c.Value))
					}
				}
			}

			// send to cql workers
			rl.Take()
			ch <- values
			sst.Queries++

			if sst.Debug && sst.Queries%sst.Sampling == 0 {
				fmt.Printf("(debug) inserted %d (%d)\n", sst.Queries, len(ch))
			}
		}
	}
}
