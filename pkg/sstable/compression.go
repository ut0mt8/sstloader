package sstable

import (
	"github.com/ghostiam/binstruct"
)

type ShortString struct {
	Length int16
	Value  string `bin:"len:Length"`
}

type Option struct {
	Key   ShortString
	Value ShortString
}

type CompressionInfo struct {
	FileSize       int64 `bin:"-"` //helper
	CompressorName ShortString
	OptionsCount   int32
	Options        []Option `bin:"len:OptionsCount"`
	ChunkLength    int32
	DataLength     int64
	ChunkCount     int32
	ChunkOffsets   []int64 `bin:"len:ChunkCount"`
	ChunkSizes     []int64 `bin:"ReadChunkSizes"`
}

func (info *CompressionInfo) ReadChunkSizes(r binstruct.Reader) ([]int64, error) {
	// populate chunk_sizes
	// chunk := le32 size + lz4 data + 4 bytes crc
	ChunkSizes := make([]int64, info.ChunkCount)

	// fill last offset with the size of the data file (compressed)
	info.ChunkOffsets = append(info.ChunkOffsets, info.FileSize)

	for i := 0; i < int(info.ChunkCount); i++ {
		// offset diff minus size + crc
		ChunkSizes[i] = info.ChunkOffsets[i+1] - info.ChunkOffsets[i] - 8
	}

	return ChunkSizes, nil
}

type DataChunk struct {
	CompressedLength   int64  `bin:"-"` //helper
	UncompressedLength int32  `bin:"le"`
	CompressedBytes    []byte `bin:"len:CompressedLength"`
	CRC                [4]byte
}
