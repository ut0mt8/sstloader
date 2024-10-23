package sstable

import "github.com/ghostiam/binstruct"

type StatisticsInfo struct {
	TOCIndex      uint32
	TOC           TOC
	Serialization Serialization `bin:"offsetStart:TOC.SerializationOffset"`
}

type TOC struct {
	ValidationType      uint32
	ValidationOffset    uint32
	CompactionType      uint32
	CompactionOffset    uint32
	StatisticsType      uint32
	StatisticsOffset    uint32
	SerializationType   uint32
	SerializationOffset uint32
}

type Serialization struct {
	MinTimestamp           uint64          `bin:"ReadUvarint"`
	MinLocalDeletionTIme   uint64          `bin:"ReadUvarint"`
	MinTTL                 uint64          `bin:"ReadUvarint"`
	PartitionKeyTypeLength uint64          `bin:"ReadUvarint"`
	PartitionKeyTypeValue  string          `bin:"len:PartitionKeyTypeLength"`
	ClusteringKeyNumber    uint64          `bin:"ReadUvarint"`
	ClusteringKey          []ClusteringKey `bin:"len:ClusteringKeyNumber"`
	StaticColumnsNumber    uint64          `bin:"ReadUvarint"`
	StaticColumns          []Column        `bin:"len:StaticColumnsNumber"`
	RegularColumnsNumber   uint64          `bin:"ReadUvarint"`
	RegularColumns         []Column        `bin:"len:RegularColumnsNumber"`
}

type ClusteringKey struct {
	TypeLength uint64 `bin:"ReadUvarint"`
	Type       string `bin:"len:TypeLength"`
	TypeSize   uint64 `bin:"GetTypeSize"`
}

type Column struct {
	NameLength uint64 `bin:"ReadUvarint"`
	Name       string `bin:"len:NameLength"`
	TypeLength uint64 `bin:"ReadUvarint"`
	Type       string `bin:"len:TypeLength"`
	TypeSize   uint64 `bin:"GetTypeSize"`
}

func (s *Serialization) ReadUvarint(r binstruct.Reader) (uint64, error) {
	return ReadUvarint(r), nil
}

func (c *ClusteringKey) GetTypeSize(r binstruct.Reader) (uint64, error) {
	return GetTypeSize(c.Type), nil
}

func (c *Column) GetTypeSize(r binstruct.Reader) (uint64, error) {
	return GetTypeSize(c.Type), nil
}

func GetTypeSize(t string) uint64 {
	switch t {
	case "org.apache.cassandra.db.marshal.UTF8Type":
		return 0
	case "org.apache.cassandra.db.marshal.Int32Type":
		return 4
	case "org.apache.cassandra.db.marshal.DoubleType":
		return 8
	}
	return 0
}
