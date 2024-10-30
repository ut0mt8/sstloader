package sstable

import "io"

const (
	EndOfPartition     byte = 0x01
	IsMarker           byte = 0x02
	HasTimestamp       byte = 0x04
	HasTTL             byte = 0x08
	HasDeletion        byte = 0x10
	HasAllColumns      byte = 0x20
	HasComplexDeletion byte = 0x40
	ExtensionFlag      byte = 0x80
)

type Row struct {
	Flags             byte   // 1byte flags
	ExtentedFlags     byte   // optional 1byte
	ClusteringHeader  uint64 // optional uvartint
	ClusteringLength  uint64 // optional uvarint
	ClusteringValue   []byte // optional ClusteringLength size
	BodySize          uint64 // uvarint
	PreviousSize      uint64 // uvarint
	Timestamp         uint64 // optional uvarint
	TTL               uint64 // optional uvarint
	DeletionTime      uint64 // optional uvarint
	DeletionTimestamp uint64 // optional uvarint
	LocalDeletionTime uint64 // optional uvarint
	MissingColumns    uint64 // optional uvarint
	Cells             []Cell // optional length determined by schema
}

func (row *Row) Read(r io.Reader) {
	// flags
	row.Flags = ReadOne(r)

	// extentedFlags if any
	if GetFlag(row.Flags, ExtensionFlag) {
		row.ExtentedFlags = ReadOne(r)
	}

	// end of partition? we're done
	if GetFlag(row.Flags, EndOfPartition) {
		return
	}

	// clusteringBlock if we have not static row FIXME: need to really check extented flag
	if !GetFlag(row.Flags, ExtensionFlag) {
		row.ClusteringHeader = ReadUvarint(r)
		row.ClusteringLength = ReadUvarint(r)
		row.ClusteringValue = ReadSome(r, int(row.ClusteringLength))
	}

	// body size
	row.BodySize = ReadUvarint(r)

	// previous size
	row.PreviousSize = ReadUvarint(r)

	// timestamp if any
	if GetFlag(row.Flags, HasTimestamp) {
		row.Timestamp = ReadUvarint(r)
	}

	// ttl if any
	if GetFlag(row.Flags, HasTTL) {
		row.TTL = ReadUvarint(r)
	}

	// deletionTime if row is deleted or expiring
	if GetFlag(row.Flags, HasDeletion) || GetFlag(row.Flags, HasTTL) {
		row.DeletionTime = ReadUvarint(r)
	}

	// deletionTimeStamp if row is deleted
	if GetFlag(row.Flags, HasDeletion) {
		row.DeletionTimestamp = ReadUvarint(r)
	}

	// localDeletionTime if row is deleted
	if GetFlag(row.Flags, HasDeletion) {
		row.LocalDeletionTime = ReadUvarint(r)
	}

	// missing columns if we don't have all colums
	if !GetFlag(row.Flags, HasAllColumns) {
		row.MissingColumns = ReadUvarint(r)
	}

	// if we have all colums get the numbers of cell from schema
	// otherwise determine it from MissingColums field TODO
	if !GetFlag(row.Flags, HasAllColumns) {
		return
	}

	// cells
	row.Cells = make([]Cell, len(Schema))

	// read number of cells according to the schema
	for i := 0; i < len(Schema); i++ {
		cell := Cell{
			TypeSize: Schema[i].Size,
		}
		cell.Read(r)
		row.Cells[i] = cell
	}
}
