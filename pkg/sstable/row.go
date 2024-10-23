package sstable

import "io"

// global for simplicty and perf
var SchemaType []uint64

// row flags
const (
	END_OF_PARTITION     byte = 0x01
	IS_MARKER            byte = 0x02
	HAS_TIMESTAMP        byte = 0x04
	HAS_TTL              byte = 0x08
	HAS_DELETION         byte = 0x10
	HAS_ALL_COLUMNS      byte = 0x20
	HAS_COMPLEX_DELETION byte = 0x40
	EXTENSION_FLAG       byte = 0x80
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
	if GetFlag(row.Flags, EXTENSION_FLAG) {
		row.ExtentedFlags = ReadOne(r)
	}

	// end of partition? we're done
	if GetFlag(row.Flags, END_OF_PARTITION) {
		return
	}

	// clusteringBlock if we have not static row FIXME: need to really check extented flag
	if !GetFlag(row.Flags, EXTENSION_FLAG) {
		row.ClusteringHeader = ReadUvarint(r)
		row.ClusteringLength = ReadUvarint(r)
		row.ClusteringValue = ReadSome(r, int(row.ClusteringLength))
	}

	// body size
	row.BodySize = ReadUvarint(r)

	// previous size
	row.PreviousSize = ReadUvarint(r)

	// timestamp if any
	if GetFlag(row.Flags, HAS_TIMESTAMP) {
		row.Timestamp = ReadUvarint(r)
	}

	// ttl if any
	if GetFlag(row.Flags, HAS_TTL) {
		row.TTL = ReadUvarint(r)
	}

	// deletionTime if row is deleted or expiring
	if GetFlag(row.Flags, HAS_DELETION) || GetFlag(row.Flags, HAS_TTL) {
		row.DeletionTime = ReadUvarint(r)
	}

	// deletionTimeStamp if row is deleted
	if GetFlag(row.Flags, HAS_DELETION) {
		row.DeletionTimestamp = ReadUvarint(r)
	}

	// localDeletionTime if row is deleted
	if GetFlag(row.Flags, HAS_DELETION) {
		row.LocalDeletionTime = ReadUvarint(r)
	}

	// missing columns if we don't have all colums
	if !GetFlag(row.Flags, HAS_ALL_COLUMNS) {
		row.MissingColumns = ReadUvarint(r)
	}

	// if we have all colums get the numbers of cell from schema
	// otherwise determine it from MissingColums field TODO
	if !GetFlag(row.Flags, HAS_ALL_COLUMNS) {
		return
	}

	// cells
	row.Cells = make([]Cell, len(SchemaType))

	// read number of cells according to the schema
	for i := 0; i < len(SchemaType); i++ {
		Cell := Cell{
			TypeSize: SchemaType[i],
		}
		Cell.Read(r)
		row.Cells[i] = Cell
	}

}
