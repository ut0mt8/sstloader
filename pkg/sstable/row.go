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

func (row *Row) Read(r io.Reader) (err error) {
	// flags
	row.Flags, err = ReadOne(r)
	if err != nil {
		return err
	}

	// extentedFlags if any
	if GetFlag(row.Flags, ExtensionFlag) {
		row.ExtentedFlags, err = ReadOne(r)
		if err != nil {
			return err
		}
	}

	// end of partition? we're done
	if GetFlag(row.Flags, EndOfPartition) {
		return nil
	}

	// clusteringBlock if we have not static row FIXME: need to really check extented flag
	if !GetFlag(row.Flags, ExtensionFlag) {
		row.ClusteringHeader, err = ReadUvarint(r)
		if err != nil {
			return err
		}

		row.ClusteringLength, err = ReadUvarint(r)
		if err != nil {
			return err
		}

		row.ClusteringValue, err = ReadSome(r, int(row.ClusteringLength))
		if err != nil {
			return err
		}
	}

	// body size
	row.BodySize, err = ReadUvarint(r)
	if err != nil {
		return err
	}

	// previous size
	row.PreviousSize, err = ReadUvarint(r)
	if err != nil {
		return err
	}

	// timestamp if any
	if GetFlag(row.Flags, HasTimestamp) {
		row.Timestamp, err = ReadUvarint(r)
		if err != nil {
			return err
		}
	}

	// ttl if any
	if GetFlag(row.Flags, HasTTL) {
		row.TTL, err = ReadUvarint(r)
		if err != nil {
			return err
		}
	}

	// deletionTime if row is deleted or expiring
	if GetFlag(row.Flags, HasDeletion) || GetFlag(row.Flags, HasTTL) {
		row.DeletionTime, err = ReadUvarint(r)
		if err != nil {
			return err
		}
	}

	// deletionTimeStamp if row is deleted
	if GetFlag(row.Flags, HasDeletion) {
		row.DeletionTimestamp, err = ReadUvarint(r)
		if err != nil {
			return err
		}
	}

	// localDeletionTime if row is deleted
	if GetFlag(row.Flags, HasDeletion) {
		row.LocalDeletionTime, err = ReadUvarint(r)
		if err != nil {
			return err
		}
	}

	// missing columns if we don't have all colums
	if !GetFlag(row.Flags, HasAllColumns) {
		row.MissingColumns, err = ReadUvarint(r)
		if err != nil {
			return err
		}
	}

	// if we have all colums get the numbers of cell from schema
	// otherwise determine it from MissingColums field TODO
	if !GetFlag(row.Flags, HasAllColumns) {
		return nil
	}

	// cells
	row.Cells = make([]Cell, len(Schema))

	// read number of cells according to the schema
	for i := 0; i < len(Schema); i++ {
		cell := Cell{
			TypeSize: Schema[i].Size,
		}

		err = cell.Read(r)
		if err != nil {
			return err
		}

		row.Cells[i] = cell
	}

	return nil
}
