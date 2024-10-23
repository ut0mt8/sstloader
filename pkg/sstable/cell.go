package sstable

import "io"

const (
	IS_DELETED        byte = 0x01
	IS_EXPIRING       byte = 0x02
	HAS_EMPTY_VALUE   byte = 0x04
	USE_ROW_TIMESTAMP byte = 0x08
	USE_ROW_TTL       byte = 0x10
)

type Cell struct {
	TypeSize          uint64 // helper
	Flags             byte   // 1byte flags
	Timestamp         uint64 // optional uvarint
	LocalDeletionTime uint64 // optional uvarint
	TTL               uint64 // optional uvarint
	CellPath          string // optional skipped
	Length            uint64 // optional uvarint
	Value             []byte // optional fixed or length size
}

func (cell *Cell) Read(r io.Reader) {

	cell.Flags = ReadOne(r)

	// timestamp if any
	if !GetFlag(cell.Flags, USE_ROW_TIMESTAMP) {
		cell.Timestamp = ReadUvarint(r)
	}

	// localDeletionTime
	// only if the cell is deleted or expiring and do not use row ttl
	if (GetFlag(cell.Flags, IS_DELETED) || GetFlag(cell.Flags, IS_EXPIRING)) && !GetFlag(cell.Flags, USE_ROW_TTL) {
		cell.LocalDeletionTime = ReadUvarint(r)
	}

	// TTL
	// only if cell is expiring and do not use row ttl
	if GetFlag(cell.Flags, IS_EXPIRING) && !GetFlag(cell.Flags, USE_ROW_TTL) {
		cell.TTL = ReadUvarint(r)
	}

	// length of value
	// if we have a value and text type read the following length
	if !GetFlag(cell.Flags, HAS_EMPTY_VALUE) && cell.TypeSize == 0 {
		cell.Length = ReadUvarint(r)
	} else {
		cell.Length = cell.TypeSize
	}

	// only if we have a value
	if !GetFlag(cell.Flags, HAS_EMPTY_VALUE) {
		cell.Value = ReadSome(r, int(cell.Length))
	}
}
