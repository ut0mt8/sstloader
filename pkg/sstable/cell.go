package sstable

import "io"

const (
	IsDeleted       byte = 0x01
	IsExpiring      byte = 0x02
	HasEmptyValue   byte = 0x04
	UseRowTimestamp byte = 0x08
	UseRowTTL       byte = 0x10
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
	if !GetFlag(cell.Flags, UseRowTimestamp) {
		cell.Timestamp = ReadUvarint(r)
	}

	// localDeletionTime
	// only if the cell is deleted or expiring and do not use row ttl
	if (GetFlag(cell.Flags, IsDeleted) || GetFlag(cell.Flags, IsExpiring)) && !GetFlag(cell.Flags, UseRowTTL) {
		cell.LocalDeletionTime = ReadUvarint(r)
	}

	// TTL
	// only if cell is expiring and do not use row ttl
	if GetFlag(cell.Flags, IsExpiring) && !GetFlag(cell.Flags, UseRowTTL) {
		cell.TTL = ReadUvarint(r)
	}

	// length of value
	// if we have a value and text type read the following length
	if !GetFlag(cell.Flags, HasEmptyValue) && cell.TypeSize == 0 {
		cell.Length = ReadUvarint(r)
	} else {
		cell.Length = cell.TypeSize
	}

	// only if we have a value
	if !GetFlag(cell.Flags, HasEmptyValue) {
		cell.Value = ReadSome(r, int(cell.Length))
	}
}
