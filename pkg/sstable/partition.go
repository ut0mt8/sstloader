package sstable

import "io"

type Partition struct {
	HeaderKeyLength         uint16      // uint16
	HeaderKeys              []HeaderKey // HeaderKeyLength size, compound key separated by 00
	HeaderLocalDeletiontime uint32      // uint32
	HeaderMarkedforDeleteAt uint64      // uint64
	Rows                    []Row
}

type HeaderKey struct {
	Value []byte // Length size
}

func (partition *Partition) Read(r io.Reader, compoundPK bool) {

	if compoundPK {
		// header key length
		partition.HeaderKeyLength = ReadUint16(r)

		// header keys
		read := 0
		for {
			hk := HeaderKey{}
			read += hk.ReadCompound(r)
			partition.HeaderKeys = append(partition.HeaderKeys, hk)
			if read >= int(partition.HeaderKeyLength) {
				break
			}
		}
	} else {
		partition.HeaderKeyLength = 1
		hk := HeaderKey{}
		hk.Read(r)
		partition.HeaderKeys = append(partition.HeaderKeys, hk)
	}
	// header local deletion time
	partition.HeaderLocalDeletiontime = ReadUint32(r)

	// header marked for detele at
	partition.HeaderMarkedforDeleteAt = ReadUint64(r)

	for {
		Row := Row{}
		Row.Read(r)

		if GetFlag(Row.Flags, END_OF_PARTITION) {
			break
		}
		partition.Rows = append(partition.Rows, Row)
	}
}

func (hk *HeaderKey) Read(r io.Reader) int {

	length := int(ReadUint16(r))
	hk.Value = ReadSome(r, length)

	return length + 2
}

func (hk *HeaderKey) ReadCompound(r io.Reader) int {

	length := int(ReadUint16(r))
	hk.Value = ReadSome(r, length)

	// last 00
	ReadOne(r)

	return length + 3
}
