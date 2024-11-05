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

func (partition *Partition) Read(r io.Reader, compoundPK bool) (err error) {
	if compoundPK {
		// header key length
		partition.HeaderKeyLength, err = ReadUint16(r)
		if err != nil {
			return err
		}

		// header keys
		hkl := 0
		for {
			hk := HeaderKey{}
			read, err := hk.ReadCompound(r)
			if err != nil {
				return err
			}
			hkl = hkl + read
			partition.HeaderKeys = append(partition.HeaderKeys, hk)
			if hkl >= int(partition.HeaderKeyLength) {
				break
			}
		}
	} else {
		partition.HeaderKeyLength = 1
		hk := HeaderKey{}
		_, err = hk.Read(r)
		if err != nil {
			return err
		}
		partition.HeaderKeys = append(partition.HeaderKeys, hk)
	}

	// header local deletion time
	partition.HeaderLocalDeletiontime, err = ReadUint32(r)
	if err != nil {
		return err
	}

	// header marked for detele at
	partition.HeaderMarkedforDeleteAt, err = ReadUint64(r)
	if err != nil {
		return err
	}

	for {
		row := Row{}
		err = row.Read(r)
		if err != nil {
			return err
		}

		if GetFlag(row.Flags, EndOfPartition) {
			break
		}
		partition.Rows = append(partition.Rows, row)
	}

	return nil
}

func (hk *HeaderKey) Read(r io.Reader) (int, error) {
	length, err := ReadUint16(r)
	if err != nil {
		return 0, err
	}

	hk.Value, err = ReadSome(r, int(length))
	if err != nil {
		return 0, err
	}

	return int(length) + 2, nil
}

func (hk *HeaderKey) ReadCompound(r io.Reader) (int, error) {
	length, err := ReadUint16(r)
	if err != nil {
		return 0, err
	}

	hk.Value, err = ReadSome(r, int(length))
	if err != nil {
		return 0, err
	}

	// last 00
	_, err = ReadOne(r)
	if err != nil {
		return 0, err
	}

	return int(length) + 3, nil
}
