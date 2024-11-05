package sstable

import (
	"encoding/binary"
	"io"
	"math/bits"
)

func ReadUvarint(r io.Reader) (uint64, error) {
	// 8 bytes container
	var number [8]byte

	// read first byte
	var buf [1]byte
	n, err := r.Read(buf[:])
	if err != nil || n != 1 {
		return 0, err
	}
	firstByte := buf[0]

	// 1 byte encoding for small numbers
	// 127 = 01111111
	if firstByte <= 127 {
		return uint64(firstByte), nil
	}

	// number of leading bits set to 1
	// take leading zeros of the inverse
	numberOfExtraBytes := bits.LeadingZeros8(firstByte ^ 0xff)

	// get the value of the rest of tte first byte
	firstByteValueMask := byte(0xff >> numberOfExtraBytes)
	firstByteValue := firstByte & firstByteValueMask

	// copy everything at the right place
	pos := 8 - numberOfExtraBytes
	number[pos-1] = firstByteValue
	for i := pos; i < 8; i++ {
		n, err := r.Read(buf[:])
		if err != nil || n != 1 {
			return 0, err
		}
		number[i] = buf[0]
	}

	return binary.BigEndian.Uint64(number[:]), nil
}
