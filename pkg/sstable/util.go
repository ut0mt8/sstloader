package sstable

import (
	"bytes"
	"encoding/binary"
	"io"
)

func GetFlag(b, n byte) bool {
	return b&n > 0
}

func ReadOne(r io.Reader) byte {
	var buf [1]byte
	n, err := r.Read(buf[:])
	if err != nil || n != 1 {
		panic("read error")
	}
	return buf[0]
}

func ReadSome(r io.Reader, nb int) []byte {
	buf := make([]byte, nb)
	n, err := r.Read(buf)
	if err != nil || n != nb {
		panic("read error")
	}
	return buf
}

func ReadUint16(r io.Reader) uint16 {
	buf := make([]byte, 2)
	n, err := r.Read(buf)
	if err != nil || n != 2 {
		panic("read error")
	}
	return binary.BigEndian.Uint16(buf)
}

func ReadUint32(r io.Reader) uint32 {
	buf := make([]byte, 4)
	n, err := r.Read(buf)
	if err != nil || n != 4 {
		panic("read error")
	}
	return binary.BigEndian.Uint32(buf)
}

func ReadUint64(r io.Reader) uint64 {
	buf := make([]byte, 8)
	n, err := r.Read(buf)
	if err != nil || n != 8 {
		panic("read error")
	}
	return binary.BigEndian.Uint64(buf)
}

func Int32(b []byte) (ret int32) {
	return int32(binary.BigEndian.Uint32(b))
}

func Float64(b []byte) (ret float64) {
	buf := bytes.NewReader(b)
	err := binary.Read(buf, binary.BigEndian, &ret)
	if err != nil {
		panic("read error")
	}
	return ret
}
