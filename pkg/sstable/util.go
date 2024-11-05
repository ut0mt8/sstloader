package sstable

import (
	"bytes"
	"encoding/binary"
	"io"
)

func GetFlag(b, n byte) bool {
	return b&n > 0
}

func ReadOne(r io.Reader) (byte, error) {
	var buf [1]byte
	n, err := r.Read(buf[:])
	if err != nil || n != 1 {
		return 0, err
	}
	return buf[0], nil
}

func ReadSome(r io.Reader, nb int) ([]byte, error) {
	buf := make([]byte, nb)
	n, err := r.Read(buf)
	if err != nil || n != nb {
		return []byte{}, err
	}
	return buf, nil
}

func ReadUint16(r io.Reader) (uint16, error) {
	buf := make([]byte, 2)
	n, err := r.Read(buf)
	if err != nil || n != 2 {
		return 0, err
	}
	return binary.BigEndian.Uint16(buf), nil
}

func ReadUint32(r io.Reader) (uint32, error) {
	buf := make([]byte, 4)
	n, err := r.Read(buf)
	if err != nil || n != 4 {
		return 0, err
	}
	return binary.BigEndian.Uint32(buf), nil
}

func ReadUint64(r io.Reader) (uint64, error) {
	buf := make([]byte, 8)
	n, err := r.Read(buf)
	if err != nil || n != 8 {
		return 0, err
	}
	return binary.BigEndian.Uint64(buf), nil
}

func Int32(b []byte) int32 {
	return int32(binary.BigEndian.Uint32(b))
}

func Float64(b []byte) (ret float64) {
	buf := bytes.NewReader(b)
	binary.Read(buf, binary.BigEndian, &ret)
	return ret
}
