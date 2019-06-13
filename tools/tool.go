package tools

import (
	"encoding/binary"
	"hash/fnv"
)

func Hash64(s string) uint64 {
	h := fnv.New64a()
	h.Write([]byte(s))
	return h.Sum64()
}
func Hash32(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}
func BytesToUint64(b []byte) uint64 {
	return binary.LittleEndian.Uint64(b)
}
func Uint64ToBytes(int uint64) []byte {
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, int)
	return b
}
