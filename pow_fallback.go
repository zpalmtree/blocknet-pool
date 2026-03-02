//go:build !cgo

package main

import (
	"encoding/binary"

	"golang.org/x/crypto/argon2"
)

// Argon2id PoW parameters — must match the blocknet node exactly.
const (
	powMemoryKB    = 2 * 1024 * 1024 // 2GB in KB
	powIterations  = 1
	powParallelism = 1
	powOutputLen   = 32
)

// PowHash computes the Argon2id hash for proof of work.
// password = nonce (8 bytes LE), salt = headerBase (92 bytes).
func PowHash(headerBase []byte, nonce uint64) [32]byte {
	nonceBuf := make([]byte, 8)
	binary.LittleEndian.PutUint64(nonceBuf, nonce)

	hash := argon2.IDKey(nonceBuf, headerBase, powIterations, powMemoryKB, powParallelism, powOutputLen)

	var out [32]byte
	copy(out[:], hash)
	return out
}
