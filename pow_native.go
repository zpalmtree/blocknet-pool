//go:build cgo

package main

/*
#cgo CFLAGS: -I${SRCDIR}/../blocknet/crypto-rs
#cgo LDFLAGS: ${SRCDIR}/../blocknet/crypto-rs/target/release/libblocknet_crypto.a -lm
#cgo linux LDFLAGS: -ldl -lpthread
#cgo darwin LDFLAGS: -ldl -lpthread -framework Security
#cgo windows LDFLAGS: -lws2_32 -luserenv -lbcrypt -lntdll
#include "../blocknet/crypto-rs/blocknet_crypto.h"
*/
import "C"
import "unsafe"

// PowHash computes the Argon2id hash for proof of work via native Rust FFI.
// password = nonce (8 bytes LE), salt = headerBase (92 bytes).
func PowHash(headerBase []byte, nonce uint64) [32]byte {
	var out [32]byte
	if len(headerBase) == 0 {
		return out
	}
	ret := C.blocknet_pow_hash(
		(*C.uint8_t)(unsafe.Pointer(&headerBase[0])),
		C.size_t(len(headerBase)),
		C.uint64_t(nonce),
		(*C.uint8_t)(unsafe.Pointer(&out[0])),
	)
	if ret != 0 {
		return [32]byte{}
	}
	return out
}
