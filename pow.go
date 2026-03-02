package main

import (
	"math/bits"
)

// CheckTarget returns true if hash <= target (big-endian comparison).
func CheckTarget(hash, target [32]byte) bool {
	for i := 0; i < 32; i++ {
		if hash[i] < target[i] {
			return true
		}
		if hash[i] > target[i] {
			return false
		}
	}
	return true // equal
}

// difficultyToTarget converts difficulty to a 32-byte target.
// This matches daemon consensus math: floor((2^256 - 1) / difficulty).
func difficultyToTarget(difficulty uint64) [32]byte {
	var target [32]byte
	if difficulty == 0 {
		return target
	}

	numerator := [4]uint64{
		^uint64(0),
		^uint64(0),
		^uint64(0),
		^uint64(0),
	}
	var quotient [4]uint64
	var rem uint64

	for i, limb := range numerator {
		q, r := bits.Div64(rem, limb, difficulty)
		quotient[i] = q
		rem = r
	}

	for i := 0; i < 4; i++ {
		word := quotient[i]
		target[i*8+0] = byte(word >> 56)
		target[i*8+1] = byte(word >> 48)
		target[i*8+2] = byte(word >> 40)
		target[i*8+3] = byte(word >> 32)
		target[i*8+4] = byte(word >> 24)
		target[i*8+5] = byte(word >> 16)
		target[i*8+6] = byte(word >> 8)
		target[i*8+7] = byte(word)
	}

	return target
}
