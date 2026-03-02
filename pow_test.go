package main

import (
	"math/big"
	"testing"
)

func TestDifficultyToTargetExact(t *testing.T) {
	tests := []uint64{
		1,
		2,
		3,
		7,
		255,
		256,
		257,
		1 << 20,
		(1 << 32) - 1,
		(1 << 63) - 1,
	}

	for _, diff := range tests {
		got := difficultyToTarget(diff)
		want := expectedTarget(diff)
		if got != want {
			t.Fatalf("difficulty=%d target mismatch\n got=%x\nwant=%x", diff, got, want)
		}
	}
}

func expectedTarget(diff uint64) [32]byte {
	var out [32]byte
	if diff == 0 {
		return out
	}

	max256 := new(big.Int).Lsh(big.NewInt(1), 256)
	max256.Sub(max256, big.NewInt(1))
	d := new(big.Int).SetUint64(diff)
	q := new(big.Int).Div(max256, d)

	b := q.Bytes()
	copy(out[32-len(b):], b)
	return out
}
