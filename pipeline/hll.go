package pipeline

import (
	"math"
	"math/bits"
)

// hll is a HyperLogLog cardinality estimator with 2^14 registers (16 KB),
// giving roughly 1% error. It bounds Profile memory for high-cardinality
// columns such as transaction IDs.
type hll struct {
	reg [1 << 14]uint8
}

func newHLL() *hll { return &hll{} }

func (h *hll) add(x uint64) {
	x = mix64(x)
	idx := x >> (64 - 14)
	rank := uint8(bits.LeadingZeros64(x<<14|1<<13)) + 1
	if rank > h.reg[idx] {
		h.reg[idx] = rank
	}
}

func (h *hll) count() int64 {
	const m = float64(1 << 14)
	var sum float64
	zeros := 0
	for _, r := range h.reg {
		sum += 1 / float64(uint64(1)<<r)
		if r == 0 {
			zeros++
		}
	}
	est := 0.7213 / (1 + 1.079/m) * m * m / sum
	if est <= 2.5*m && zeros > 0 {
		est = m * math.Log(m/float64(zeros)) // linear counting for small sets
	}
	return int64(est + 0.5)
}

// mix64 is the MurmurHash3 finalizer. FNV leaves the high bits weak for
// short, similar strings; this spreads entropy into them.
func mix64(x uint64) uint64 {
	x ^= x >> 33
	x *= 0xff51afd7ed558ccd
	x ^= x >> 33
	x *= 0xc4ceb9fe1a85ec53
	x ^= x >> 33
	return x
}
