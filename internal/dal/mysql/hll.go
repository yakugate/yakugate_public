package mysql

import (
	"encoding/binary"
	"hash/fnv"
	"math"
	"math/bits"
)

// A small HyperLogLog for the abuse counters' "how many different numbers"
// (abuse_numbers): what Redis's PFADD / PFCOUNT do, for a gateway with no
// Redis. 4,096 registers (about 1.6% standard error; Redis's has 16,384 and
// 0.8%); a few numbers are counted exactly (linear counting). The hash is
// fixed, so sketches written by different processes merge (register by
// register, the larger wins).
const (
	hllBits      = 12
	hllRegisters = 1 << hllBits
)

type hll [hllRegisters]uint8

// hllHash is FNV-1a 64 mixed by splitmix64's finalizer (FNV's high bits are
// weak on short, similar strings such as phone numbers).
func hllHash(s string) uint64 {
	h := fnv.New64a()
	h.Write([]byte(s))
	x := h.Sum64()
	x ^= x >> 30
	x *= 0xbf58476d1ce4e5b9
	x ^= x >> 27
	x *= 0x94d049bb133111eb
	x ^= x >> 31
	return x
}

// add notes one member.
func (h *hll) add(s string) {
	x := hllHash(s)
	i := x >> (64 - hllBits)
	rest := x<<hllBits | 1<<(hllBits-1) // never all zero
	if r := uint8(bits.LeadingZeros64(rest) + 1); r > h[i] {
		h[i] = r
	}
}

// merge takes the larger of each register.
func (h *hll) merge(o *hll) {
	for i, r := range o {
		if r > h[i] {
			h[i] = r
		}
	}
}

// count estimates how many different members were added.
func (h *hll) count() int64 {
	m := float64(hllRegisters)
	sum, zeros := 0.0, 0
	for _, r := range h {
		sum += math.Ldexp(1, -int(r))
		if r == 0 {
			zeros++
		}
	}
	est := 0.7213 / (1 + 1.079/m) * m * m / sum
	if est <= 2.5*m && zeros > 0 {
		est = m * math.Log(m/float64(zeros))
	}
	return int64(math.Round(est))
}

// encode writes the sketch short: 's' and (register, value) pairs while few
// registers are set, else 'd' and every register.
func (h *hll) encode() []byte {
	set := 0
	for _, r := range h {
		if r != 0 {
			set++
		}
	}
	if 1+set*3 < 1+hllRegisters {
		out := make([]byte, 1, 1+set*3)
		out[0] = 's'
		for i, r := range h {
			if r != 0 {
				out = binary.BigEndian.AppendUint16(out, uint16(i))
				out = append(out, r)
			}
		}
		return out
	}
	out := make([]byte, 1+hllRegisters)
	out[0] = 'd'
	copy(out[1:], h[:])
	return out
}

// decodeInto merges an encoded sketch into h; a sketch it cannot read adds
// nothing.
func (h *hll) decodeInto(b []byte) {
	if len(b) == 0 {
		return
	}
	switch b[0] {
	case 'd':
		if len(b) != 1+hllRegisters {
			return
		}
		var o hll
		copy(o[:], b[1:])
		h.merge(&o)
	case 's':
		for p := b[1:]; len(p) >= 3; p = p[3:] {
			i, r := binary.BigEndian.Uint16(p), p[2]
			if int(i) < hllRegisters && r > h[i] {
				h[i] = r
			}
		}
	}
}
