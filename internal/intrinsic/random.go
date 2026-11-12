package intrinsic

// javaRandom is java.util.Random's 48-bit linear congruential generator.
// nextInt follows that class, including the power-of-two shortcut and the
// rejection sample of next(31).
type javaRandom struct {
	state uint64
}

const (
	javaMultiplier = uint64(0x5DEECE66D)
	javaAddend     = uint64(0xB)
	javaMask       = (uint64(1) << 48) - 1
)

func newJavaRandom(seed int64) *javaRandom {
	return &javaRandom{state: (uint64(seed) ^ javaMultiplier) & javaMask}
}

func (r *javaRandom) next(bits uint) int32 {
	r.state = (r.state*javaMultiplier + javaAddend) & javaMask
	return int32(r.state >> (48 - bits))
}

func (r *javaRandom) nextInt(bound int32) int32 {
	if bound <= 0 {
		return 0
	}
	if bound&(bound-1) == 0 {
		return int32((int64(bound) * int64(r.next(31))) >> 31)
	}
	bits := r.next(31)
	val := bits % bound
	m := bound - 1
	for bits-val+m < 0 {
		bits = r.next(31)
		val = bits % bound
	}
	return val
}
