package isureal

import "os"

//go:generate sh -c "go run ./internal/signgen > sign_avx512_amd64.s"

// chainState is the lane-transposed input of chainAVX512: word j of lane l is
// at [j][l]. ist/ost are the HMAC ipad/opad midstates, v the running value.
type chainState struct {
	ist [8][16]uint32
	ost [8][16]uint32
	v   [8][16]uint32
}

//go:noescape
func chainAVX512(st *chainState, n int)

func cpuHasAVX512() bool

func init() { signHasAVX512 = cpuHasAVX512() && os.Getenv("ISUREAL_SIGN_SIMD") != "0" }
