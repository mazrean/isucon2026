//go:build !amd64

package isureal

type chainState struct {
	ist [8][16]uint32
	ost [8][16]uint32
	v   [8][16]uint32
}

func chainAVX512(st *chainState, n int) { panic("unreachable") }
