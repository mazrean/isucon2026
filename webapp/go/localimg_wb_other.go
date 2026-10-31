//go:build !(linux && amd64)

package isureal

import "os"

func kickWriteback(*os.File) {}
