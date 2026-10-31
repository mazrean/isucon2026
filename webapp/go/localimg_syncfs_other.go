//go:build !(linux && amd64)

package isureal

func syncStoreDir(_ string, paths []string) error { return syncFilesAndDirs(paths) }
