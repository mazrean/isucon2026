package jpegcheck

import (
	"bytes"
	"image/jpeg"
	"math/rand"
	"os"
	"path/filepath"
	"testing"
)

func ref(data []byte) (int, int, bool) {
	img, err := jpeg.Decode(bytes.NewReader(data))
	if err != nil {
		return 0, 0, false
	}
	b := img.Bounds()
	return b.Dx(), b.Dy(), true
}

func got(data []byte) (int, int, bool) {
	w, h, err := Check(bytes.NewReader(data))
	return w, h, err == nil
}

func compare(t *testing.T, name string, data []byte) bool {
	t.Helper()
	rw, rh, rok := ref(data)
	gw, gh, gok := got(data)
	if rw != gw || rh != gh || rok != gok {
		t.Errorf("%s: ref=(%d,%d,%v) got=(%d,%d,%v)", name, rw, rh, rok, gw, gh, gok)
		return false
	}
	return rok
}

// forEachCase feeds every equivalence-test input (real images plus
// truncated / corrupted / header-corrupted / padded variants) to fn, in a
// fixed order with a fixed seed so every consumer sees the same inputs.
func forEachCase(t testing.TB, dir string, fn func(name string, data []byte)) int {
	files, _ := filepath.Glob(filepath.Join(dir, "*", "*.jpg"))
	rng := rand.New(rand.NewSource(1))
	fn("empty", nil)
	fn("soi-only", []byte{0xff, 0xd8})
	for _, f := range files {
		data, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		fn(f, data)
		// truncations
		for _, frac := range []float64{0, 0.001, 0.01, 0.1, 0.3, 0.5, 0.9, 0.99, 0.999} {
			n := int(float64(len(data)) * frac)
			fn(f+" trunc", data[:n])
		}
		fn(f+" -2", data[:len(data)-2])
		fn(f+" noSOI", data[2:])
		// random byte corruption
		for k := 0; k < 60; k++ {
			c := append([]byte(nil), data...)
			for j := 0; j <= k%5; j++ {
				c[rng.Intn(len(c))] = byte(rng.Intn(256))
			}
			fn(f+" flip", c)
		}
		// header-region corruption (markers, SOF dims, DHT/DQT)
		for k := 0; k < 60; k++ {
			c := append([]byte(nil), data...)
			lim := 700
			if lim > len(c) {
				lim = len(c)
			}
			c[rng.Intn(lim)] = byte(rng.Intn(256))
			fn(f+" hdr", c)
		}
		// garbage appended / prepended
		fn(f+" app", append(append([]byte(nil), data...), 0xde, 0xad))
		fn(f+" pre", append([]byte{0}, data...))
	}
	return len(files)
}

func TestEquivalence(t *testing.T) {
	dir := os.Getenv("JPEGCHECK_CORPUS")
	if dir == "" {
		t.Skip("JPEGCHECK_CORPUS not set")
	}
	okCount, badCount := 0, 0
	nfiles := forEachCase(t, dir, func(name string, data []byte) {
		if compare(t, name, data) {
			okCount++
		} else {
			badCount++
		}
	})
	t.Logf("files=%d ok=%d rejected=%d", nfiles, okCount, badCount)
}

func BenchmarkCheck(b *testing.B) {
	dir := os.Getenv("JPEGCHECK_CORPUS")
	data, err := os.ReadFile(filepath.Join(dir, "jpg", "000.jpg"))
	if err != nil {
		b.Skip()
	}
	b.Run("std", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			ref(data)
		}
	})
	b.Run("check", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			got(data)
		}
	})
}
