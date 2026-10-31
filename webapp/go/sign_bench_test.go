package isureal

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"math/rand/v2"
	"strconv"
	"testing"

	"github.com/google/uuid"
)

// signatureReference is the original Go-hmac implementation, kept as the
// oracle for the differential tests.
func signatureReference(key []byte, imageID string, exp int64) []byte {
	mac := hmac.New(sha256.New, key)
	mac.Write([]byte(strconv.FormatInt(exp, 10) + ":" + imageID))
	var buf [sha256.Size]byte
	value := mac.Sum(buf[:0])
	for range imageSignatureIterations - 1 {
		mac.Reset()
		mac.Write(value)
		value = mac.Sum(value[:0])
	}
	return append([]byte(nil), value...)
}

func randomJobs(r *rand.Rand, n int) []signJob {
	jobs := make([]signJob, n)
	for i := range jobs {
		for k := range jobs[i].key {
			jobs[i].key[k] = byte(r.Uint32())
		}
		switch i % 3 {
		case 0:
			jobs[i].imageID = uuid.NewString()
		case 1:
			jobs[i].imageID = strconv.Itoa(r.IntN(1 << 30))
		default:
			b := make([]byte, r.IntN(80))
			for k := range b {
				b[k] = byte(r.Uint32())
			}
			jobs[i].imageID = string(b)
		}
		jobs[i].exp = r.Int64N(1<<40) - 1<<20
	}
	return jobs
}

func TestSignBatchDifferential(t *testing.T) {
	r := rand.New(rand.NewPCG(1, 2))
	for _, simd := range []bool{true, false} {
		if simd && !cpuHasAVX512Test() {
			t.Log("no AVX-512 here; SIMD path skipped")
			continue
		}
		saved := signHasAVX512
		signHasAVX512 = simd
		total := 0
		for _, n := range []int{1, 2, 3, 15, 16, 17, 31, 33, 100, 1000, 8784} {
			jobs := randomJobs(r, n)
			computeSignatures(jobs)
			for i := range jobs {
				want := signatureReference(jobs[i].key[:], jobs[i].imageID, jobs[i].exp)
				if !bytes.Equal(jobs[i].sig, want) {
					t.Fatalf("simd=%v n=%d i=%d mismatch", simd, n, i)
				}
			}
			total += n
		}
		signHasAVX512 = saved
		t.Logf("simd=%v: %d signatures identical", simd, total)
	}
}

func TestMidstates(t *testing.T) {
	// midstate + one compression of (value||pad) must equal HMAC(key, value).
	key := bytes.Repeat([]byte{7}, 32)
	val := bytes.Repeat([]byte{9}, 32)
	ist, ost := hmacMidstates(key)
	var blk [64]byte
	copy(blk[:], val)
	blk[32] = 0x80
	blk[62], blk[63] = 0x03, 0x00
	compressGeneric(&ist, &blk)
	for i := range 8 {
		blk[4*i], blk[4*i+1], blk[4*i+2], blk[4*i+3] = byte(ist[i]>>24), byte(ist[i]>>16), byte(ist[i]>>8), byte(ist[i])
	}
	compressGeneric(&ost, &blk)
	mac := hmac.New(sha256.New, key)
	mac.Write(val)
	want := mac.Sum(nil)
	for i := range 8 {
		got := []byte{byte(ost[i] >> 24), byte(ost[i] >> 16), byte(ost[i] >> 8), byte(ost[i])}
		if !bytes.Equal(got, want[4*i:4*i+4]) {
			t.Fatal("midstate mismatch")
		}
	}
}

func TestSignedImageURLsMatchesSingle(t *testing.T) {
	r := rand.New(rand.NewPCG(3, 4))
	jobs := randomJobs(r, 200)
	reqs := make([]imageURLReq, 0, 400)
	for i := range jobs {
		reqs = append(reqs, imageURLReq{Key: jobs[i].key[:], ImageID: jobs[i].imageID})
	}
	reqs = append(reqs, reqs[:50]...) // duplicates within one batch
	got := signedImageURLs(reqs)
	for i, rq := range reqs {
		u := User{ImageKey: rq.Key}
		// signedImageURL now hits the cache populated by the batch...
		if s := signedImageURL(u, rq.ImageID); s != got[i] {
			t.Fatalf("cached url mismatch at %d", i)
		}
		// ...and must equal a from-scratch computation.
		exp := currentImageExp()
		if want := buildSignedImageURL(rq.ImageID, exp, signatureReference(rq.Key, rq.ImageID, exp)); want != got[i] {
			t.Fatalf("url mismatch at %d: %s vs %s", i, want, got[i])
		}
	}
	// signedImageURL's own (uncached) URL format equals buildSignedImageURL.
	signedURLCacheMu.Lock()
	signedURLCache = make(map[signedURLCacheKey]signedURLEntry)
	signedURLCacheMu.Unlock()
	signatureCacheMu.Lock()
	signatureCache = make(map[signatureCacheKey][]byte)
	signatureCacheMu.Unlock()
	for i := range 20 {
		if s := signedImageURL(User{ImageKey: reqs[i].Key}, reqs[i].ImageID); s != got[i] {
			t.Fatalf("fresh signedImageURL mismatch at %d", i)
		}
	}
}

func cpuHasAVX512Test() bool { return signHasAVX512 }

func BenchmarkSignStd(b *testing.B) {
	key := make([]byte, 32)
	for i := 0; i < b.N; i++ {
		_ = signatureReference(key, "abc", int64(i))
	}
}

// per-signature cost of the 16-lane kernel on one core.
func BenchmarkSignAVX512Group16(b *testing.B) {
	if !signHasAVX512 {
		b.Skip("no AVX-512")
	}
	jobs := randomJobs(rand.New(rand.NewPCG(5, 6)), 16)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		signGroupAVX512(jobs)
	}
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*16), "ns/sig")
}
