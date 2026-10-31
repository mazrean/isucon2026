package isureal

import (
	"os"
	"runtime"
	"time"
)

// Every host's signed-URL cache goes cold at once when exp crosses a
// 10-minute boundary. From signPrefetchLead before the boundary, sign every
// cached (key, imageID) for the next exp in the background so that after the
// boundary signedImageURL / image verifies hit signatureCache. The exp rule
// itself (currentImageExp) is unchanged.
const signPrefetchLead = 60 * time.Second

// small chunks keep the shared FIFO pool responsive to request misses.
const signPrefetchChunk = 32

func init() {
	if os.Getenv("ISUREAL_SIGN_PREFETCH") == "0" {
		return
	}
	go signPrefetchLoop()
}

func signPrefetchLoop() {
	var done int64
	for {
		time.Sleep(time.Second)
		if next, ok := signPrefetchTarget(time.Now()); ok && next != done {
			done = next
			prefetchSignatures(next)
		}
	}
}

// signPrefetchTarget returns the next exp when now is within the lead window.
func signPrefetchTarget(now time.Time) (int64, bool) {
	step := int64((imageURLLifetime - imageURLReuseMargin) / time.Second)
	raw := now.Add(imageURLLifetime).Unix()
	if raw%step < step-int64(signPrefetchLead/time.Second) {
		return 0, false
	}
	return raw - raw%step + step, true
}

// prefetchSignatures computes signatures for next for every URL cached at the
// current exp (next-step). Returns the number computed.
func prefetchSignatures(next int64) int {
	cur := next - int64((imageURLLifetime-imageURLReuseMargin)/time.Second)
	var cks []signatureCacheKey
	signedURLCacheMu.RLock()
	for k, e := range signedURLCache {
		if e.exp == cur {
			cks = append(cks, signatureCacheKey{key: k.key, imageID: k.imageID, exp: next})
		}
	}
	signedURLCacheMu.RUnlock()

	computed := 0
	for lo := 0; lo < len(cks); lo += signPrefetchChunk {
		hi := min(lo+signPrefetchChunk, len(cks))
		var jobs []signJob
		signatureCacheMu.RLock()
		for _, ck := range cks[lo:hi] {
			if _, ok := signatureCache[ck]; !ok {
				jobs = append(jobs, signJob{key: ck.key, imageID: ck.imageID, exp: ck.exp})
			}
		}
		full := len(signatureCache)+len(jobs) > signatureCacheLimit
		signatureCacheMu.RUnlock()
		if full {
			return computed // never trigger the cache reset from the background
		}
		if len(jobs) == 0 {
			continue
		}
		computeSignatures(jobs)
		signatureCacheMu.Lock()
		for _, j := range jobs {
			signatureCache[signatureCacheKey{key: j.key, imageID: j.imageID, exp: j.exp}] = j.sig
		}
		signatureCacheMu.Unlock()
		computed += len(jobs)
		runtime.Gosched()
	}
	return computed
}
