package isureal

import (
	"runtime"
	"sync"
)

// signPool: GOMAXPROCS workers share one queue of signature jobs. A worker
// takes everything pending (up to 16 lanes) and runs it as one kernel call,
// so concurrent cache misses from different requests (image GET verifies,
// single signedImageURL calls) coalesce into lanes without any added delay.
type poolJob struct {
	job  *signJob
	done *sync.WaitGroup
}

const signPoolSpins = 4

var (
	signQueue   chan poolJob
	signPoolOnce sync.Once
)

func startSignPool() {
	signQueue = make(chan poolJob, 8192)
	for range runtime.GOMAXPROCS(0) {
		go signWorker()
	}
}

func signWorker() {
	var batch [16]poolJob
	var jobs [16]signJob
	for {
		batch[0] = <-signQueue
		n := 1
		// yield a few times so runnable submitters (requests whose previous
		// signature just finished) can enqueue: a kernel run costs the same
		// for 1 or 16 lanes, so filling lanes is worth a few microseconds.
		for spin := 0; n < 16 && spin < signPoolSpins; spin++ {
			before := n
		drain:
			for n < 16 {
				select {
				case pj := <-signQueue:
					batch[n] = pj
					n++
				default:
					break drain
				}
			}
			if n == before && spin > 0 {
				break
			}
			runtime.Gosched()
		}
		if !signHasAVX512 || n < signSIMDMin {
			for i := range n {
				j := batch[i].job
				j.sig = signatureScalar(j.key[:], j.imageID, j.exp)
				batch[i].done.Done()
			}
			continue
		}
		for i := range n {
			jobs[i] = *batch[i].job
		}
		signGroupAVX512(jobs[:n])
		for i := range n {
			batch[i].job.sig = jobs[i].sig
			jobs[i] = signJob{}
			batch[i].done.Done()
			batch[i] = poolJob{}
		}
	}
}

// computeSignatures fills jobs[i].sig, bit-identical to imageSignature.
func computeSignatures(jobs []signJob) {
	if len(jobs) == 0 {
		return
	}
	signPoolOnce.Do(startSignPool)
	var wg sync.WaitGroup
	wg.Add(len(jobs))
	for i := range jobs {
		signQueue <- poolJob{job: &jobs[i], done: &wg}
	}
	wg.Wait()
}

// computeSignature is the uncached single-signature entry used by imageSignature.
func computeSignature(key []byte, imageID string, exp int64) []byte {
	jobs := [1]signJob{{imageID: imageID, exp: exp}}
	copy(jobs[0].key[:], key)
	computeSignatures(jobs[:])
	return jobs[0].sig
}
