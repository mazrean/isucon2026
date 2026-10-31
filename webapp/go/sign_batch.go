package isureal

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"math/bits"
	"net/url"
	"os"
	"strconv"
	"time"
)

// signHasAVX512 enables the 16-lane chain kernel (set in init on amd64).
var signHasAVX512 bool

// batches with fewer cache misses than this use the scalar stdlib path: a
// 16-lane kernel run costs about as much as a few scalar chains.
var signSIMDMin = 3

func init() {
	if os.Getenv("ISUREAL_SIGN_SIMD") == "0" {
		signHasAVX512 = false
	}
	if v, err := strconv.Atoi(os.Getenv("ISUREAL_SIGN_SIMD_MIN")); err == nil && v > 0 {
		signSIMDMin = v
	}
}

// imageURLReq is one image URL to sign with the owner's image key.
type imageURLReq struct {
	Key     []byte
	ImageID string
}

func appendImageURLReq(reqs []imageURLReq, user User, imageID string) []imageURLReq {
	if len(user.ImageKey) != sha256.Size {
		return reqs
	}
	return append(reqs, imageURLReq{Key: user.ImageKey, ImageID: imageID})
}

// appendUserProfileImageReq adds the profile image URL that the user's
// response objects sign (newGetMeResponse / newRelationshipUserResponse).
func appendUserProfileImageReq(reqs []imageURLReq, user User) []imageURLReq {
	if user.ProfileImageID == nil {
		return reqs
	}
	return appendImageURLReq(reqs, user, *user.ProfileImageID)
}

func currentImageExp() int64 {
	exp := time.Now().Add(imageURLLifetime).Unix()
	exp -= exp % int64((imageURLLifetime-imageURLReuseMargin)/time.Second)
	return exp
}

func buildSignedImageURL(imageID string, exp int64, sig []byte) string {
	query := url.Values{
		"exp": {strconv.FormatInt(exp, 10)},
		"sig": {base64.RawURLEncoding.EncodeToString(sig)},
	}
	return "/api/images/" + url.PathEscape(imageID) + "?" + query.Encode()
}

// prewarmImageURLs signs every request whose URL is not cached yet, all at
// once (16 chains per AVX-512 kernel run, spread over the CPUs), so the
// signedImageURL calls that build the response afterwards hit the cache.
func prewarmImageURLs(reqs []imageURLReq) {
	if len(reqs) == 0 {
		return
	}
	_ = signedImageURLs(reqs)
}

// signedImageURLs returns signedImageURL for every request, computing the
// missing signatures as one batch.
func signedImageURLs(reqs []imageURLReq) []string {
	out := make([]string, len(reqs))
	exp := currentImageExp()
	type miss struct {
		ck  signatureCacheKey
		idx []int
	}
	var misses []miss
	missIdx := map[signatureCacheKey]int{}
	sigs := make([][]byte, len(reqs))

	signedURLCacheMu.RLock()
	for i, r := range reqs {
		var uk signedURLCacheKey
		copy(uk.key[:], r.Key)
		uk.imageID = r.ImageID
		if e, ok := signedURLCache[uk]; ok && e.exp == exp {
			out[i] = e.url
		}
	}
	signedURLCacheMu.RUnlock()

	signatureCacheMu.RLock()
	for i, r := range reqs {
		if out[i] != "" {
			continue
		}
		var ck signatureCacheKey
		copy(ck.key[:], r.Key)
		ck.imageID, ck.exp = r.ImageID, exp
		if s, ok := signatureCache[ck]; ok {
			sigs[i] = s
			continue
		}
		if j, ok := missIdx[ck]; ok {
			misses[j].idx = append(misses[j].idx, i)
			continue
		}
		missIdx[ck] = len(misses)
		misses = append(misses, miss{ck: ck, idx: []int{i}})
	}
	signatureCacheMu.RUnlock()

	if len(misses) > 0 {
		jobs := make([]signJob, len(misses))
		for j := range misses {
			jobs[j] = signJob{key: misses[j].ck.key, imageID: misses[j].ck.imageID, exp: exp}
		}
		computeSignatures(jobs)
		signatureCacheMu.Lock()
		if len(signatureCache)+len(jobs) > signatureCacheLimit {
			signatureCache = make(map[signatureCacheKey][]byte)
		}
		for j := range misses {
			signatureCache[misses[j].ck] = jobs[j].sig
			for _, i := range misses[j].idx {
				sigs[i] = jobs[j].sig
			}
		}
		signatureCacheMu.Unlock()
	}

	type fresh struct {
		k signedURLCacheKey
		u string
	}
	var news []fresh
	for i, r := range reqs {
		if out[i] != "" {
			continue
		}
		out[i] = buildSignedImageURL(r.ImageID, exp, sigs[i])
		var uk signedURLCacheKey
		copy(uk.key[:], r.Key)
		uk.imageID = r.ImageID
		news = append(news, fresh{uk, out[i]})
	}
	if len(news) > 0 {
		signedURLCacheMu.Lock()
		if len(signedURLCache)+len(news) > signatureCacheLimit {
			signedURLCache = make(map[signedURLCacheKey]signedURLEntry)
		}
		for _, n := range news {
			signedURLCache[n.k] = signedURLEntry{exp: exp, url: n.u}
		}
		signedURLCacheMu.Unlock()
	}
	return out
}

type signJob struct {
	key     [sha256.Size]byte
	imageID string
	exp     int64
	sig     []byte
}

// signatureScalar is the uncached stdlib computation (same as imageSignature).
func signatureScalar(key []byte, imageID string, exp int64) []byte {
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

// signGroupAVX512: round 1 (variable-length message) via stdlib hmac, rounds
// 2..1000 in the 16-lane kernel. Unused lanes are computed and discarded.
func signGroupAVX512(jobs []signJob) {
	var st chainState
	for l := range jobs {
		j := &jobs[l]
		ist, ost := hmacMidstates(j.key[:])
		mac := hmac.New(sha256.New, j.key[:])
		mac.Write([]byte(strconv.FormatInt(j.exp, 10) + ":" + j.imageID))
		var buf [sha256.Size]byte
		first := mac.Sum(buf[:0])
		for w := range 8 {
			st.ist[w][l] = ist[w]
			st.ost[w][l] = ost[w]
			st.v[w][l] = binary.BigEndian.Uint32(first[4*w:])
		}
	}
	chainAVX512(&st, imageSignatureIterations-1)
	for l := range jobs {
		sig := make([]byte, sha256.Size)
		for w := range 8 {
			binary.BigEndian.PutUint32(sig[4*w:], st.v[w][l])
		}
		jobs[l].sig = sig
	}
}

var sha256IV = [8]uint32{0x6a09e667, 0xbb67ae85, 0x3c6ef372, 0xa54ff53a, 0x510e527f, 0x9b05688c, 0x1f83d9ab, 0x5be0cd19}

// hmacMidstates returns the SHA-256 states after the key^ipad / key^opad block.
func hmacMidstates(key []byte) (ist, ost [8]uint32) {
	var ib, ob [64]byte
	copy(ib[:], key)
	copy(ob[:], key)
	for i := range ib {
		ib[i] ^= 0x36
		ob[i] ^= 0x5c
	}
	ist, ost = sha256IV, sha256IV
	compressGeneric(&ist, &ib)
	compressGeneric(&ost, &ob)
	return
}

var sha256K = [64]uint32{
	0x428a2f98, 0x71374491, 0xb5c0fbcf, 0xe9b5dba5, 0x3956c25b, 0x59f111f1, 0x923f82a4, 0xab1c5ed5,
	0xd807aa98, 0x12835b01, 0x243185be, 0x550c7dc3, 0x72be5d74, 0x80deb1fe, 0x9bdc06a7, 0xc19bf174,
	0xe49b69c1, 0xefbe4786, 0x0fc19dc6, 0x240ca1cc, 0x2de92c6f, 0x4a7484aa, 0x5cb0a9dc, 0x76f988da,
	0x983e5152, 0xa831c66d, 0xb00327c8, 0xbf597fc7, 0xc6e00bf3, 0xd5a79147, 0x06ca6351, 0x14292967,
	0x27b70a85, 0x2e1b2138, 0x4d2c6dfc, 0x53380d13, 0x650a7354, 0x766a0abb, 0x81c2c92e, 0x92722c85,
	0xa2bfe8a1, 0xa81a664b, 0xc24b8b70, 0xc76c51a3, 0xd192e819, 0xd6990624, 0xf40e3585, 0x106aa070,
	0x19a4c116, 0x1e376c08, 0x2748774c, 0x34b0bcb5, 0x391c0cb3, 0x4ed8aa4a, 0x5b9cca4f, 0x682e6ff3,
	0x748f82ee, 0x78a5636f, 0x84c87814, 0x8cc70208, 0x90befffa, 0xa4506ceb, 0xbef9a3f7, 0xc67178f2,
}

func compressGeneric(h *[8]uint32, block *[64]byte) {
	var w [64]uint32
	for i := range 16 {
		w[i] = binary.BigEndian.Uint32(block[4*i:])
	}
	for i := 16; i < 64; i++ {
		v1, v2 := w[i-2], w[i-15]
		t1 := bits.RotateLeft32(v1, -17) ^ bits.RotateLeft32(v1, -19) ^ (v1 >> 10)
		t2 := bits.RotateLeft32(v2, -7) ^ bits.RotateLeft32(v2, -18) ^ (v2 >> 3)
		w[i] = t1 + w[i-7] + t2 + w[i-16]
	}
	a, b, c, d, e, f, g, hh := h[0], h[1], h[2], h[3], h[4], h[5], h[6], h[7]
	for i := range 64 {
		t1 := hh + (bits.RotateLeft32(e, -6) ^ bits.RotateLeft32(e, -11) ^ bits.RotateLeft32(e, -25)) + ((e & f) ^ (^e & g)) + sha256K[i] + w[i]
		t2 := (bits.RotateLeft32(a, -2) ^ bits.RotateLeft32(a, -13) ^ bits.RotateLeft32(a, -22)) + ((a & b) ^ (a & c) ^ (b & c))
		hh, g, f, e, d, c, b, a = g, f, e, d+t1, c, b, a, t1+t2
	}
	h[0] += a
	h[1] += b
	h[2] += c
	h[3] += d
	h[4] += e
	h[5] += f
	h[6] += g
	h[7] += hh
}
