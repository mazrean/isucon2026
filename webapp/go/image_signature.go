package isureal

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"net/url"
	"strconv"
	"sync"
	"time"
)

const imageURLLifetime = 24 * time.Hour
const imageSignatureIterations = 1000

// signed URLs are reused while they stay valid for at least this long, so the
// 1000-round HMAC is computed about once per (key, image) per 10 minutes.
const imageURLReuseMargin = imageURLLifetime - 10*time.Minute

func imageSigningKey(secretKeyHash string) ([]byte, error) {
	key, err := hex.DecodeString(secretKeyHash)
	if err != nil || len(key) != sha256.Size {
		return nil, fmt.Errorf("invalid image signing key")
	}
	return key, nil
}

type signatureCacheKey struct {
	key     [sha256.Size]byte
	imageID string
	exp     int64
}

type signedURLCacheKey struct {
	key     [sha256.Size]byte
	imageID string
}

type signedURLEntry struct {
	exp int64
	url string
}

const signatureCacheLimit = 1 << 20

var (
	signatureCacheMu sync.RWMutex
	signatureCache   = make(map[signatureCacheKey][]byte)
	signedURLCacheMu sync.RWMutex
	signedURLCache   = make(map[signedURLCacheKey]signedURLEntry)
)

func imageSignature(key []byte, imageID string, exp int64) []byte {
	var ck signatureCacheKey
	copy(ck.key[:], key)
	ck.imageID, ck.exp = imageID, exp
	signatureCacheMu.RLock()
	cached, ok := signatureCache[ck]
	signatureCacheMu.RUnlock()
	if ok {
		return cached
	}
	// 16-lane AVX-512 kernel shared with concurrent misses (sign_pool.go).
	value := computeSignature(key, imageID, exp)
	signatureCacheMu.Lock()
	if len(signatureCache) >= signatureCacheLimit {
		signatureCache = make(map[signatureCacheKey][]byte)
	}
	signatureCache[ck] = value
	signatureCacheMu.Unlock()
	return value
}

func signedImageURL(user User, imageID string) string {
	var ck signedURLCacheKey
	copy(ck.key[:], user.ImageKey)
	ck.imageID = imageID
	// exp is floored to a shared 10-minute boundary (lifetime 23h50m-24h), so
	// every process and host signs/verifies the same (image, exp) pair.
	exp := time.Now().Add(imageURLLifetime).Unix()
	exp -= exp % int64((imageURLLifetime-imageURLReuseMargin)/time.Second)
	signedURLCacheMu.RLock()
	entry, ok := signedURLCache[ck]
	signedURLCacheMu.RUnlock()
	if ok && entry.exp == exp {
		return entry.url
	}
	query := url.Values{
		"exp": {strconv.FormatInt(exp, 10)},
		"sig": {base64.RawURLEncoding.EncodeToString(imageSignature(user.ImageKey, imageID, exp))},
	}
	u := "/api/images/" + url.PathEscape(imageID) + "?" + query.Encode()
	signedURLCacheMu.Lock()
	if len(signedURLCache) >= signatureCacheLimit {
		signedURLCache = make(map[signedURLCacheKey]signedURLEntry)
	}
	signedURLCache[ck] = signedURLEntry{exp: exp, url: u}
	signedURLCacheMu.Unlock()
	return u
}
