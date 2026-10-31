package isureal

import (
	"crypto/hmac"
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
	// one HMAC reused via Reset: the padded key blocks are hashed once, so each
	// round costs 2 SHA-256 compressions instead of 4 and allocates nothing.
	mac := hmac.New(sha256.New, key)
	mac.Write([]byte(strconv.FormatInt(exp, 10) + ":" + imageID))
	var buf [sha256.Size]byte
	value := mac.Sum(buf[:0])
	for range imageSignatureIterations - 1 {
		mac.Reset()
		mac.Write(value)
		value = mac.Sum(value[:0])
	}
	value = append([]byte(nil), value...)
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
	now := time.Now()
	signedURLCacheMu.RLock()
	entry, ok := signedURLCache[ck]
	signedURLCacheMu.RUnlock()
	if ok && time.Unix(entry.exp, 0).Sub(now) >= imageURLReuseMargin {
		return entry.url
	}
	exp := now.Add(imageURLLifetime).Unix()
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
