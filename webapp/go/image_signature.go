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
// 1000-round HMAC is computed about once per (key, image) per hour.
const imageURLReuseMargin = imageURLLifetime - time.Hour

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
	value := []byte(strconv.FormatInt(exp, 10) + ":" + imageID)
	for range imageSignatureIterations {
		mac := hmac.New(sha256.New, key)
		mac.Write(value)
		value = mac.Sum(nil)
	}
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
