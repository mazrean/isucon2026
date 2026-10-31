// Go side of the signature micro-benchmark (same loop as image_signature.go).
package main

import (
	"crypto/hmac"
	"crypto/sha256"
	"fmt"
	"strconv"
	"time"
)

func main() {
	t := time.Now()
	for i := 0; i < 800; i++ {
		key := make([]byte, 32)
		for j := range key {
			key[j] = byte(i)
		}
		mac := hmac.New(sha256.New, key)
		mac.Write([]byte(strconv.FormatInt(1793501400, 10) + ":" + fmt.Sprintf("%08x-aaaa-bbbb-cccc-0123456789ab", i)))
		var buf [32]byte
		v := mac.Sum(buf[:0])
		for range 999 {
			mac.Reset()
			mac.Write(v)
			v = mac.Sum(v[:0])
		}
	}
	d := time.Since(t)
	fmt.Printf("go 800 sigs: %v (%.1fus/sig)\n", d, d.Seconds()*1e6/800)
}
