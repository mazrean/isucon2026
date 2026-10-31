package isureal

import (
	"bytes"
	"context"
	"strconv"
	"testing"
	"time"
)

func TestImageStoreDiskRoundTrip(t *testing.T) {
	s := newImageStore(t.TempDir())
	id := "0b0c5f3e-1111-4222-8333-944455556666"
	obj := &imageObject{Owner: "owner-1", MediaType: "image/jpeg", Data: []byte("a\nb\x00c\n")}
	s.writes <- imageDiskWrite{id: id, obj: obj}
	deadline := time.Now().Add(2 * time.Second)
	var got *imageObject
	for time.Now().Before(deadline) {
		if got = s.readDisk(id); got != nil {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if got == nil || got.Owner != obj.Owner || got.MediaType != obj.MediaType || !bytes.Equal(got.Data, obj.Data) {
		t.Fatalf("round trip mismatch: %+v", got)
	}
	// ディスクから引けるので DB (nil) には触れない。
	got2, err := s.get(context.Background(), nil, id)
	if err != nil || got2 == nil || !bytes.Equal(got2.Data, obj.Data) {
		t.Fatalf("get from disk failed: %v %+v", err, got2)
	}
	if _, ok := s.mem.get(id); !ok {
		t.Fatal("not promoted to memory")
	}
	if s.readDisk("../etc/passwd") != nil || safeImageID("../x") {
		t.Fatal("unsafe id accepted")
	}
}

func TestImageLRUBudget(t *testing.T) {
	l := newImageLRU(imageMemCacheShards * 10000)
	for i := 0; i < 10000; i++ {
		l.add("id-"+strconv.Itoa(i), &imageObject{Data: make([]byte, 1000)})
	}
	var total int64
	for i := range l.shards {
		if l.shards[i].bytes > l.shards[i].limit {
			t.Fatalf("shard %d over budget: %d", i, l.shards[i].bytes)
		}
		total += l.shards[i].bytes
	}
	if total == 0 {
		t.Fatal("empty")
	}
}

func BenchmarkImageLRUGet(b *testing.B) {
	l := newImageLRU(imageMemCacheBudget)
	l.add("x", &imageObject{Data: make([]byte, 8000)})
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			l.get("x")
		}
	})
}
