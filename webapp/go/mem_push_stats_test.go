package isureal

import (
	"bytes"
	"io"
	"net/http/httptest"
	"strings"
	"testing"
)

// gzip は push だけ、閾値以上だけ。受け側は Content-Encoding で展開する。
func TestMemPushGzipRoundTrip(t *testing.T) {
	old := memPushGzip
	memPushGzip = true
	defer func() { memPushGzip = old }()
	data := []byte(strings.Repeat(`{"q":1,"c":[]}`, 200))
	if _, gz := memMaybeGzip("/internal/mem/wm", data); gz {
		t.Fatal("only push is compressed")
	}
	if _, gz := memMaybeGzip("/internal/mem/push", data[:100]); gz {
		t.Fatal("small bodies are not compressed")
	}
	wire, gz := memMaybeGzip("/internal/mem/push", data)
	if !gz || len(wire) >= len(data) {
		t.Fatalf("gz=%v %d >= %d", gz, len(wire), len(data))
	}
	for _, c := range []struct {
		body []byte
		enc  string
	}{{wire, "gzip"}, {data, ""}} {
		req := httptest.NewRequest("POST", "/internal/mem/push", bytes.NewReader(c.body))
		if c.enc != "" {
			req.Header.Set("Content-Encoding", c.enc)
		}
		r, done, err := memRequestBody(req)
		if err != nil {
			t.Fatal(err)
		}
		got, _ := io.ReadAll(r)
		done()
		if !bytes.Equal(got, data) {
			t.Fatalf("enc=%q mismatch", c.enc)
		}
	}
}
