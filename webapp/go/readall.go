package isureal

import "io"

// readAllSized is io.ReadAll with the buffer pre-sized to the expected length
// (Content-Length / multipart part size), so a 300KB upload is one allocation
// instead of ~12 append-growths (memclr + memmove of every intermediate slice).
// The returned bytes are identical to io.ReadAll's.
func readAllSized(r io.Reader, n int64) ([]byte, error) {
	if n <= 0 || n > 64<<20 {
		return io.ReadAll(r)
	}
	b := make([]byte, 0, n+1)
	for {
		m, err := r.Read(b[len(b):cap(b)])
		b = b[:len(b)+m]
		if err != nil {
			if err == io.EOF {
				err = nil
			}
			return b, err
		}
		if len(b) == cap(b) {
			b = append(b, 0)[:len(b)]
		}
	}
}
