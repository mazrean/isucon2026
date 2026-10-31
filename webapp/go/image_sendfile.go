package isureal

import (
	"bytes"
	"io"
	"net/http"
	"os"
	"strconv"

	"github.com/labstack/echo/v5"
)

// ISUREAL_IMG_SENDFILE=1: on a memory-LRU miss, an image in the disk cache is
// sent with sendfile (the file is not read into the heap or the LRU; repeat
// reads come from the page cache). The body bytes are the same as c.Blob's;
// the response additionally carries Content-Length (net/http needs it to
// avoid chunking, which disables sendfile).
var imgSendfileOn = os.Getenv("ISUREAL_IMG_SENDFILE") == "1"

type imageFile struct {
	f         *os.File
	owner     string
	mediaType string
	off, n    int64
}

// openDisk opens the disk-cache file of id and parses its header
// ("owner\nmedia_type\n" + data). nil when absent or malformed.
func (s *imageStore) openDisk(id string) *imageFile {
	if s.dir == "" || !safeImageID(id) {
		return nil
	}
	f, err := os.Open(s.path(id))
	if err != nil {
		return nil
	}
	var hdr [512]byte
	n, _ := io.ReadFull(f, hdr[:])
	b := hdr[:n]
	i := bytes.IndexByte(b, '\n')
	j := -1
	if i >= 0 {
		j = bytes.IndexByte(b[i+1:], '\n')
	}
	st, err := f.Stat()
	if i < 0 || j < 0 || err != nil {
		f.Close()
		return nil
	}
	off := int64(i + 1 + j + 1)
	return &imageFile{f: f, owner: string(b[:i]), mediaType: string(b[i+1 : i+1+j]), off: off, n: st.Size() - off}
}

// sendImageFile writes 200 + the image body from the file. Falls back to a
// buffered Blob when the response writer is wrapped (sendfile needs net/http's).
func sendImageFile(c *echo.Context, file *imageFile) error {
	if _, err := file.f.Seek(file.off, io.SeekStart); err != nil {
		return err
	}
	r, ok := c.Response().(*echo.Response)
	if !ok {
		data, err := io.ReadAll(io.LimitReader(file.f, file.n))
		if err != nil {
			return err
		}
		return c.Blob(http.StatusOK, file.mediaType, data)
	}
	h := r.Header()
	if h.Get(echo.HeaderContentType) == "" {
		h.Set(echo.HeaderContentType, file.mediaType)
	}
	h.Set(echo.HeaderContentLength, strconv.FormatInt(file.n, 10))
	r.WriteHeader(http.StatusOK)
	n, err := io.Copy(r.ResponseWriter, io.LimitReader(file.f, file.n))
	r.Size += n
	r.Write(nil) // run echo's after-write hooks
	return err
}
