package jpegcheck

import (
	"os"
	"strings"
	"testing"
)

// markerScan is a decode-free structural check (SOI, segment lengths, SOF,
// SOS, entropy data skipped to the next non-RST marker, EOI). It exists only
// to measure how far such a scan is from image/jpeg.Decode; it is not used by
// the app because it cannot see entropy-coded corruption.
func markerScan(d []byte) (w, h int, ok bool) {
	if len(d) < 4 || d[0] != 0xff || d[1] != 0xd8 {
		return 0, 0, false
	}
	i := 2
	sawSOF, sawSOS := false, false
	for {
		for i < len(d) && d[i] == 0xff && i+1 < len(d) && d[i+1] == 0xff {
			i++
		}
		if i+1 >= len(d) || d[i] != 0xff {
			return 0, 0, false
		}
		m := d[i+1]
		i += 2
		switch {
		case m == 0xd9:
			if !sawSOS {
				return 0, 0, false
			}
			return w, h, true
		case m >= 0xd0 && m <= 0xd7, m == 0x01:
			continue
		}
		if i+1 >= len(d) {
			return 0, 0, false
		}
		n := int(d[i])<<8 | int(d[i+1])
		if n < 2 || i+n > len(d) {
			return 0, 0, false
		}
		seg := d[i+2 : i+n]
		switch m {
		case 0xc0, 0xc1, 0xc2:
			if sawSOF || len(seg) < 6 {
				return 0, 0, false
			}
			h = int(seg[1])<<8 | int(seg[2])
			w = int(seg[3])<<8 | int(seg[4])
			nc := int(seg[5])
			if w == 0 || h == 0 || (nc != 1 && nc != 3 && nc != 4) || len(seg) != 6+3*nc {
				return 0, 0, false
			}
			sawSOF = true
		case 0xc3, 0xc5, 0xc6, 0xc7, 0xc9, 0xca, 0xcb, 0xcd, 0xce, 0xcf:
			return 0, 0, false
		case 0xda:
			if !sawSOF {
				return 0, 0, false
			}
			sawSOS = true
			i += n
			// skip entropy-coded data
			for i < len(d) {
				if d[i] == 0xff && i+1 < len(d) && d[i+1] != 0x00 && !(d[i+1] >= 0xd0 && d[i+1] <= 0xd7) {
					break
				}
				i++
			}
			continue
		}
		i += n
	}
}

// TestMarkerScanGap: how many inputs a decode-free scan would judge
// differently from image/jpeg.Decode, by input class.
func TestMarkerScanGap(t *testing.T) {
	dir := os.Getenv("JPEGCHECK_CORPUS")
	if dir == "" {
		t.Skip("JPEGCHECK_CORPUS not set")
	}
	type tally struct{ n, falseAccept, falseReject int }
	by := map[string]*tally{}
	forEachCase(t, dir, func(name string, data []byte) {
		cls := name
		if k := strings.LastIndex(name, " "); k >= 0 {
			cls = name[k+1:]
		} else if strings.Contains(name, "/") {
			cls = "orig"
		}
		tl := by[cls]
		if tl == nil {
			tl = &tally{}
			by[cls] = tl
		}
		tl.n++
		rw, rh, rok := ref(data)
		mw, mh, mok := markerScan(data)
		switch {
		case mok && !rok:
			tl.falseAccept++
		case !mok && rok:
			tl.falseReject++
		case mok && (mw != rw || mh != rh):
			tl.falseAccept++
		}
	})
	for k, v := range by {
		t.Logf("%-6s n=%5d false_accept=%5d false_reject=%5d", k, v.n, v.falseAccept, v.falseReject)
	}
}
