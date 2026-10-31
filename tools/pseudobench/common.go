package main

import (
	"archive/zip"
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"time"
)

var regions = []string{"asia", "europe", "america"}

const initialMaxLocalDate = "2026-10-27"

// openWindows opens a posting window with the same localDate in all regions.
// The date is probed (exponentially) past existing windows.
func openWindows(admin *Client, from time.Time, onTimeSeconds int64) (string, map[string]string, error) {
	date := from
	step := 1
	for attempt := 0; attempt < 40; attempt++ {
		ds := date.Format(time.DateOnly)
		ids := map[string]string{}
		conflict := false
		for _, rg := range regions {
			r := admin.JSONNoExpect("POST", "/api/admin/posting-window", map[string]any{"region": rg, "localDate": ds, "onTimeSeconds": onTimeSeconds})
			if r.Code == 409 {
				conflict = true
				break
			}
			if r.Code != 201 {
				return ds, ids, fmt.Errorf("open window %s %s: %s", rg, ds, r.String())
			}
			var v struct {
				ID string `json:"id"`
			}
			_ = r.JSON(&v)
			ids[rg] = v.ID
		}
		if !conflict {
			return ds, ids, nil
		}
		date = date.AddDate(0, 0, step)
		step *= 2
	}
	return "", nil, fmt.Errorf("could not find free localDate")
}

func sha(b []byte) string {
	s := sha256.Sum256(b)
	return hex.EncodeToString(s[:])
}

func parseExport(body []byte) ([]PostJSON, map[string]string, error) {
	zr, err := zip.NewReader(bytes.NewReader(body), int64(len(body)))
	if err != nil {
		return nil, nil, err
	}
	shas := map[string]string{}
	var items []PostJSON
	for _, f := range zr.File {
		rc, err := f.Open()
		if err != nil {
			return nil, nil, err
		}
		b, err := io.ReadAll(rc)
		rc.Close()
		if err != nil {
			return nil, nil, err
		}
		if f.Name == "posts.json" {
			if err := json.Unmarshal(b, &items); err != nil {
				return nil, nil, err
			}
		} else {
			shas[f.Name] = sha(b)
		}
	}
	return items, shas, nil
}
