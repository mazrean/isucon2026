package isureal

import (
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"time"

	"github.com/jmoiron/sqlx"
)

// ISUREAL_IMAGE_PREWARM=1: 起動時に image_objects の全画像をローカルディスクキャッシュへ書き出す (既にあるものは飛ばす)。
// 1 回温めればディスクに残るので、以後の再起動では stat だけで終わる。帯域は ISUREAL_IMAGE_PREWARM_MBPS (既定 8MB/s) で抑える。
func startImagePrewarm(db *sqlx.DB) {
	if os.Getenv("ISUREAL_IMAGE_PREWARM") != "1" || images.dir == "" {
		return
	}
	go func() {
		if err := imagePrewarm(context.Background(), db, envIntOrDefault("ISUREAL_IMAGE_PREWARM_MBPS", 8)); err != nil {
			slog.Warn("imgwarm: failed", "error", err)
		}
	}()
}

func imagePrewarm(ctx context.Context, db *sqlx.DB, mbps int) error {
	start := time.Now()
	var ids []string
	if err := db.SelectContext(ctx, &ids, `SELECT id FROM image_objects`); err != nil {
		return err
	}
	var missing []string
	for _, id := range ids {
		if !safeImageID(id) {
			continue
		}
		if _, err := os.Stat(images.path(id)); err != nil {
			missing = append(missing, id)
		}
	}
	slog.Info("imgwarm: start", "total", len(ids), "missing", len(missing))
	var bytesDone int64
	for i := 0; i < len(missing); i += 100 {
		chunk := missing[i:min(i+100, len(missing))]
		var rows []struct {
			ID        string `db:"id"`
			Owner     string `db:"owner_user_id"`
			MediaType string `db:"media_type"`
			Data      []byte `db:"data"`
		}
		if err := db.SelectContext(ctx, &rows, `SELECT id, owner_user_id, media_type, data FROM image_objects WHERE id = ANY($1)`, chunk); err != nil {
			return err
		}
		for _, r := range rows {
			writeImageDisk(r.ID, &imageObject{Owner: r.Owner, MediaType: r.MediaType, Data: r.Data})
			bytesDone += int64(len(r.Data))
		}
		// 帯域制限: 経過時間が bytesDone / mbps に追いつくまで待つ
		if mbps > 0 {
			want := time.Duration(float64(bytesDone) / float64(mbps<<20) * float64(time.Second))
			if d := want - time.Since(start); d > 0 {
				time.Sleep(d)
			}
		}
	}
	slog.Info("imgwarm: done", "written", len(missing), "bytes", bytesDone, "elapsed", time.Since(start))
	return nil
}

func writeImageDisk(id string, obj *imageObject) {
	p := images.path(id)
	if _, err := os.Stat(p); err == nil {
		return
	}
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		return
	}
	f, err := os.CreateTemp(filepath.Dir(p), ".tmp-*")
	if err != nil {
		return
	}
	buf := make([]byte, 0, len(obj.Owner)+len(obj.MediaType)+2+len(obj.Data))
	buf = append(buf, obj.Owner...)
	buf = append(buf, '\n')
	buf = append(buf, obj.MediaType...)
	buf = append(buf, '\n')
	buf = append(buf, obj.Data...)
	_, werr := f.Write(buf)
	cerr := f.Close()
	if werr != nil || cerr != nil {
		os.Remove(f.Name())
		return
	}
	if err := os.Rename(f.Name(), p); err != nil {
		os.Remove(f.Name())
	}
}
