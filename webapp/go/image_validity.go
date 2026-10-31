package isureal

import (
	"context"
	"database/sql"
	"errors"
	"net"
	"net/url"
	"os"
	"sync"
	"time"

	"github.com/jmoiron/sqlx"
)

// 有効性判定の結果を保持する時間。削除の反映猶予 5 秒に対して十分小さくする
// (stale になりうるのは最大でこの時間 + replica の適用遅延)。
const imageValidityTTL = time.Second

type imageValidity struct {
	found         bool // owner が active
	secretKeyHash string
	valid         bool // プロフィール画像 / 削除されていない自分の投稿 / 有効な RealMoji
	at            time.Time
}

var (
	imageValidityMu    sync.RWMutex
	imageValidityCache = make(map[string]imageValidity)
	imageValidityGCAt  time.Time
)

var (
	imageReaderOnce sync.Once
	imageReaderDB   *sqlx.DB
)

// imageReader は画像の読み取りに使う DB。ISUREAL_DB_READ_HOST (ローカル replica) が
// 指定されていればそちら、無ければ primary。
// TODO: reader router (h.Reader()) がマージされたらそちらに置き換える。
func imageReader(h *HandlerContext) *sqlx.DB {
	imageReaderOnce.Do(func() {
		host := os.Getenv("ISUREAL_DB_READ_HOST")
		if host == "" || host == h.Config.DBHost {
			return
		}
		u := url.URL{
			Scheme: "postgres",
			User:   url.UserPassword(h.Config.DBUser, h.Config.DBPassword),
			Host:   net.JoinHostPort(host, h.Config.DBPort),
			Path:   "/" + h.Config.DBName,
		}
		db, err := sqlx.Open("pgx", u.String())
		if err != nil {
			return
		}
		db.SetMaxOpenConns(40)
		db.SetMaxIdleConns(40)
		if err := db.Ping(); err != nil {
			db.Close()
			return
		}
		imageReaderDB = db
	})
	if imageReaderDB != nil {
		return imageReaderDB
	}
	return h.DB
}

const imageValidityQuery = `
SELECT u.secret_key_hash, COALESCE(
  u.profile_image_id = $1
  OR EXISTS (
    SELECT 1 FROM posts p
    WHERE p.image_id = $1 AND p.author_id = u.id AND p.deleted_at IS NULL
  )
  OR EXISTS (
    SELECT 1 FROM realmojis r
    JOIN posts p ON p.id = r.post_id AND p.deleted_at IS NULL
    JOIN users author ON author.id = p.author_id AND author.status = 'active'
    WHERE r.image_id = $1 AND r.reactor_id = u.id AND r.replaced_at IS NULL AND r.deleted_at IS NULL
  ), false) AS valid
FROM users u
WHERE u.id = $2 AND u.status = 'active'`

func queryImageValidity(ctx context.Context, db *sqlx.DB, imageID, owner string) (imageValidity, error) {
	var row struct {
		SecretKeyHash string `db:"secret_key_hash"`
		Valid         bool   `db:"valid"`
	}
	err := db.GetContext(ctx, &row, imageValidityQuery, imageID, owner)
	if errors.Is(err, sql.ErrNoRows) {
		return imageValidity{found: false}, nil
	}
	if err != nil {
		return imageValidity{}, err
	}
	return imageValidity{found: true, secretKeyHash: row.SecretKeyHash, valid: row.Valid}, nil
}

// checkImageValidity は、owner の状態と画像の有効性を返す (TTL 付きでメモ化)。
// image_objects には触れず、posts / realmojis / users のインデックスだけで判定する。
func checkImageValidity(ctx context.Context, h *HandlerContext, imageID, owner string) (imageValidity, error) {
	now := time.Now()
	imageValidityMu.RLock()
	v, ok := imageValidityCache[imageID]
	imageValidityMu.RUnlock()
	if ok && now.Sub(v.at) < imageValidityTTL {
		return v, nil
	}

	reader := h.Reader()
	v, err := queryImageValidity(ctx, reader, imageID, owner)
	if err != nil {
		return v, err
	}
	// replica が遅れている可能性に備え、否定結果は primary で確かめる (誤 403 を防ぐ)。
	if (!v.found || !v.valid) && reader != h.DB {
		if v, err = queryImageValidity(ctx, h.DB, imageID, owner); err != nil {
			return v, err
		}
	}
	v.at = now

	imageValidityMu.Lock()
	if now.Sub(imageValidityGCAt) > 10*time.Second {
		for id, e := range imageValidityCache {
			if now.Sub(e.at) >= imageValidityTTL {
				delete(imageValidityCache, id)
			}
		}
		imageValidityGCAt = now
	}
	imageValidityCache[imageID] = v
	imageValidityMu.Unlock()
	return v, nil
}
