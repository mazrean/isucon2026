package isureal

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/labstack/echo/v5"
)

type getImageRequest struct {
	ImageID string `param:"imageId" json:"-"`
	Exp     string `query:"exp" json:"-"`
	Sig     string `query:"sig" json:"-"`
}

func getImageHandler(h *HandlerContext) echo.HandlerFunc {
	return func(c *echo.Context) error {
		c.Response().Header().Set("Cache-Control", "no-store")
		var input getImageRequest
		if err := c.Bind(&input); err != nil {
			return NewAPIError(http.StatusForbidden, "image_forbidden")
		}
		query, err := url.ParseQuery(c.Request().URL.RawQuery)
		if err != nil || len(query["exp"]) != 1 || len(query["sig"]) != 1 {
			return NewAPIError(http.StatusForbidden, "image_forbidden")
		}
		exp, err := strconv.ParseInt(input.Exp, 10, 64)
		if err != nil || strconv.FormatInt(exp, 10) != input.Exp || exp <= time.Now().Unix() {
			return NewAPIError(http.StatusForbidden, "image_forbidden")
		}
		sig, err := base64.RawURLEncoding.Strict().DecodeString(input.Sig)
		if err != nil || len(sig) != sha256.Size || base64.RawURLEncoding.EncodeToString(sig) != input.Sig {
			return NewAPIError(http.StatusForbidden, "image_forbidden")
		}

		ctx := c.Request().Context()
		// 画像の中身 (owner / media_type / data) は ID ごとに不変なのでキャッシュから引く。
		rdb, pdb := h.imageDBs()
		obj, err := images.get(ctx, rdb, pdb, input.ImageID)
		if err != nil {
			return err
		}
		if obj == nil {
			return NewAPIError(http.StatusForbidden, "image_forbidden")
		}
		// owner の状態と削除状態は毎回 (1 秒以内の鮮度で) 確認する。
		validity, ok := memImageValid(h, input.ImageID, obj.Owner)
		if !ok {
			validity, err = checkImageValidity(ctx, h, input.ImageID, obj.Owner)
			if err != nil {
				return err
			}
		}
		if !validity.found {
			return NewAPIError(http.StatusForbidden, "image_forbidden")
		}
		key, err := imageSigningKey(validity.secretKeyHash)
		if err != nil {
			return err
		}
		if !hmac.Equal(sig, imageSignature(key, input.ImageID, exp)) {
			c.Set("abuse", "invalid_signature")
			return NewAPIError(http.StatusForbidden, "image_forbidden")
		}
		if !validity.valid {
			return NewAPIError(http.StatusForbidden, "image_forbidden")
		}
		if exp <= time.Now().Unix() {
			return NewAPIError(http.StatusForbidden, "image_forbidden")
		}
		return c.Blob(http.StatusOK, obj.MediaType, obj.Data)
	}
}

// imageDBs: images.get の (reader, primary)。primary 向けは画像の読み取り専用プール (h.ImgRead) に回す。
func (h *HandlerContext) imageDBs() (*sqlx.DB, *sqlx.DB) {
	r, p := h.Reader(), h.DB
	if h.ImgRead != nil {
		if r == h.DB {
			r = h.ImgRead
		}
		p = h.ImgRead
	}
	return r, p
}
