package isureal

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/labstack/echo/v5"
)

// GP の締め切りでクライアントが投稿の接続を切っても、asia1 が後から commit (201) したなら、eu/am の LOCALIMG の
// data と meta は残り、画像が持ち主付きで取れる (job25833 / 27110 の 403)。逆に asia1 が 4xx / 5xx (commit しない) なら、
// 持ち主付きでは公開されない (meta 無し、localImgOwn は nil = 画像の GET は 403。data の孤児は可)。
func TestLocalImgClientCancelDuringForward(t *testing.T) {
	for _, tc := range []struct {
		name   string
		status int
		kept   bool
	}{
		{"asia1 commits after cancel", http.StatusCreated, true},
		{"asia1 rejects 409", http.StatusConflict, false},
		{"asia1 fails 500", http.StatusInternalServerError, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			setupLocalImgTest(t)
			good := testJPEG(t, 120, 90)
			got := make(chan string, 1)
			release := make(chan struct{})
			done := make(chan struct{})

			// asia1 役: 受けたことを知らせ、クライアントが切れた後に (遅れて) 答える
			asia := echo.New()
			asia.Pre(localImgAccept)
			asia.POST("/api/posts", func(c *echo.Context) error {
				defer close(done)
				id := newImageID(c.Request().Context())
				got <- id
				<-release
				if !externalImage(c, "owner-1") {
					return c.String(500, "not external")
				}
				return c.String(tc.status, id)
			})
			asiaSrv := httptest.NewServer(asia)
			t.Cleanup(asiaSrv.Close)
			prevalidateTarget = strings.TrimPrefix(asiaSrv.URL, "http://")

			// eu 役: prevalidate の proxy で投稿を受ける
			eu := echo.New()
			registerPrevalidate(eu, "europe")
			registerLocalImgRoutes(eu)
			localImgRegion = "europe"
			euSrv := httptest.NewServer(eu)
			t.Cleanup(euSrv.Close)

			ct, body := multipartBody(t, map[string]string{"caption": "hi", "visibility": "public"}, map[string][]byte{"image": good})
			ctx, cancel := context.WithCancel(context.Background())
			req, _ := http.NewRequestWithContext(ctx, "POST", euSrv.URL+"/api/posts", bytes.NewReader(body))
			req.Header.Set("Content-Type", ct)
			req.Header.Set(prevalidateHeader, "test-secret")
			errc := make(chan error, 1)
			go func() {
				res, err := http.DefaultClient.Do(req)
				if err == nil {
					res.Body.Close()
				}
				errc <- err
			}()
			var id string
			select {
			case id = <-got:
			case <-time.After(5 * time.Second):
				t.Fatal("asia1 never saw the forward")
			}
			if localImgOriginOf(id) != "europe" {
				t.Fatalf("id %q is not a europe LOCALIMG id", id)
			}
			cancel() // GP の締め切り: クライアントが切る
			if err := <-errc; err == nil {
				t.Fatal("client request was not cancelled")
			}
			time.Sleep(50 * time.Millisecond) // eu がクライアントの切断を見る時間
			close(release)                    // asia1 が遅れて答える
			<-done

			metaPath, dataPath := localStore.p("meta", id), localStore.p("data", id)
			if tc.kept {
				deadline := time.Now().Add(3 * time.Second)
				for {
					if _, err := os.Stat(metaPath); err == nil {
						break
					}
					if time.Now().After(deadline) {
						t.Fatal("meta never written after asia1 committed")
					}
					time.Sleep(10 * time.Millisecond)
				}
				if _, err := os.Stat(dataPath); err != nil {
					t.Fatalf("data removed although asia1 committed: %v", err)
				}
				obj := localImgOwn(id)
				if obj == nil || obj.Owner != "owner-1" || !bytes.Equal(obj.Data, good) {
					t.Fatalf("committed image not served: %v", obj)
				}
				return
			}
			// commit しなかった: 転送が終わるのを待ってから、meta が無く、持ち主付きで公開されていないこと
			time.Sleep(300 * time.Millisecond)
			if _, err := os.Stat(metaPath); err == nil {
				t.Fatal("meta written for an uncommitted post")
			}
			if obj := localImgOwn(id); obj != nil {
				t.Fatalf("uncommitted image served with owner %q", obj.Owner)
			}
			if obj, _ := localStore.read(id); obj != nil {
				t.Fatal("uncommitted image readable from the store")
			}
		})
	}
}
