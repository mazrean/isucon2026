package isureal

import (
	"net/http"
	"os"

	"github.com/jmoiron/sqlx"
	"github.com/labstack/echo/v5"
	"github.com/labstack/echo/v5/middleware"

	_ "github.com/jackc/pgx/v5/stdlib"
)

type HandlerContext struct {
	DB     *sqlx.DB
	reads  *readRouter
	Config *Config
	IPBans *IPBanList
	BG     *sqlx.DB // small pool for background pollers
	// Admin: 管理者の窓の開閉と管理者セッションの確認だけが使う小さなプール (primary)。
	// GP の投稿ラッシュで DB (60 本) が埋まると、窓の POST が接続待ちの列に並んで 3〜5 秒遅れていた。
	Admin *sqlx.DB
	// Img: image_objects の bytea (361KB) の INSERT / SELECT だけが使うプール (primary)。
	// 投稿の画像 INSERT は TOAST の relation extension lock 待ちで数十〜数百 ms 接続を握るため、
	// DB (60 本) から外して、友達申請・login など短い書き込みの接続待ちを無くす。nil なら DB を使う。
	Img *sqlx.DB
	// ImgRead: 画像の GET (image_objects の SELECT) 用。INSERT と同じプールだと GET が INSERT の接続を奪う。
	ImgRead *sqlx.DB

	Notifications *notificationHub

	Mem     *memStore // ISUREAL_MEM=1 のときだけ非 nil
	memPump *memPump
	Store   *storeEngine // ISUREAL_STORE=1 (asia1) のときだけ非 nil
}

// Reader は、トランザクション外の読み取り専用クエリに使う接続を返す。
// ローカル replica が同期メンバーとして健全なときだけ replica、それ以外は primary。
func (h *HandlerContext) Reader() *sqlx.DB {
	return h.reads.reader()
}

func NewServer(config *Config) *echo.Echo {
	if !validRegion(config.Region) {
		panic("invalid server region: " + config.Region)
	}
	e := echo.New()
	e.IPExtractor = extractClientIP

	e.Use(ActionLogMiddleware(config.LogLevel, os.Stdout))
	e.Use(middleware.Recover())
	e.HTTPErrorHandler = APIErrorHandler
	registerPrevalidate(e, config.Region)

	db, err := sqlx.Open("pgx", config.DatabaseDSN())
	if err != nil {
		panic("failed to open database: " + err.Error())
	}
	// 既定の MaxIdleConns=2 だと 100ms 先の primary へ頻繁に再接続するため、プールを保持する。
	configurePool(db, config.DBMaxOpen)
	err = db.Ping()
	if err != nil {
		panic("failed to ping database: " + err.Error())
	}

	handlerContext := &HandlerContext{
		DB:     db,
		reads:  newReadRouter(config, db),
		Config: config,
		IPBans: &IPBanList{},
	}
	// background pollers (SSE hub, mem pump) get their own small pool so a burst
	// of writes holding every request connection cannot starve them.
	bgDB := sqlx.MustOpen("pgx", config.DatabaseDSN())
	configurePool(bgDB, 4)
	handlerContext.BG = bgDB
	adminDB := sqlx.MustOpen("pgx", config.DatabaseDSN())
	configurePool(adminDB, 4)
	handlerContext.Admin = adminDB
	if n := envIntOrDefault("ISUREAL_DB_IMG_OPEN", 12); n > 0 {
		imgDB := sqlx.MustOpen("pgx", config.DatabaseDSN())
		configurePool(imgDB, n)
		handlerContext.Img = imgDB
		// ASIA_LOCALIMG=1 では投稿が h.Img を使わず上限が無くなるので、枠から外さない。
		if !asiaLocalImgOn {
			fwdPriorityLocalExempt = func(req *http.Request) bool {
				return req.Method == http.MethodPost && req.URL.Path == "/api/posts"
			}
		}
	}
	if n := envIntOrDefault("ISUREAL_DB_IMG_READ_OPEN", 16); n > 0 && handlerContext.Img != nil {
		imgReadDB := sqlx.MustOpen("pgx", config.DatabaseDSN())
		configurePool(imgReadDB, n)
		handlerContext.ImgRead = imgReadDB
	}
	handlerContext.Notifications = newNotificationHub(func() *sqlx.DB { return bgDB })
	e.Use(IPBanMiddleware(handlerContext))
	e.Use(FwdPriorityMiddleware(handlerContext))
	setupMem(handlerContext, e)
	registerLocalImg(e, handlerContext)
	setupStore(handlerContext)
	registerLocalSession(e, handlerContext)

	e.POST("/api/initialize", initializeHandler(handlerContext))
	e.POST("/api/auth/register", registerHandler(handlerContext))
	e.POST("/api/auth/login", loginHandler(handlerContext))
	e.POST("/api/auth/logout", logoutHandler(handlerContext), SessionMiddleware(handlerContext, true))
	e.GET("/api/users", searchUsersHandler(handlerContext), SessionMiddleware(handlerContext, true), RegionMiddleware(handlerContext))
	e.GET("/api/users/:userId", getUserHandler(handlerContext), SessionMiddleware(handlerContext, true), RegionMiddleware(handlerContext))
	e.GET("/api/me/export", exportPostsHandler(handlerContext), SessionMiddleware(handlerContext, true), RegionMiddleware(handlerContext))
	e.GET("/api/me", getMeHandler(handlerContext), SessionMiddleware(handlerContext, true), RegionMiddleware(handlerContext))
	e.PATCH("/api/me", updateMeHandler(handlerContext), SessionMiddleware(handlerContext, true))
	e.PUT("/api/me/profile-image", putProfileImageHandler(handlerContext), SessionMiddleware(handlerContext, true))
	e.DELETE("/api/me/profile-image", deleteProfileImageHandler(handlerContext), SessionMiddleware(handlerContext, true))
	e.POST("/api/me/account-deletion", deleteAccountHandler(handlerContext), SessionMiddleware(handlerContext, true), RegionMiddleware(handlerContext))
	e.POST("/api/posts", createPostHandler(handlerContext), SessionMiddleware(handlerContext, true), RegionMiddleware(handlerContext))
	e.GET("/api/posting-window/current", getCurrentPostingWindowHandler(handlerContext), SessionMiddleware(handlerContext, true), RegionMiddleware(handlerContext))
	e.GET("/api/posts/me/current", getMyCurrentPostHandler(handlerContext), SessionMiddleware(handlerContext, true), RegionMiddleware(handlerContext))
	e.GET("/api/posts/:postId", getPostHandler(handlerContext), SessionMiddleware(handlerContext, true), RegionMiddleware(handlerContext))
	e.PATCH("/api/posts/:postId", updatePostHandler(handlerContext), SessionMiddleware(handlerContext, true), RegionMiddleware(handlerContext))
	e.DELETE("/api/posts/:postId", deletePostHandler(handlerContext), SessionMiddleware(handlerContext, true), RegionMiddleware(handlerContext))
	e.GET("/api/feeds/:feed", getFeedHandler(handlerContext), SessionMiddleware(handlerContext, true), RegionMiddleware(handlerContext))
	e.GET("/api/memories", getMemoriesHandler(handlerContext), SessionMiddleware(handlerContext, true), RegionMiddleware(handlerContext))
	e.GET("/api/memories/:date", getMemoriesByDateHandler(handlerContext), SessionMiddleware(handlerContext, true), RegionMiddleware(handlerContext))
	e.POST("/api/posts/:postId/comments", createCommentHandler(handlerContext), SessionMiddleware(handlerContext, true), RegionMiddleware(handlerContext))
	e.GET("/api/posts/:postId/comments", getCommentsHandler(handlerContext), SessionMiddleware(handlerContext, true), RegionMiddleware(handlerContext))
	e.DELETE("/api/comments/:commentId", deleteCommentHandler(handlerContext), SessionMiddleware(handlerContext, true), RegionMiddleware(handlerContext))
	e.POST("/api/posts/:postId/realmojis", replaceRealMojiHandler(handlerContext), SessionMiddleware(handlerContext, true), RegionMiddleware(handlerContext))
	e.GET("/api/posts/:postId/realmojis", getRealMojisHandler(handlerContext), SessionMiddleware(handlerContext, true), RegionMiddleware(handlerContext))
	e.DELETE("/api/realmojis/:realMojiId", deleteRealMojiHandler(handlerContext), SessionMiddleware(handlerContext, true), RegionMiddleware(handlerContext))
	e.GET("/api/images/:imageId", getImageHandler(handlerContext))
	registerImageReplicationRoutes(e)
	registerJPEGCheckRoutes(e)
	e.GET("/api/notifications", getNotificationsHandler(handlerContext), SessionMiddleware(handlerContext, true), RegionMiddleware(handlerContext))
	e.POST("/api/friend-requests", createFriendRequestHandler(handlerContext), SessionMiddleware(handlerContext, true), RegionMiddleware(handlerContext))
	e.POST("/api/friend-requests/:requestId/:action", transitionFriendRequestHandler(handlerContext), SessionMiddleware(handlerContext, true), RegionMiddleware(handlerContext))
	e.GET("/api/relationships/:kind", getRelationshipsHandler(handlerContext), SessionMiddleware(handlerContext, true), RegionMiddleware(handlerContext))
	e.DELETE("/api/friendships/:userId", deleteFriendshipHandler(handlerContext), SessionMiddleware(handlerContext, true), RegionMiddleware(handlerContext))
	e.PUT("/api/follows/:userId", followHandler(handlerContext), SessionMiddleware(handlerContext, true), RegionMiddleware(handlerContext))
	e.DELETE("/api/follows/:userId", unfollowHandler(handlerContext), SessionMiddleware(handlerContext, true), RegionMiddleware(handlerContext))
	e.PUT("/api/blocks/:userId", blockHandler(handlerContext), SessionMiddleware(handlerContext, true), RegionMiddleware(handlerContext))
	e.DELETE("/api/blocks/:userId", unblockHandler(handlerContext), SessionMiddleware(handlerContext, true), RegionMiddleware(handlerContext))
	e.POST("/api/admin/auth/login", adminLoginHandler(handlerContext))
	e.POST("/api/admin/auth/logout", adminLogoutHandler(handlerContext), AdminSessionMiddleware(handlerContext, true))
	e.PUT("/api/admin/users/:userId/certification", certifyUserHandler(handlerContext), AdminSessionMiddleware(handlerContext, true))
	e.DELETE("/api/admin/users/:userId/certification", deleteUserCertificationHandler(handlerContext), AdminSessionMiddleware(handlerContext, true))
	e.POST("/api/admin/posting-window", openPostingWindowHandler(handlerContext), AdminSessionMiddleware(handlerContext, true))

	return e
}
