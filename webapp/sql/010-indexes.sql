-- 追加インデックス (既存 DB へは CONCURRENTLY で適用済み。init/reset では消えない)
CREATE INDEX CONCURRENTLY IF NOT EXISTS posts_image_id ON posts(image_id);
CREATE INDEX CONCURRENTLY IF NOT EXISTS realmojis_image_id ON realmojis(image_id);
CREATE INDEX CONCURRENTLY IF NOT EXISTS friendships_user_high_id ON friendships(user_high_id);
CREATE INDEX CONCURRENTLY IF NOT EXISTS comments_author_id ON comments(author_id);
CREATE INDEX CONCURRENTLY IF NOT EXISTS realmojis_reactor_id ON realmojis(reactor_id);
CREATE INDEX CONCURRENTLY IF NOT EXISTS follows_followee_id ON follows(followee_id);
CREATE INDEX CONCURRENTLY IF NOT EXISTS block_relations_blocked_id ON block_relations(blocked_id);
CREATE INDEX CONCURRENTLY IF NOT EXISTS friend_requests_recipient ON friend_requests(recipient_id, status);
CREATE INDEX CONCURRENTLY IF NOT EXISTS friend_requests_requester ON friend_requests(requester_id, status);
CREATE INDEX CONCURRENTLY IF NOT EXISTS login_sessions_token_hash ON login_sessions(token_hash);
CREATE INDEX CONCURRENTLY IF NOT EXISTS login_sessions_user_active ON login_sessions(user_id) WHERE active;
CREATE INDEX CONCURRENTLY IF NOT EXISTS posting_windows_active ON posting_windows(region) WHERE ended_at IS NULL;
CREATE INDEX CONCURRENTLY IF NOT EXISTS posts_author_posted ON posts(author_id, posted_at DESC);
CREATE INDEX CONCURRENTLY IF NOT EXISTS notifications_user_created ON notifications(user_id, created_at);
