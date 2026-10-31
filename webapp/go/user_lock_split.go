package isureal

import (
	"fmt"
	"os"
)

// ISUREAL_USER_LOCK_SPLIT=1 (既定 0 = 従来どおり users の行を FOR NO KEY UPDATE):
//
// 従来は login・投稿・友達申請/承認・follow/block・RealMoji・コメントが、すべて users の行を FOR NO KEY UPDATE で
// 取り合っていた (互いに衝突する)。人気の利用者に申請が集まると、その人の login と投稿も後ろで待つ
// (asia2 の Lock:tuple / Lock:transactionid)。
//
// split では:
//   - 退会だけが users の行を FOR UPDATE で取る (その利用者の全操作と排他)。退会の UPDATE status は暗黙に
//     NO KEY UPDATE しか取らず FOR KEY SHARE と衝突しないので、必ず先に FOR UPDATE を明示する。
//   - それ以外は users の行を FOR KEY SHARE (互いに衝突せず FOR UPDATE とだけ衝突) にし、守るべき不変条件ごとに
//     advisory xact lock を足す:
//     login/logout = (userAdvSession, hashtext(user_id))、投稿の作成/更新/削除 = (userAdvPost, hashtext(user_id))、
//     2 人の関係 = (userAdvPair, hashtext(min || ':' || max)) (min/max は id のバイト順 = COLLATE "C")。
//   - 取る順序は固定: users の行 (組は min → max) → advisory (1 つの tx で高々 1 つ)。advisory を持ったまま
//     users の行を待つことはない。退会は advisory を取らない。だから待ちの輪はできない。
//   - 退会との競合: KEY SHARE を持つ tx がある間は退会の FOR UPDATE が待つ。退会が先にコミットすると、
//     KEY SHARE は新しい版 (status='deleted') を返すので (READ COMMITTED の再評価)、従来どおり拒否される。
//   - 既存の advisory (1 = 登録の username、2 = 窓の region、単一 bigint の 727274/727275) と名前空間が重ならない
//     クラス番号 (101〜103) を使う。hash の衝突は余計に待つだけで安全。
var userLockSplit = os.Getenv("ISUREAL_USER_LOCK_SPLIT") == "1"

const (
	userAdvNone    = 0
	userAdvSession = 101
	userAdvPost    = 102
	userAdvPair    = 103
)

// lockUserRowSQL: sel は `SELECT <id を含む列> FROM users WHERE ...` (1 行を返す)。
// 既定は sel + FOR NO KEY UPDATE。split なら FOR KEY SHARE の後に advisory (adv != 0 のとき) を同じ文で取る。
// CTE は MATERIALIZED なので、行のロック → (行が返ったときだけ) advisory の順になる。返る列は sel と同じ。
func lockUserRowSQL(sel string, adv int) string {
	if !userLockSplit {
		return sel + " FOR NO KEY UPDATE"
	}
	if adv == userAdvNone {
		return sel + " FOR KEY SHARE"
	}
	return fmt.Sprintf(`WITH u AS MATERIALIZED (%s FOR KEY SHARE) SELECT u.* FROM u, LATERAL (SELECT pg_advisory_xact_lock(%d, hashtext(u.id))) l`, sel, adv)
}

// deleteAccountLockSQL: 退会の行ロック。
func deleteAccountLockSQL() string {
	if userLockSplit {
		return `SELECT id FROM users WHERE id=$1 AND status='active' FOR UPDATE`
	}
	return `SELECT id FROM users WHERE id=$1 AND status='active' FOR NO KEY UPDATE`
}

// pairKey: 組の advisory のキー (min:max)。
func pairKey(a, b string) string { return min(a, b) + ":" + max(a, b) }

// relationshipLockSplitSQL: relationshipLockSQL の split 版 ($3 = pairKey)。2 行の KEY SHARE を min → max の順に
// 取り切ってから (MATERIALIZED の集約が先に u を読み切る)、組の advisory を 1 つ取る。返るのは active な id。
const relationshipLockSplitSQL = `WITH u AS MATERIALIZED (
  SELECT id FROM users WHERE id IN ($1, $2) AND status='active' ORDER BY id COLLATE "C" FOR KEY SHARE
), a AS MATERIALIZED (SELECT coalesce(array_agg(id), '{}') AS ids FROM u)
SELECT x.id FROM a, LATERAL (SELECT pg_advisory_xact_lock(103, hashtext($3::text)) WHERE cardinality(a.ids) >= 0) l, unnest(a.ids) AS x(id)`

// relationshipLock: 組の 2 行のロックの文と引数。
func relationshipLock(a, b string) (string, []any) {
	if userLockSplit {
		return relationshipLockSplitSQL, []any{a, b, pairKey(a, b)}
	}
	return relationshipLockSQL, []any{a, b}
}

// pairAdvisorySQL: すでに users の行を取った tx で、組の advisory だけを取る ($1 = pairKey)。
const pairAdvisorySQL = `SELECT pg_advisory_xact_lock(103, hashtext($1::text))`
