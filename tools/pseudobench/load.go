package main

import (
	"context"
	"fmt"
	"math"
	"math/rand/v2"
	"net/url"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// 疑似スコア (マニュアル配点表)。caps は投稿期間ごと・ユーザーごとの上限 (0 = 無制限)。
var points = map[string]float64{
	"feed_verify": 10, "feed_verify_last": 20, "feed": 1, "image": 2, "post_detail": 1, "interaction_list": 1,
	"profile": 1, "notification": 1, "memories_prev": 1, "realmoji": 5, "comment": 3, "interaction_delete": 1,
	"search": 2, "friend_request": 2, "friend_accept": 2, "follow": 1, "profile_update": 1, "profile_image": 3,
	"caption_update": 1, "memories_month": 3, "memories_date": 2, "export": 20, "own_image": 2,
}
var caps = map[string]int{"feed": 20, "image": 4, "post_detail": 8, "interaction_list": 8, "profile": 4, "realmoji": 5, "comment": 5}
var regionMul = map[string]float64{"asia": 1, "europe": 1.5, "america": 1.5}

type loadUser struct {
	*Client
	idx     int
	friends []int
	counts  map[string]int // per-day counters (reset per day)
	mu      sync.Mutex
}

type loadState struct {
	users   []*loadUser
	posts   sync.Map // userIdx -> PostJSON (current day)
	score   map[string]map[string]float64
	scoreMu sync.Mutex
	viol    atomic.Int64
	violMsg sync.Map
	sem     chan struct{}
}

func (s *loadState) add(u *loadUser, kind string) {
	if c := caps[kind]; c > 0 {
		u.mu.Lock()
		if u.counts[kind] >= c {
			u.mu.Unlock()
			return
		}
		u.counts[kind]++
		u.mu.Unlock()
	}
	s.scoreMu.Lock()
	if s.score[u.Region] == nil {
		s.score[u.Region] = map[string]float64{}
	}
	s.score[u.Region][kind] += points[kind] * regionMul[u.Region]
	s.scoreMu.Unlock()
}

func (s *loadState) violation(msg string) {
	s.viol.Add(1)
	s.violMsg.LoadOrStore(msg, true)
}

// gate bounds the number of in-flight requests.
func (s *loadState) gate(f func()) {
	s.sem <- struct{}{}
	defer func() { <-s.sem }()
	f()
}

func runLoad(base string, dur time.Duration, conc int, scale float64, doInit bool, verbose bool) int {
	start := time.Now()
	deadline := start.Add(dur)
	st := &loadState{score: map[string]map[string]float64{}, sem: make(chan struct{}, conc)}
	run := fmt.Sprintf("lb%d%03d", time.Now().Unix()%100000, rand.IntN(1000))

	if doInit {
		r := NewClient(base, "asia").JSONReq("POST", "/api/initialize", nil)
		if r.Code != 200 {
			fmt.Println("initialize failed:", r.String())
			return 1
		}
	}
	admin := NewClient(base, "asia")
	if r := admin.AdminLogin(); r.Code != 200 {
		fmt.Println("admin login failed:", r.String())
		return 1
	}

	// ---- setup: users (bench log: asia 18 / europe 7 / america 6) ----
	per := map[string]int{"asia": int(math.Round(18 * scale)), "europe": int(math.Round(7 * scale)), "america": int(math.Round(6 * scale))}
	for _, rg := range regions {
		for i := 0; i < max(per[rg], 1); i++ {
			u := &loadUser{Client: NewClient(base, rg), idx: len(st.users), counts: map[string]int{}}
			st.users = append(st.users, u)
		}
	}
	parallel(st.users, conc, func(u *loadUser) {
		u.Register(fmt.Sprintf("%s_%d", run, u.idx), fmt.Sprintf("Load %s %d", u.Region, u.idx))
		u.Login()
	})
	// friendships: ring within region (+2 neighbours) and one cross-region link.
	type pair struct{ a, b int }
	var pairs []pair
	byRegion := map[string][]int{}
	for _, u := range st.users {
		byRegion[u.Region] = append(byRegion[u.Region], u.idx)
	}
	for _, ids := range byRegion {
		for i := range ids {
			for _, d := range []int{1, 2} {
				if len(ids) > d && !(len(ids) == 2 && d == 1 && i == 1) {
					pairs = append(pairs, pair{ids[i], ids[(i+d)%len(ids)]})
				}
			}
		}
	}
	for i := range st.users {
		j := (i + len(st.users)/3) % len(st.users)
		if st.users[i].Region != st.users[j].Region {
			pairs = append(pairs, pair{i, j})
		}
	}
	seen := map[[2]int]bool{}
	var uniq []pair
	for _, p := range pairs {
		k := [2]int{min(p.a, p.b), max(p.a, p.b)}
		if p.a != p.b && !seen[k] {
			seen[k] = true
			uniq = append(uniq, p)
		}
	}
	var fmu sync.Mutex
	parallel(uniq, conc, func(p pair) {
		a, b := st.users[p.a], st.users[p.b]
		r := a.JSONReq("POST", "/api/friend-requests", map[string]string{"recipientId": b.ID})
		var v struct {
			RelationID string `json:"relationId"`
		}
		if r.Code != 201 || r.JSON(&v) != nil {
			return
		}
		st.add(a, "friend_request")
		if b.JSONReq("POST", "/api/friend-requests/"+v.RelationID+"/accept", nil).Code == 200 {
			st.add(b, "friend_accept")
			fmu.Lock()
			a.friends = append(a.friends, p.b)
			b.friends = append(b.friends, p.a)
			fmu.Unlock()
		}
	})
	// certify a few and follow them
	for i := 0; i < len(st.users); i += 10 {
		admin.JSONReq("PUT", "/api/admin/users/"+st.users[i].ID+"/certification", nil)
	}
	parallel(st.users, conc, func(u *loadUser) {
		t := (u.idx / 10) * 10
		if t != u.idx && u.JSONReq("PUT", "/api/follows/"+st.users[t].ID, nil).Code == 204 {
			st.add(u, "follow")
		}
	})
	setupDur := time.Since(start)

	// ---- days ----
	days := 5
	remain := time.Until(deadline)
	dayLen := remain / time.Duration(days)
	if dayLen < 2*time.Second {
		days = max(int(remain/(2*time.Second)), 1)
		dayLen = remain / time.Duration(days)
	}
	date, _ := time.Parse(time.DateOnly, initialMaxLocalDate)
	date = date.AddDate(0, 0, 1)
	var dayStats []string
	for day := 1; day <= days; day++ {
		last := day == days
		dayEnd := time.Now().Add(dayLen)
		if dayEnd.After(deadline) {
			dayEnd = deadline
		}
		st.posts = sync.Map{}
		for _, u := range st.users {
			u.counts = map[string]int{}
		}
		ctx, cancel := context.WithDeadline(context.Background(), dayEnd)
		// SSE subscribe before opening the window
		chs := make([]<-chan sseResult, len(st.users))
		parallel(st.users, conc, func(u *loadUser) {
			ch, err := u.SubscribeSSE(ctx)
			if err == nil {
				chs[u.idx] = ch
			}
		})
		openStart := time.Now()
		ds, _, err := openWindows(admin, date, 3600)
		openDur := time.Since(openStart)
		if err != nil {
			fmt.Println("open window failed:", err)
			cancel()
			break
		}
		date, _ = time.Parse(time.DateOnly, ds)
		date = date.AddDate(0, 0, 1)
		var posted, verified atomic.Int64
		var wg sync.WaitGroup
		for _, u := range st.users {
			wg.Add(1)
			go func(u *loadUser) {
				defer wg.Done()
				if ch := chs[u.idx]; ch != nil {
					select {
					case res := <-ch:
						if res.Err == nil {
							st.add(u, "notification")
						}
					case <-ctx.Done():
						return
					case <-time.After(5 * time.Second):
					}
				}
				userDay(ctx, st, u, last, day, &posted, &verified)
			}(u)
		}
		wg.Wait()
		cancel()
		dayStats = append(dayStats, fmt.Sprintf("day%d %s: open=%dms posted=%d/%d feedVerified=%d", day, ds, openDur.Milliseconds(), posted.Load(), len(st.users), verified.Load()))
		if time.Now().After(deadline) {
			break
		}
	}

	// ---- report ----
	fmt.Printf("load: users=%d (asia %d / europe %d / america %d) friendships=%d setup=%.1fs total=%.1fs conc=%d\n",
		len(st.users), per["asia"], per["europe"], per["america"], len(uniq), setupDur.Seconds(), time.Since(start).Seconds(), conc)
	for _, s := range dayStats {
		fmt.Println("  " + s)
	}
	printStats()
	total := 0.0
	kinds := map[string]float64{}
	fmt.Println("pseudo score by region:")
	for _, rg := range regions {
		sum := 0.0
		for k, v := range st.score[rg] {
			sum += v
			kinds[k] += v
		}
		total += sum
		fmt.Printf("  %-8s %8.1f\n", rg, sum)
	}
	keys := make([]string, 0, len(kinds))
	for k := range kinds {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool { return kinds[keys[i]] > kinds[keys[j]] })
	var parts []string
	for _, k := range keys {
		parts = append(parts, fmt.Sprintf("%s=%.0f", k, kinds[k]))
	}
	fmt.Println("  by kind: " + strings.Join(parts, " "))
	fmt.Printf("PSEUDO SCORE: %d\n", int(total))
	if n := st.viol.Load(); n > 0 {
		fmt.Printf("CONSISTENCY VIOLATIONS: %d\n", n)
		st.violMsg.Range(func(k, _ any) bool { fmt.Println("  -", k); return true })
		return 1
	}
	return 0
}

func userDay(ctx context.Context, st *loadState, u *loadUser, last bool, day int, posted, verified *atomic.Int64) {
	alive := func() bool { return ctx.Err() == nil }
	// post
	vis := []string{"friends", "friends", "friends_of_friends", "public"}[rand.IntN(4)]
	var r *Resp
	var p PostJSON
	img := makeJPEG(160+rand.IntN(160), 120+rand.IntN(120))
	st.gate(func() { r, p = u.Post(vis, fmt.Sprintf("day%d chair by %d", day, u.idx), img) })
	if r.Code != 201 {
		if alive() && r.Err == nil {
			st.violation(fmt.Sprintf("post failed: %d %s", r.Code, r.ErrCode()))
		}
		return
	}
	posted.Add(1)
	st.posts.Store(u.idx, p)
	// own image + detail right after posting (separate from browse caps)
	st.gate(func() { r = u.Get(p.ImageURL) })
	if r.Code == 200 {
		st.add(u, "own_image")
	}

	// verify friend feed: all friends' posts known before the request must appear.
	verifiedOnce := false
	for alive() && !verifiedOnce {
		want := map[string]bool{}
		for _, f := range u.friends {
			if v, ok := st.posts.Load(f); ok {
				want[v.(PostJSON).ID] = true
			}
		}
		var items []PostJSON
		st.gate(func() { r, items = u.Feed("friends") })
		if r.Code != 200 {
			break
		}
		st.add(u, "feed")
		got := map[string]bool{}
		for _, it := range items {
			got[it.ID] = true
			if !isFriend(st, u, it.Author.ID) {
				st.violation("friends feed contains non-friend post")
			}
		}
		ok := true
		for id := range want {
			if !got[id] {
				ok = false
			}
		}
		if ok && len(want) > 0 {
			verifiedOnce = true
			verified.Add(1)
			if last {
				st.add(u, "feed_verify_last")
			} else {
				st.add(u, "feed_verify")
			}
			break
		}
		select {
		case <-ctx.Done():
		case <-time.After(300 * time.Millisecond):
		}
	}

	// memories (previous posts)
	if alive() {
		st.gate(func() { r = u.Get("/api/memories?month=" + time.Now().Format("2006-01")) })
		if r.Code == 200 {
			st.add(u, "memories_month")
		}
	}
	if last && alive() && u.idx%3 == 0 {
		st.gate(func() { r = u.Get("/api/me/export") })
		if r.Code == 200 {
			st.add(u, "export")
		}
	}
	if day == 2 && alive() && u.idx%4 == 0 {
		st.gate(func() {
			r = u.JSONReq("PATCH", "/api/me", map[string]string{"bio": fmt.Sprintf("bio %d", day)})
		})
		if r.Code == 200 {
			st.add(u, "profile_update")
		}
	}
	if day == 3 && alive() && u.idx%5 == 0 {
		st.gate(func() {
			r = u.do("PUT", "/api/me/profile-image", strings.NewReader(string(makeJPEG(96, 96))), "image/jpeg", true)
		})
		if r.Code == 204 {
			st.add(u, "profile_image")
		}
	}
	if alive() && rand.IntN(3) == 0 {
		st.gate(func() { r = u.Get("/api/users?q=" + url.QueryEscape("load")) })
		if r.Code == 200 {
			st.add(u, "search")
		}
	}

	// browse loop
	for alive() {
		var items []PostJSON
		feed := []string{"friends", "friends", "public", "friends-of-friends", "following"}[rand.IntN(5)]
		st.gate(func() { r, items = u.Feed(feed) })
		if r.Code == 200 {
			st.add(u, "feed")
		}
		if len(items) == 0 {
			select {
			case <-ctx.Done():
				return
			case <-time.After(200 * time.Millisecond):
			}
			continue
		}
		it := items[rand.IntN(len(items))]
		steps := []func(){
			func() {
				st.gate(func() { r = u.Get(it.ImageURL) })
				if r.Code == 200 {
					st.add(u, "image")
				}
			},
			func() {
				st.gate(func() { r = u.Get("/api/posts/" + it.ID) })
				if r.Code == 200 {
					st.add(u, "post_detail")
				}
			},
			func() {
				st.gate(func() { r = u.Get("/api/posts/" + it.ID + "/realmojis") })
				if r.Code == 200 {
					st.add(u, "interaction_list")
				}
			},
			func() {
				if !isFriend(st, u, it.Author.ID) && it.Visibility != "public" {
					return
				}
				st.gate(func() { r = u.Get("/api/posts/" + it.ID + "/comments") })
				if r.Code == 200 {
					st.add(u, "interaction_list")
				}
			},
			func() {
				st.gate(func() { r = u.Get("/api/users/" + it.Author.ID) })
				if r.Code == 200 {
					st.add(u, "profile")
				}
			},
			func() {
				st.gate(func() {
					r = u.Multipart("/api/posts/"+it.ID+"/realmojis", map[string]string{"reactionType": "like"}, makeJPEG(48, 48), true)
				})
				if r.Code == 201 {
					st.add(u, "realmoji")
				}
			},
			func() {
				if !isFriend(st, u, it.Author.ID) && it.Visibility != "public" {
					return
				}
				st.gate(func() {
					r = u.JSONReq("POST", "/api/posts/"+it.ID+"/comments", map[string]string{"body": "nice chair"})
				})
				if r.Code == 201 {
					st.add(u, "comment")
				}
			},
		}
		for _, i := range rand.Perm(len(steps))[:3] {
			if !alive() {
				return
			}
			steps[i]()
		}
	}
}

func isFriend(st *loadState, u *loadUser, authorID string) bool {
	for _, f := range u.friends {
		if st.users[f].ID == authorID {
			return true
		}
	}
	return false
}

func parallel[T any](items []T, conc int, f func(T)) {
	sem := make(chan struct{}, conc)
	var wg sync.WaitGroup
	for _, it := range items {
		wg.Add(1)
		sem <- struct{}{}
		go func(it T) {
			defer wg.Done()
			defer func() { <-sem }()
			f(it)
		}(it)
	}
	wg.Wait()
}

func printStats() {
	stats.mu.Lock()
	defer stats.mu.Unlock()
	keys := make([]string, 0, len(stats.m))
	total := 0
	for k, s := range stats.m {
		keys = append(keys, k)
		total += len(s.durs)
	}
	sort.Slice(keys, func(i, j int) bool { return len(stats.m[keys[i]].durs) > len(stats.m[keys[j]].durs) })
	fmt.Printf("%-48s %6s %5s %8s %8s %8s  %s\n", "endpoint", "count", "err", "p50ms", "p99ms", "maxms", "status")
	for _, k := range keys {
		s := stats.m[k]
		d := append([]time.Duration(nil), s.durs...)
		sort.Slice(d, func(i, j int) bool { return d[i] < d[j] })
		q := func(p float64) float64 {
			return float64(d[min(int(float64(len(d))*p), len(d)-1)].Microseconds()) / 1000
		}
		var sts []string
		for code, n := range s.status {
			sts = append(sts, fmt.Sprintf("%d:%d", code, n))
		}
		sort.Strings(sts)
		fmt.Printf("%-48s %6d %5d %8.1f %8.1f %8.1f  %s\n", k, len(d), s.errors, q(0.5), q(0.99), q(1), strings.Join(sts, " "))
	}
	fmt.Printf("total requests: %d\n", total)
}
