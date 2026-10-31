package isureal

import (
	"slices"
)

// ISUREAL_MEM_APPLY_CHUNK=N (既定 0 = off。pump と peer の両方に同じ値を設定する):
// eu/am の peer が 1 つのバッチを適用する間、メモリの書き込みロック (m.mu) を最大でおよそ N 件ごとに手放す。
// GP の終わりに 1000 件超の投稿が 1 バッチで届くと、適用の間 (apply99 461ms) すべての読みが m.mu.RLock で止まっていた。
//
// 刻み方 (tx の境界でだけ刻む):
//   - pump は、このフラグが on のときだけ、変更に書いた tx の xid を付ける (memChange.Xid)。
//     1 つの tx の変更は必ず同じバッチに入る (poll は「前のスナップショットで不可視、今のスナップショットで可視」の
//     xid の変更をすべて取るので、tx は丸ごと 1 回の poll に入る)。
//   - バッチの中は seq 順だが、並行する tx の行は交互に並ぶ。そこで xid ごとにまとめ、各 tx の最後の seq の順に並べて、
//     1 つの tx の中では seq 順に適用する。ロックは tx の途中では手放さない (N を超えても tx の終わりまで続ける)。
//   - 並べ替えても、最終状態と、途中の状態の因果は崩れない:
//     同じ行を 2 つの tx が書くなら、後の tx の書き込みは前の tx の COMMIT を (行ロックで) 待つので、
//     後の tx の seq はすべて前の tx の seq より大きく、最後の seq の順でも後になる。外部キーの参照も同じ (親の COMMIT を待つ)。
//     「A の応答を見てから B を書いた」なら B の seq はすべて A の最後の seq より大きい → 途中の状態で B だけ見えて A が見えないことは無い。
//   - wm と r.seq は、バッチを最後まで適用してから進める (r.mu はバッチの間ずっと握る)。
//     wm を待つ読みは、到着時刻までに応答した書き込みが全部入った状態だけを見る。wm を既に超えている読みが途中の状態を見ても、
//     見えるのは「tx を丸ごと、因果の順に」何本か余分に、だけ (1 つの tx の一部だけが見えることは無い)。
//   - xid の無い変更が混じる (pump が off / 旧版) バッチは、従来どおり 1 回のロックで丸ごと適用する。
var memApplyChunk = envInt("ISUREAL_MEM_APPLY_CHUNK", 0)

// memTxGroups: 変更を tx ごとにまとめ、各 tx の最後の seq の順に並べる (tx の中は seq 順のまま)。
// xid の無い変更があれば nil (刻まない)。
func memTxGroups(changes []memChange) [][]memChange {
	idx := map[string]int{}
	var groups [][]memChange
	for _, ch := range changes {
		if ch.Xid == "" {
			return nil
		}
		i, ok := idx[ch.Xid]
		if !ok {
			i = len(groups)
			idx[ch.Xid] = i
			groups = append(groups, nil)
		}
		groups[i] = append(groups[i], ch)
	}
	for _, g := range groups {
		slices.SortStableFunc(g, func(a, b memChange) int { return cmpInt64(a.Seq, b.Seq) })
	}
	slices.SortStableFunc(groups, func(a, b []memChange) int { return cmpInt64(a[len(a)-1].Seq, b[len(b)-1].Seq) })
	return groups
}

func cmpInt64(a, b int64) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	}
	return 0
}

// applyBatchChunked: n <= 0 か、tx の境界が分からなければ applyBatch と同じ。
// それ以外は tx を丸ごと単位に、およそ n 件ごとに m.mu を手放しながら適用する。
func (m *memStore) applyBatchChunked(changes []memChange, n int) error {
	if n <= 0 || len(changes) <= n {
		return m.applyBatch(changes)
	}
	groups := memTxGroups(changes)
	if groups == nil {
		return m.applyBatch(changes)
	}
	for len(groups) > 0 {
		k, cnt := 0, 0
		for k < len(groups) && (k == 0 || cnt+len(groups[k]) <= n) {
			cnt += len(groups[k])
			k++
		}
		if err := m.applyGroups(groups[:k]); err != nil {
			return err
		}
		groups = groups[k:]
	}
	return nil
}

func (m *memStore) applyGroups(groups [][]memChange) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, g := range groups {
		for _, ch := range g {
			if err := m.d.applyChange(ch); err != nil {
				return err
			}
		}
	}
	if m.d.sortedDirty {
		m.d.sortedUsers()
	}
	return nil
}
