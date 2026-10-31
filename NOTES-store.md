# ISUREAL_STORE=1: asia1 のメモリを正本にする書き込み経路

## 何をしたか
書き込み 1 件 = 「メモリで検証・変更 (µs)」→「peer へ直接 push」→「PG へは束ねて書き戻し (並行)」。
元の経路 (PG トランザクション数往復 + FOR UPDATE/advisory lock 待ち → トリガー → 200ms tick/バリアの poll → push) から、
ハンドラ内の PG 往復・行ロック待ち・changelog トリガー・poll 量子化を外した。
**バリアの下限 (peer push/ack の直列化 0.1〜0.2s) は変えていない** (push のパイプライン化は別担当)。

## 構成 (asia1 = pump だけ。peer はコード・設定とも無変更)
- コード: webapp/go/store_engine.go (エンジン)、store_handlers.go / store_handlers2.go (各書き込み)。
  既存ハンドラには `if h.Store != nil { if ok, err := h.Store.xxx(...); ok { return err } }` を差し込んだだけ。
  mem_pump.go は 2 箇所 (memWriteBarrier の skip、reload 前の preReload フック)。
- 正本 = asia1 の memData (読みモデル)。ホット集合 (開いている投稿期間の最小 local_date 以降の投稿/コメント/RealMoji、
  全ユーザー、有効セッション、関係、保留申請、全投稿期間) はメモリが完全なので検証もメモリで行う。
- store.write(fn): 排他 (store.mu → pump.cycleMu → memData.mu) の中で fn がメモリを読んで検証し、
  memChange (トリガーの to_jsonb と同じ {tbl, op, row}) を emit (その場で applyChange) と PG 文を積む。
  → peers.publish(changes) → 書き戻しキューへ。画像の複製 (replicateImage) はロック外・バリア待ちの前に開始。
- 応答前の待ち: PG 書き戻し ≥ seq (ISUREAL_STORE_AWAIT_PG=1, 既定) ∧ 全 peer ack。両者は並行に進むので
  待ち時間は max(PG 書き戻し 数 ms, peer ack)。store が処理したリクエストでは memWriteBarrier の pump バリアは呼ばない。
- PG 書き戻し: 単一 goroutine、溜まった文を 1 トランザクション・1 pgx batch (1 往復) で送る。
  `SET LOCAL session_replication_role = replica` でトリガー (mem_changelog) と FK を止める → pump が二重適用しない。
  失敗時は 3 回再試行、その後 1 文ずつ実行して失敗文はログを出して捨てる (メモリとずれるのでログ監視: `store: write-behind`).
- 元実装の制約はすべて store の排他の中で再現: ユーザー名一意、ログイン時の既存セッション無効化、投稿 (開いている期間・
  同一 local_date の有効 0/削除 <2・starts_at 以降)、削除 2 回まで、realmoji の (post, reactor) 有効 1 件、
  コメント/RealMoji の可視性、友達申請 (ブロック・既存友達・逆向き保留=承認・ペア一意)、フォロー (認定のみ)、
  ブロック (友達/フォロー削除・保留申請 cancel)、投稿期間 (region の local_date 以降と競合、開いている期間を閉じて開く)。
- メモリで判定できないもの (終了済み期間の投稿へのコメント、ロード前に確定済みの申請の遷移、未知 ID の削除 等) は
  PG 書き戻しを出し切ってから元のハンドラ (DB 経路) にフォールバック。フォールバックの書き込みは従来どおり
  トリガー → pump でメモリと peer に届く。
- pump の reload (initialize / poll 失敗 / 部分書き込み時) の前には書き戻しを出し切る (preReload)。
- 時刻は Go で 1 回だけ作る (time.Now().Truncate(µs))。メモリ・PG・応答で同じ値。

## 永続化 / 再起動
- AWAIT_PG=1 (既定): 2xx を返した書き込みは必ず PG (asia2, synchronous_commit=on) に入っている。
  再起動 = 従来の pump と同じく PG からロード。kill -9 でも応答済みの書き込みは失われない (TestStoreCrash)。
- **ローカル WAL は実装していない。** AWAIT_PG=1 では応答済みの書き込みは既に PG にあるので WAL は何も足さない。
  AWAIT_PG=0 (PG を待たない) は、Memories / export / 画像の有効性確認 / 通知 hub / ホット外の読み / peer の DB フォールバックが
  PG を読むため read-your-writes が崩れる → それらをメモリに移すまで使わないこと (そのときに WAL が必要になる)。

## ベンチ中の asia2 PG の用途
書き戻し (バッチ)、ホット外のフォールバック、Memories / export、画像 miss と有効性確認、通知 hub のポーリング、
pump の 200ms poll (フォールバック書き込み分のみ)、initialize。

## env / ロールバック
- 有効化: asia1 の isureal-go に `ISUREAL_STORE=1` (ISUREAL_MEM=1 + pump 役のときだけ効く。peer は無視)。
  `ISUREAL_STORE_AWAIT_PG` は未設定のまま。
- ロールバック: `ISUREAL_STORE` を外して asia1 を再起動。PG は常に最新なので状態移行は不要。

## 検証 (ローカル PG 15433、他エージェントと DB を共有しないよう専用 DB isureal_store / isureal_orig を作成)
`ISUREAL_DB_NAME=isureal_store ISUREAL_DB_HOST=127.0.0.1 ISUREAL_DB_PORT=15433 ISUREAL_LOG_LEVEL=error go test -tags memdiff -run <Test> -v .`
- TestStoreDiff: 書き込みは store、GET を DB 経路 (=書き戻し結果) / asia1 メモリ / peer で比較。1500 ステップ、mismatches=0。
  store 処理/フォールバック: transitionFriendRequest 1627/273, createComment 155/23, 他はほぼ全件 store。
- TestStoreWriteParity: 元実装 (別 DB isureal_orig) と同じ書き込み列で各書き込みのステータス・エラー本文を比較。1500 ステップ、mismatches=0。
- TestStoreCrash: 子プロセスで起動 → 書き込み → 書き込み中に kill -9 → 再起動 ×2。応答済みコメント 1239 件・投稿すべて残存、
  再起動前後のスナップショット差分 0、initialize で初期状態に戻る。
- TestStoreReloadRace: 並行書き込み中に invalidate (reload) を 10 回 → メモリと PG の差分 0。
- TestStoreBench (ローカル、RTT≒0 なので差は過小評価、peer なし): 64 並列の混合書き込み
  STORE=0 p50 17.7ms / p99 74〜82ms / 2.9k rps → STORE=1 p50 8.5ms / p99 15ms / 7.4k rps。
