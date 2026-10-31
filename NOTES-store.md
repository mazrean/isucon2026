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
- store.write(fn): 適用役 1 本が、たまった書き込み (最大 512) をまとめて 1 回の排他 (store.mu → pump.cycleMu → memData.mu) の中で順に実行する
  (書き込みごとに memData.mu を排他で取ると重い読みと交互に待ち合う: job13083 で読み p90 7 倍・書き込み p50 2.5s の原因と推定)。
  login のセッション無効化はユーザー別索引、register は並べ替えの差し込み。各 fn はメモリを読んで検証し、
  memChange (トリガーの to_jsonb と同じ {tbl, op, row}) を emit (その場で applyChange) と PG 文を積む。
  → peers.publish(changes) → 書き戻しキューへ。画像の複製 (replicateImage) はロック外・バリア待ちの前に開始。
- 応答前の待ち: PG 書き戻し ≥ seq (ISUREAL_STORE_AWAIT_PG=1, 既定) ∧ 全 peer ack。両者は並行に進むので
  待ち時間は max(PG 書き戻し 数 ms, peer ack)。store が処理したリクエストでは memWriteBarrier の pump バリアは呼ばない。
- PG 書き戻し: レーン 0 (画像以外の全表、1 本で順序維持) + 画像レーン N 本 (ISUREAL_STORE_IMG_LANES, 既定 4。image_objects を画像 ID でシャード、各 1 本)。
  各レーンは溜まった文を 1 トランザクション・1 pgx batch で送る。書き込みは自分が積んだレーンだけ待つ
  (job10322 の 4 日目崩れ = 361KB 画像のバッチに友達申請・login が同じ列で待たされていた、という見立てへの対策)。
  `SET LOCAL session_replication_role = replica` でトリガー (mem_changelog) と FK を止める → pump が二重適用しない。
  接続系の失敗は成功するまで再試行 (その間 書き込み応答は止まる = PG なしでは応答しない、元実装と同じ)。
  文そのものの失敗 (PgError) のときだけ 1 文ずつ実行し、失敗文はログを出して捨てる (`store: write-behind op dropped` を監視)。
  文は再送しても結果が変わらないよう対象を ID で指定 (セッション無効化・RealMoji 置換・投稿期間の終了)。
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
- このブランチは main (ef34e90) をマージ済み (deploy.sh / initialize.go / mem_pump.go / server.go の衝突を解消)。
- 有効化: `STORE=1 scripts/deploy.sh` (asia1 に `ISUREAL_STORE=1`、他ホストは 0。ISUREAL_MEM=1 + pump 役のときだけ効く)。
  `ISUREAL_STORE_AWAIT_PG` は未設定のまま。
- ロールバック: `STORE` なしで deploy し直す (= asia1 の ISUREAL_STORE=0 で再起動)。PG は常に最新なので状態移行は不要。

## 検証 (ローカル PG 15433、他エージェントと DB を共有しないよう専用 DB isureal_store / isureal_orig を作成)
`ISUREAL_DB_NAME=isureal_store ISUREAL_DB_HOST=127.0.0.1 ISUREAL_DB_PORT=15433 ISUREAL_LOG_LEVEL=error go test -tags memdiff -run <Test> -v .`
- TestStoreDiff: 書き込みは store、GET を DB 経路 (=書き戻し結果) / asia1 メモリ / peer で比較。1500 ステップ、mismatches=0。
  store 処理/フォールバック: transitionFriendRequest 1627/273, createComment 155/23, 他はほぼ全件 store。
- TestStoreWriteParity: 元実装 (別 DB isureal_orig) と同じ書き込み列で各書き込みのステータス・エラー本文を比較。
  1500 ステップ × seed 3/11、期間ごとに大半のユーザーが投稿 (許可側も比較)、成功 コメント 76/107・RealMoji 69/74・投稿 117/116、mismatches=0。
- TestStoreCrash: 子プロセスで起動 → 書き込み → 書き込み中に kill -9 → 再起動 ×2。応答済みコメント 1239 件・投稿すべて残存、
  再起動前後のスナップショット差分 0、initialize で初期状態に戻る。
- TestStoreReloadRace: 並行書き込み中に invalidate (reload) を 10 回 → メモリと PG の差分 0。
- 未検証: LOCALIMG/PREVALIDATE で peer から転送された書き込み (externalImage=true) の store 経路は main のロジックを写しただけで、
  store のテストでは通っていない (ハーネスに転送リクエストが無い)。
- deleted_at / starts_at などの時刻の出どころが asia2 の clock_timestamp() から asia1 の時計に変わった (NTP 誤差 ms)。
- TestStoreBench + 読み 64 本 (STORE_BENCH_READERS=64): バッチ化前 909〜1387 rps / p50 31〜69ms → 後 2935〜4142 rps / p50 10〜17ms。
- TestStoreBench (main マージ前に計測。エンジンは同じ。ローカル RTT≒0 なので差は過小評価、peer なし): 64 並列の混合書き込み
  STORE=0 p50 17.7ms / p99 74〜82ms / 2.9k rps → STORE=1 p50 8.5ms / p99 15ms / 7.4k rps。

## テーブル別レーン (088e7fd, ISUREAL_STORE_TABLE_LANES 既定 1)
- job15806 (a449f26) vs 15544: eu/am の upstream 時間は d1-d2 で同等以下、d3-d4 で転送書き込みの p90 だけ 2 倍 (friend-requests 1.04s vs 0.50s, login 0.88 vs 0.35, posts 0.77 vs 0.58)。p50 は同じ。
- 要求数は d1 から少ない (eu 5 秒ごとの数: 10-15s で 128 vs 182、40-50s で 1751 vs 4072)。逆に asia は多い (5521 vs 2980)。asia の負荷が高い区間ほど eu/am が細る形。
- 原因の推定: 書き戻しが 1 本の直列 tx で、転送された書き込みも asia の login/セッションの洪水の後ろで PG の反映を待っていた (PG 側も 1 コア)。FWD_PRIORITY の優先がストアの中で消える。
- 対策: 画像以外の文をテーブルごとのレーンに分ける。文はすべて単一テーブル・副問い合わせ無しで FK/トリガーも無効なので、効くのはテーブル内の順序だけ。
- 注意: テーブルをまたぐ原子性は無くなる。kill -9 がレーンの flush の間に入ると、応答していない書き込みの一部だけが PG に残りうる (例: friendship は入ったが request はまだ pending)。2xx を返したものは全レーンの flush 後なので失われない。
- 通知 (long-poll) は posting_windows の DB ポーリングで起きる。eu の分布は両回で同じ (p50 8.85 vs 8.84) なので、原因から外した。
