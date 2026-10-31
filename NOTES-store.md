# ISUREAL_STORE=1: asia1 のメモリを正本にする書き込み経路 (設計)

## 目的
書き込み 1 件 = 「メモリで検証・変更 (µs)」→「peer へ直接 push」→「PG へは束ねて書き戻し」。
今の経路 (PG トランザクション数往復 + 行ロック → トリガー → 200ms tick/バリアの poll → push) から、
PG の往復・ロック待ち・poll 量子化を外す。

## 構成 (asia1 だけ。peer は無変更)
- 正本 = asia1 の memData (mem_store.go の読みモデル)。ホット集合 (開いている投稿期間の最小 local_date 以降の
  投稿/コメント/RealMoji、全ユーザー、有効セッション、関係、保留申請、全投稿期間) はメモリが完全なので、
  検証もメモリで行う。
- 書き込み = store.write(fn): グローバル排他 (store.mu + pump.cycleMu + memData.mu) の中で fn がメモリを読んで検証し、
  memChange (トリガーが作っていたのと同じ {tbl, op, row}) と PG 文を積む。各 memChange はその場で applyChange
  するので fn 内で後続の検証に反映される。
- コミット: seq を振る → peers.publish(changes) (peer は今と同じ applyChange) → WAL に追記 → PG 書き戻しキューへ。
- 応答前の待ち (バリア): WAL fsync ≥ seq (ISUREAL_STORE_WAL 指定時) ∧ PG 書き戻し ≥ seq (ISUREAL_STORE_AWAIT_PG=1, 既定)
  ∧ 全 peer ack。PG 書き戻し (asia2 まで 1ms、pgx batch で 1 往復) は peer の ack (100ms) と並行なので隠れる。
  memWriteBarrier は store が処理したリクエストでは pump バリアを呼ばない (c.Set("storeHandled")).
- PG 書き戻し: 単一 goroutine。溜まった文を 1 トランザクション・1 pgx batch で送る。
  `SET LOCAL session_replication_role = replica` でトリガー (mem_changelog) と FK を止める
  → pump が自分の書き込みを二重適用しない。失敗時は 1 文ずつ再試行してスキップ (ログ)。
- 元実装の制約の再現 (すべて store の排他の中):
  - ユーザー名一意 (register) / ログイン時の既存セッション無効化
  - 投稿: 開いている期間があること、同一 local_date の有効投稿 0・削除 2 未満、starts_at 以降
  - realmoji の (post, reactor) で有効 1 件 (置き換え)、コメント/RealMoji の可視性チェック
  - 友達申請: ブロック・既存友達・逆向き保留 (=承認) の判定、友達ペア一意
  - 投稿期間: (region, local_date 以降) の競合、開いている期間を閉じてから開く
- ホット集合の外 (終了済み期間の投稿へのコメント、確定済み申請の遷移 等) は store で判定できないので、
  PG 書き戻しを現在の seq まで待ってから元のハンドラ (DB 経路) にフォールバックする。
  フォールバックの書き込みはトリガー → pump で従来どおりメモリと peer に届く。
- 時刻は Go で 1 回だけ作る (time.Now().Truncate(µs))。メモリ・WAL・PG・応答で同じ値。

## 永続化 / 再起動
- AWAIT_PG=1 (既定): 応答した書き込みは PG に入っている。再起動 = 今の pump と同じく PG からロード。
- WAL (ISUREAL_STORE_WAL=/home/isucon/store-wal): 1 レコード = 長さ + crc32 + JSON(seq, PG 文と引数)。
  グループコミット fsync (書き込みが来たら最大 2ms 溜めて fsync)。PG の store_flush(seq) を書き戻しと同じトランザクションで更新し、
  起動時は WAL のうち seq > store_flush を PG に再実行 (文は冪等: INSERT .. ON CONFLICT DO NOTHING / 絶対値 UPDATE)
  してから通常ロード。initialize で WAL を新しい epoch のファイルに切り替える。
- AWAIT_PG=0 は WAL 必須。ただし Memories/export/通知/画像/ホット外は PG を読むので、
  それらを store に移すまでは RYW が崩れる → 現時点では使わない。

## ベンチ中の asia2 PG の用途
- AWAIT_PG=1: 書き戻し (バッチ)、ホット外のフォールバック、Memories / export / 画像の miss / 通知 (hub) の読み、initialize。
- 「initialize の読み込みのみ」にするには Memories/export/画像/通知/ホット外の読みを store に移す必要がある (次段)。

## ロールバック
ISUREAL_STORE を外して再起動 (asia1 のみ)。PG は常に最新なので状態の移行は不要。

## 検証
- `go test -tags memdiff -run TestStoreDiff` (ローカル PG 15433): 書き込みは store (sb)、読みは DB 経路 (sa, 同じ PG) /
  store の asia1 メモリ (sb) / peer (sc) を比較 → PG 書き戻しとメモリと peer が一致すること。
- 再起動: 書き込み列の後に store を捨てて PG から作り直し、同一応答になること。
