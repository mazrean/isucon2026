# Rust (isureal-rs) メモ

## 方針 (12:00 判断)
- 8,300 行 + mem pump/peer の全面移植は 15:30 までにバイト一致で間に合わない → **ハイブリッド**。
- Rust が受けるのは DB 経路で自己完結する読み取り 3 本だけ: `GET /api/memories`、`GET /api/memories/:date`、`GET /api/me/export` (asia1 CPU の Memories 19% + export 15%)。
- nginx の location でこの 3 本だけを 127.0.0.1:8081 に振る。Go は無改変 (:8080)。Rust が判断に迷う入力 (他メソッド、本文付き GET、変わったクエリ符号化、厳密 RFC3339 でない beforePostedAt、符号付きの年など) は Go にそのままプロキシする → エラー文言まで Go と同じ。
- mem (pump/peer)・書き込み・画像 GET・SSE・initialize は Go のまま。15:00 の「mem + 全 API」マイルストーンは設計判断で未着手。

## 実装 (webapp/rust/isureal-rs)
- hyper 1 + tokio-postgres (deadpool, 最大 12 接続) / x86_64-unknown-linux-musl 静的バイナリ (`cargo +nightly`)。
- ミドルウェア相当: IP BAN (200ms ごとに ip_bans を再読込) → セッション (DB) → Region (転送ヘッダ + secret + 送信元 IP) → Bind → ハンドラ。
- JSON は Go encoding/json と同じ形 (HTML エスケープ、 、float の書式、RFC3339Nano を Local で)。
- 署名: HMAC のパッド状態を事前計算して、1 ラウンドを圧縮 2 回にする。キャッシュミスはまとめて **AVX2 8 レーン**で計算 (SHA-NI 不要)。
- export: zip ライターは Go の archive/zip と同じ形式 (data descriptor、v20)。posts.json の deflate は **goflate** (Go 1.27.3 の compress/flate level 5 を移植したもの。Go と 1488 件比較して不一致 0)。zip はユーザーごとにキャッシュする (指紋 = 作者行 + 投稿行 + 署名 URL)。画像は メモリ → Go のディスクキャッシュ (読むだけ) → DB の順に取る。

## 検証 (ローカル。PG は 15434 の専用クローン: difftest/pg-own.sh)
- `difftest/drive.py`: 書き込みは Go にだけ送る (投稿、コメント、RealMoji、友達、削除、プロフィール変更、プロフィール画像)。読み取りの GET は Go と Rust の両方に投げて、status・本文・ヘッダを比較する。
  - seed 1/200 steps: 605 件、seed 7/400: 1089 件、seed 11/600: **1573 件、不一致 0** (zip もバイト一致)。
- `difftest/pb-compare.sh`: pseudobench check (66 項目) を Go 直結と Rust 前段の両方に流して、PASS/FAIL の一覧が同じになることを確認 (ローカルは asia 1 台なので、地域が絡む項目は両方とも FAIL する)。
- 性能 (この PC。署名キャッシュが温まった状態): CPU/req は Go 498–527us、Rust 176–182us (約 2.8 倍)。
  HMAC 単体: Go 103us (SHA-NI あり) / 250us (SHA-NI なし = asia1 相当)、Rust x8 59us/署名 (約 4.2 倍)。

## 引き渡し
- `scripts/deploy-rs.sh build` → `on` (asia1。バイナリと unit を入れて起動 → 健全性確認 → yuma で Go と Rust の差分スモーク → nginx の location を追加して reload) → 公式ベンチで A/B → 戻すときは `off`。
- `scripts/deploy.sh` を流すと nginx 設定が書き戻されるので、自動的に Go に戻る (安全側)。採用する場合は deploy の後に毎回 `on` を実行すること。
- **asia1 だけで使う**こと。eu/am では、Rust のセッション確認が DB 往復 (100ms) になり、Go のメモリ上のセッションに負ける。
- メモリ: 画像 64MB + zip キャッシュ 128MB + α (unit の Environment で調整可)。

## 既知の差異
- フレーミング: Go は chunked で、Rust は Content-Length を付ける (nginx が吸収する)。5xx の本文 (エラー文言) は Rust 独自。
- memories/export の URL を Rust が署名するので、その画像の初回 `/api/images` 検証では Go の署名キャッシュがミスする。ただし計算回数は、以前 Go が memories で署名していた回数と同じ (10 分バケットごとに 1 回)。
- goflate は GOAMD64=v1 (deploy.sh の既定。確認済み) と go1.27.3 が前提。
