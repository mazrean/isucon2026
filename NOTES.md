# ISUCON2026 本番メモ

END: 2026-10-31 18:00 JST

## 方針 (ユーザー指示)
- レギュレーション範囲内なら何でも可 (オンメモリ化・言語/Nginx/カーネル改造 OK)。再起動後もデータ保持必須。
- 並列: 目安 5、最大 20。待機中も作業継続。中盤以降は大胆な改善。
- Git に状態を保存 (戻せるように)。
- 競技後ブラウザ確認あり → 初期挙動を基に playwright E2E を構築。最後に差分確認。
- ベンチ: `playwright-cli open --profile=$HOME/.cache/playwright-isucon-portal` のログイン済みポータル経由でコマンド化。
- 疑似ベンチはローカルから。対象ポートのみローカル IP 許可可。**競技終了前に必ず戻す。**
- グレーゾーン/判断はデフォルト (有効スコア最大化) を選び、ここに記録。

## ポータル
- https://portal.isucon.net (isuxportal, cookie: isuxportal_session)
- チーム: INUCON2026 (team_id=90031)

## ホスト / 役割
(構築後に記入)

## 一時的な FW ルール (終了前に戻す)
(なし)

## スコアログ
| 時刻 | commit | score | pass | 内容 |
|---|---|---|---|---|
| 10:10 | 初期 | 3023 | true | 初期実装 (posting-window タイムアウト×3, 4日目欠落) |
| 10:25 | 8cd4fff | 3407 | true | 画像署名キャッシュ |
| 10:36 | e33b814 | 7633 | true | 書き込みを asia1 へ転送 |
| 10:44 | 9315bb9 | 16875 | true | 読み書き分離基盤+posting-window修正+SSE集約+画像キャッシュ+ログ抑制 |
| 10:50 | 6f6685e | 0 | **false** | nginx 調整。デプロイ直後の asia1 再起動 (停止 10s) 中に initialize → 502。deploy で起動待ち + TimeoutStopSec=3 に修正 |
| 10:52 | 6032c39 | 16175 | true | レプリカ再生停止で回復 |
| 10:58 | 7b0c307 | 0 | **false** | GET 転送 + PG 1GB。driver の zsh で lock 取得失敗 → E2E の投稿がベンチ中に混入し「存在しない投稿」で整合性違反。アプリ起因ではない |
| 11:03 | 8c9e0e0 | 17423 | true | GET も asia1 へ転送 + PG shared_buffers 1GB。eu/am 人数増、asia 18 で停滞 |
| 11:10 | 2f19b70 | 17351 | true | pprof 有効 (再現確認)。CPU の 6 割が画像署名 HMAC |
| 11:16 | 2f19b70 | 24549 | true | HMAC Reset 使い回し 4x。eu 43 人 / asia 32 人まで伸びる |
| 11:25 | 8d41ed4 | 20946/19645 | true | 統合版。人数 2.6 倍/日 だが asia 127 で横ばい |
| 11:58 | a41e963 | **39703** | true | 分散インメモリ MEM=1 + GET ローカル + jpegcheck。asia 317 人。グランプリ全滅 (2/1/0 人) |

## バックログ (優先順)
1. [進行中] 各アプリホストにローカル PG ホットスタンバイ (asia2 から streaming, remote_apply ANY 3) → 読みをローカル化 (europe/america の 100ms/クエリ を除去)
2. [進行中] アプリ: 読み書き分離 / posting-window タイムアウト修正 / SSE を DB ポーリングから脱却 / 書き込み往復削減 / initialize 申告値
3. 書き込みの asia1 転送: europe1/america1 の非 GET /api/* を asia1 アプリへ (ルーター経由のプライベート IP) 転送し、SQL 往復を 1ms 化。RegionMiddleware はオリジン地域を内部ヘッダーで引き継ぐ (192.168.x からのみ信頼)
4. 画像: ローカル replica から読む + ディスクキャッシュ / nginx 配信 (署名検証・削除 5 秒ルールは維持)
5. guard 依存: ログイン失敗ログ形式維持 or アプリ内 BAN 実装 (10 回/10 秒 → 2 分)
6. 監視: スロット wal_status 監視 + 自動 sync 縮退 watchdog、追試のランダム順再起動で書き込みが止まらないこと
7. 最終: 全台ランダム順再起動 → 公式ベンチ、E2E、envcheck 全台、SG 差分なし確認

## ポート登録

## 課題 (紹介動画より, 開始前)
- BeReal ライク: 特定時刻に通知 → 椅子の写真を撮って投稿する SNS。
- 世界同時に写真投稿イベントがある → 一斉投稿のスパイク (thundering herd) 想定。
- 仮説: 画像アップロード/配信 (nginx 静的配信・ディスク直書き)、タイムライン/フィードの N+1、通知の待ち (long-poll/SSE?)、投稿締切時刻判定の正しさ。

## 環境 (CloudFormation stack: isucon2026, ap-northeast-1)
- 構成: Router(c6in.large, 192.168.11.10 public / 192.168.10.10 data, ポータル管理の haproxy/isureal-router, :443/:8080 がベンチ入口)
  - App1 192.168.1.10 c5.large  ISUREAL_REGION=asia
  - App2 192.168.2.10 c7i.large ISUREAL_REGION=europe
  - App3 192.168.3.10 c7i.large ISUREAL_REGION=america
  - DB   192.168.4.10 c5.large  (gp3 6000IOPS/200MB/s)
- ホスト間通信は全てルーター経由 (ルートテーブルで data ENI を next-hop) → リージョン間レイテンシ模擬の可能性大。DB 往復が高コスト想定。
- 各 app に isureal-go, nginx, isureal-guard (guard の役割要確認: 改変検知?)。
- ドメイン: app.team90031-aws.127.0.0.1.nip.io、自己署名証明書 (ルーター側 haproxy が TLS 終端)

## マニュアル要点 (原文: ~/programs/isucon/isucon2026-docs/docs.txt, rules.txt)
- サーバ名: asia1=192.168.1.10, europe1=192.168.2.10, america1=192.168.3.10 (各 nginx+Go), asia2=192.168.4.10 (共有 PostgreSQL 16)。ルーターはログイン・設定変更禁止。
- ルーターは X-Country-Code で転送 (JP/KR/CN/AU→asia1, GB/DE/FR→europe1, US/CA/MX→america1)。リージョン間は遅延+帯域制限。asia1-asia2 は同一リージョン。
- **禁止**: ルーター迂回のサーバ間通信 / リージョン・遅延・帯域の変更回避 / ルーターログイン。SG で 22,443,8080 は変更禁止。
- X-Client-IP をクライアント IP として使う。
- 書き込み後の読み取りはリージョンをまたいでも反映必須 (read-your-writes, 強整合寄り)。削除投稿の画像は 5 秒後には 403。
- 初期化: 全リージョン整合した初期状態へ。レスポンス {"language":"go","agent":...,"harness":...,"model":...} → agent="claude", harness="claude-code", model="claude-opus-5-5" に設定する。
- スコア = Σ 採点件数×配点×リージョン倍率 (asia 1, europe 1.5, america 1.5)。高配点: エクスポート20, 最後の投稿期間フィード検証20, 通常10, RealMoji 5, 退会5, コメント3, プロフ画像3, Memories月3。
- 投稿後友達フィード検証は「投稿期間が終了する前に」成功が条件 → 投稿〜フィード反映のレイテンシが鍵。
- **最終スコアは最後に行った負荷走行**。FAIL なら受賞なし。追試で 75% 以下×3 なら失格。
- 終了時は 4 台 + ルーター起動状態。envcheck: `sudo /usr/local/bin/envcheck`。
- 変更禁止: isucon-register.service 関連, isuadmin, envcheck 関連ファイル (/usr/local/bin/envcheck, isucon-contest-envcheck, envcheck*.service, /etc/isucon/envcheck.json, /var/lib/isucon-envcheck/)。
- クリティカルエラー(即 FAIL): 初期ユーザーログイン 401 / 閲覧権限のない投稿がフィードに出る・取得できる / 権限なし RealMoji・コメント受理 / Memories・エクスポートが期待値不一致 / 削除 5 秒後も画像取得可。
- 動作確認ユーザー: yuma_kobayashi_186 / SecretKey 0f6-BAnyBELD2MA-t-fekeDtQN4ynqD0mfgOF8JWwWs (asia)。admin / password1234。
- 質問 #13 (リージョン間委譲の可否, ユーザー未承認で送信してしまった) → 回答「はい」(個別回答, 10:22)。転送施策 (バックログ3) は許可済み。
| 10:26 | 8cd4fff | 3407 | true | 画像署名キャッシュ |

## 運用方針 (ユーザー指示 10:45)
- 環境ロックが詰まるので、スモークテストや実機での個別確認はしない。ローカルで確認できるものはローカルで (build/vet/test)。確認しにくいものは main に取り込み、公式ベンチでまとめて検証する。ベンチは積極的に回してよい (キューは空いている)。

## インフラ
- scripts/host-tune.sh (冪等, env.lock 取得) で 4 台に適用済み: 不要デーモン停止 (unattended-upgrades, ModemManager, multipathd, udisks2, apt/fwupd/motd/man-db 等 timer)、sysctl `/etc/sysctl.d/99-zz-isucon-tune.conf` (somaxconn 65535, tw_reuse, slow_start_after_idle=0, tcp_rmem 既定 262144)、`isucon-route-tune.service` (192.168.0.0/16 を initcwnd 32 / initrwnd 64)、isureal-go drop-in (LimitNOFILE, GOGC=200, GOMEMLIMIT=1200MiB)、nginx drop-in (LimitNOFILE=65536)。
- 計測 (europe1→asia1, ルーター経由): RTT 100.8ms、新規接続 HTTP 0.203s / keepalive 再利用 0.101s。80–100KB 応答は初期受信窓 64KB で +1RTT (0.304s) → rmem 既定 + initrwnd で 0.205s。30MB で再送 0、帯域上限は観測されず → BBR 不採用。
- ルーター↔アプリは 0.2–0.5ms (遅延はホスト間のみ)。ip_local_port_range は公式 90-isucon.conf のまま。

## PG レプリカ
- 構成: asia2 (192.168.4.10) がプライマリ、asia1/europe1/america1 がローカル PG16 ホットスタンバイ (物理スロット = ホスト名、application_name = ホスト名)。ロール `replicator` (REPLICATION)。
- 再現: `scripts/pg-replica-setup.sh primary|standby [hosts]|walserve start|stop|restart-primary|status`。設定は `etc/pg/` (primary-repl.conf / primary-tuning.conf / primary-hba.conf / standby.conf / isucon_sync_members.sql / pg-sync-watchdog.service)。env.lock は適用の瞬間だけ取る。
- 初期化: 4 台とも同一イメージ (system id 7702429198198691336, checkpoint 4/57A3CDC8) なので base backup なしで standby.signal + restore_command (asia2 walsave の一時 HTTP :18081、停止済み) → streaming。
- 帯域 (asia2→各ホスト, 50MB curl): asia1 約 120MB/s、europe1/america1 約 18.5MB/s (~150Mbps)、RTT 約 100ms。
- 追従が遅い原因: スタンバイのディスク IOPS (gp3 3000) 上限。初期 WAL は全表 VACUUM (Heap2/VISIBLE) で全ページ読み → 1 セグメント ~1 分。ベンチ中は画像 INSERT で WAL ~100MB/分出るので、ベンチ直後は lag 1.5GB 程度になりうる。スタンバイの replay がアプリホストの IOPS を食う点に注意。
- スタンバイ設定: shared_buffers=256MB, max_connections=200 (プライマリ以上必須。下回ると recovery 停止), hot_standby_feedback=on, max_standby_streaming_delay=500ms, wal_receiver_status_interval=1s。postgresql は enable 済みで再起動後に自動追従。restore_command は残っているが HTTP 停止済みなので即失敗 → streaming に移るだけ (無害)。
- asia2 チューニング (conf.d/isucon-tuning.conf): shared_buffers=1GB / max_connections=200 は **要再起動 (未反映, pending_restart)**。公式ベンチ外で `scripts/pg-replica-setup.sh restart-primary` (lock 取得)。その他 (wal_compression=lz4, work_mem 等) は reload 済み。再起動・reboot 前にスタンバイ側 max_connections=200 が入っていること (入っている)。
- asia2 再起動: スタンバイは primary_conninfo で自動再接続 (スロットは永続)。

### 同期レプリケーション (pg-sync-watchdog)
- `scripts/pg-sync.sh on|off|status`。on/off は `/etc/postgresql/16/main/pg-sync.mode` に書くだけで、asia2 の `pg-sync-watchdog.service` (enable 済み) が `conf.d/zz-sync.conf` を書き換えて reload する。現在 **off**。
- on: 健全なスタンバイ (streaming, reply_time 3 秒以内, replay 遅れ 16MB 未満, スロット wal_status が lost/unreserved でない) だけで `synchronous_standby_names='ANY n (...)'` + `synchronous_commit=remote_apply`。不健全が 5 秒続くと除外、2 秒健全で復帰。
- 順序: 外す時は `isucon_sync_members.in_sync=false` を commit (synchronous_commit=local) → 除外 → reload。戻す時は追加 → reload → sync_state=quorum 確認 → in_sync=true。app はローカル replica で in_sync を読み false ならプライマリ読み。
- 注意: 切断されたスタンバイ自身は false を受け取れないため、ローカルの in_sync=true が古いまま残る。app 側は `pg_stat_wal_receiver.status='streaming'` も併せて確認すること。
- 危険: on の間、スタンバイが落ちると検出まで最大 ~5 秒 (+wal_sender_timeout 10s で切断が見えない場合はreply_time で検出) 全書き込みが止まる。watchdog が止まっていたら `scripts/pg-sync.sh off` (watchdog 不在時は直接 zz-sync.conf を空にして reload する)。
- 起動時: watchdog の ExecStartPre が PG 起動前に sync を空にし、起動後は全員 in_sync=false から始める → asia2 が先に上がっても書き込みはブロックされない。
- ログ: `sudo journalctl -u pg-sync-watchdog`。

## PG レプリカ中止 (10:45)
- スタンバイの再生が gp3 3000 IOPS 上限に張り付き (lag 1.6GB, ベンチ中に増加)、アプリホストの性能を巻き添えにした (16875→9225)。
- 対応: 3 台の postgresql を stop+disable、asia2 のスロット全削除、pg-sync-watchdog disable。READ_HOST は primary (192.168.4.10)。
- 代替: GET も asia1 へ転送 (画像/SSE はローカル) → インメモリ読みモデル (行データ付きイベント, replica 非依存) へ。

## インメモリ読みモデル (ISUREAL_MEM=1, branch worktree-agent-a2367fe4e56a03cd4)
- 仕組み: primary の 10 テーブル (users, login_sessions, friendships, follows, block_relations, friend_requests, posting_windows, posts, comments, realmojis) に AFTER ROW トリガー → `mem_changelog(seq, xid xid8, tbl, op, row jsonb)`。asia1 起動時に冪等 DDL を自動適用 (ISUREAL_MEM=1 のときだけ)。
- asia1 (pump) が `pg_current_snapshot()` 差分で changelog を tail (単一文で取得、欠番問題なし) しメモリ適用。非 GET /api/* はレスポンスをバッファし「ハンドラ完了後に開始したサイクル」(+分散時は全 peer の ack) を待ってから返す → RYW。
- メモリに持つもの: 全ユーザー/有効セッション/関係/保留申請/全投稿期間/ホット投稿 (開いている期間の最小 local_date 以降) とその有効コメント・RealMoji。ホット外は終了済み期間のみ → DB へフォールバック。Memories/export/SSE は DB のまま。
- ロード前・initialize 中・poll 失敗時は not-ready → 全て元の DB 経路。initialize は init.sh → TRUNCATE mem_changelog → 再ロード → peer 再構築完了待ち。
- 分散モード: asia1 に `ISUREAL_MEM_PEERS=192.168.2.10:8080,192.168.3.10:8080`、europe1/america1 に `ISUREAL_MEM=1 ISUREAL_MEM_PUMP=192.168.1.10:8080` (ISUREAL_FWD_SECRET 共有, /internal/mem/* は 192.168.x + secret のみ)。peer は DB を読まず、pump のダンプ (gzip JSON) + 連番バッチで同期。欠番は pump のバッファから pull、epoch 変化で再構築。peer は heartbeat (250ms) が lease 1s 途絶えると自発的に DB 経路 (pump の push タイムアウト 3s > lease)。
- push のパイプライン化 (12:10〜): peer ごとに未 ack の push を最大 4 本並列に送る (heartbeat は送信中でないときだけ)。publish 時に即送信するので、書き込みのバリア待ちは「自分のサイクルの poll + 1 RTT」。peer は seq 順に適用し、先行分が未着なら最大 50ms 待ってから従来どおり pump のバッファから pull (失敗なら invalidate)。
- lease 規則: push に pump の送信時刻を載せ、peer の lease は min(受信時刻, 送信時刻) + 1s。pump は peer ごとに「lease を与えうる push の最終送信時刻」+ 1s + 時計ずれ 200ms を過ぎたら ack 無しでも先に進む (ack 受信時刻ベースより厳密: 応答待ちの push で延びた lease も上界に入る)。down の peer への送信は probe (lease を延ばさない) にして、復帰を確認してから lease を与える。前提: ホスト間の時計ずれ < 200ms (chrony)。
- 撤去: 各テーブルの `DROP TRIGGER mem_changelog ON <tbl>`、`DROP TABLE mem_changelog`。フラグ off でもトリガーは残る (1 行 insert/変更行、負荷小)。
- 検証: ローカル PG (primary のダンプ, 画像除く) で `go test -tags memdiff -run TestMemDiff` (DB 経路 / pump / peer の GET 応答を 1500 ステップの書き込み列で比較、差分 0)。
## 他チーム考察 (10:55 時点のリーダーボード)
- 1位 sotozaki 80631 / 2位 Dorayaki 58943 / 3位 KatoMegumi 49407 / 4位 42027 / 5位 35953 … 25位 18936。自チーム 17423 (30位)。
- スコア ≒ Σ(地域重み × 人数 × 日) × 約 38 点。人数は「前日の出来が良いと約 1.5〜1.75 倍」で増える (調査エージェント)。
  - 例: 3 地域とも 18/7/6 から毎日 1.6 倍 → 5 日目に 120/45/40 程度 → 4〜5 万点。8 万点は「全地域で毎日増やし切った上に最終日 (20 点/人) も取り切る」水準。
- 上位チームがやっていると推定されること:
  1. 3 地域すべての人数を毎日伸ばしている = eu/am の応答も速い (100ms×多数クエリを消している)。
     書き込み転送 / 読みのキャッシュ (少なくとも user・friend graph・投稿期間のメモリ化) は必須級。
  2. posting-window 開始 (通知 INSERT 数千行) を速くして、各日の投稿期間 (約 9 秒) を食わないようにしている。
  3. asia が 1 台に集中して頭打ちになるのを避けている (自チームは GET 転送で asia が 18 人で停滞)。
- 自チームの差分: asia の伸び (以前 74 人まで伸びた) を取り戻しつつ、eu/am も伸ばす必要がある。
  → 分散インメモリ (各ホストで読みを返す) が本命。asia1 一極集中を避ける (転送先 asia2 の A/B 中)。

## ゲーム展開予測と方針 (11:05)
- 人数は前日の出来で 1.5〜1.75 倍に増える → 上位は「3 地域で毎日伸ばし切る」段階へ。次の壁は人数爆発後 (5 日目に地域あたり 150〜300 人、最終日は一斉投稿) の容量:
  1. フィード計算 (全アクティブ投稿走査 = O(人数²)) の CPU
  2. 画像取得 (eu/am→asia2 DB, 100ms+150Mbps) の帯域
  3. 投稿期間開始 (通知 INSERT 数千行) と SSE 一斉配信
  4. asia1 2vCPU への書き込み集中 (既に isureal 180% CPU を観測)
  5. グランプリの一斉投稿スパイク
- 打ち手: 分散インメモリ (フィード索引で O(友達数)) / 画像の全リージョン push 複製 / posting-window O(1) / asia1 CPU プロファイルでホットスポット除去 / (将来) asia1 書き込みのグループコミット

## リーダーボード (11:30 ポータル時刻)
- 1 Typing Bears 506241 / 2 Dorayaki 158066 / 3 pixiv 105161 / 4 sotozaki 93107 / 5 ちゃん 62337 … 自チーム 20 位 41584。
- Typing Bears だけ桁違い (2 位の 3.2 倍)。仮説: 上限なしの加点 (エクスポート 20、Memories 月 3 / 日 2 / 前回 1、過去画像 2、検索 2、友達申請 2…) を高速に回して積んでいる。閲覧系 (フィード等) には 1 窓上限がある (マニュアル「スコアの計算」)。
- 注意: スコア表の時刻のうち 11:10 以降の行は推定がずれている (実時刻はベンチログの job 番号順)。

## スコア追記 (11:20 以降、job 順)
| job | commit | score | 内容 |
|---|---|---|---|
| 3241 | 8d41ed4 | 39703 | 分散インメモリ MEM=1 + GET ローカル + jpegcheck。asia 317。GP 全滅 |
| 3469 | 同 | 38047 | 再現 + GP プローブ: asia1 CPU 張り付き、JPEG 50% |
| 3724 | 6973095 | 38515 | JPEG を asia2 へ (cap2)。GP 全滅: mem poll がプール枯渇でタイムアウト → 全リロード 4.5s |
| 3997 | 6a62af1 | 41584 | bg プール + poll 再試行。too many clients 発生 (全ホスト 80) |
| 4395 | 11cfa30 | **48422** | JPEG オフロード 3 台 + 画素バッファ無し + ホスト別プール。5 日目 362/62/41、GP 8/4/0 |
| 4513 | 1cd1f59 | 49892 | GOMEMLIMIT 2600 / GOGC 400。GP asia 51 |
| 4692 | 2b1c03c | **68277** | JPEG オフロード待ち合わせ + push バイト上限 + GOMEMLIMIT 2200。GP 322/54/44。asia1 CPU: HMAC 35% / Memories 19% / export 15% / JPEG 14% |
| 4952 | f55196e | **70862** | 4xx はバリアを飛ばす。GP 376/55/33 |
| 5168 | 6cb20bc | 69730 | A/B: 画像 push 停止 → 差なし (eu/am 減, asia 増)。push は維持 |
| 5400 | 1f0a1d1 | 0 FAIL | PGO + v3 + Memories/export を asia2 へ。asia 5 日目 676 人 / GP 441。走行後の整合性で too many clients (asia2 プール既定 80 + 画像判定 40×4) |
| 5642 | e2a74ab | 69709 | プール是正 + login バリア自ホスト + PG 400。asia2 が CPU 100% で Memories 平均 3.3s (タイムアウト多数) |
| 5893 | cf55c32 | 0 FAIL | heavy 分担 + JPEG を eu/am のみ + push off。eu/am が 23〜31 人に低下 (JPEG の転送で回線を圧迫)。整合性違反 1 件: america のユーザーの friends フィードに、5.6s 後に削除された asia の投稿 (friends_of_friends) が見えた。原因は調査中 |
| 6145 | cb15c96 | 0 FAIL | JPEG 配分を戻した。asia 5 日目 557。friends フィードで見えてはいけない投稿が 7 件 (読み手は america、削除されていない friends 投稿)。再発 → 潜在の競合を調査中 |
| 6305 | 55b8f09 | 68929 | 切り分け: heavy を asia1 ローカル + push on + barrier 修正 → pass。ただし「見えるべき投稿がありません」3 件。致命的な「見えてはいけない」は push off + heavy 分担の 2 回 (5893, 6145) だけ → 既定を 5642 の構成 (heavy は asia2 + backup、push on) に戻す |
| 6507 | 2b7a1c0 | **73358** | 5642 構成 (heavy は asia2 + backup、push on) + barrier 修正。pass だが「見えるべき投稿がありません」10 件 (dump は logs/faildump/job6507.txt) |
| 6869 | - | 69483 | synchronous_commit=off。73k とノイズの範囲。フィードの誤り 0 件 |
| 7020 | - | 68859 | PREVALIDATE=1: eu/am の 4〜5 日目 74/61〜75/62 (前回 55/40)。GP の asia は 312 (asia2 の Memories タイムアウト)。eu/am が伸びるので既定 on |
| 7184 | 8b2d632 | 0 FAIL | 統合版: initialize 504。起動時ロード 37s + initialize が peer の全量ダンプ (15〜17s × 数回) を待った → 待たないよう修正 |
| 7382 | 7c40de3 | 9478 | 統合版 (全投稿メモリ化): ベンチ中に 62.5 万件の全量ダンプが 4 回 (15〜33s)、回線と CPU を占有して人数が 1 日目から増えず → 全投稿メモリ化を revert。署名 (AVX-512) だけ残す |
| 7706 | 6e86484 | **93068** | 全投稿メモリ化を revert + AVX-512 署名 + PREVALIDATE + STORAGE EXTERNAL + sync commit off。asia 5 日目 663、eu 89、am 74、GP 466/67/50。フィードの誤り 0 |
| 7851 | - | **111842** | LOCALIMG=1 (eu/am の画像を自地域に fsync 保存、asia1 はメタデータのみ、PG へ非同期 flush)。asia 742 / eu 175 / am 95、GP 545/161/84。flush_err 0。409 post_slot_unavailable が 1 件 (要観察) |
| 8010 | - | 84958 | FRONT=1 (asia1 nginx 撤去、LOCALIMG=0)。93068 の構成より低い → 不採用 (既定 FRONT=0)。旧 lease のため「見えるべき」4 件 |
| 8178 | - | 0 FAIL | 既定 (LOCALIMG=1) の再現: 整合性違反「見えてはいけない投稿」(旧 lease の穴) → lease を送信時刻で上限する修正を投入 |
| 8353 | - | **112338** | lease を送信時刻で上限 (ドライバー版) + LOCALIMG。asia 728 / eu 206 / am 92、GP 543/175/85。フィードの誤り 0、409 0 |
| 8506 | - | 110123 | push パイプライン化 (mem-pipeline)。112k とノイズの範囲。エラー無し。バリア p50 220ms / p90 316〜459ms |
| 8713 | - | **113783** | MEM_WM=1 (watermark)。asia 5 日目 944 / eu 192 / am 111。GP は asia 199 に崩れ (asia1 飽和)、eu 171、am 103。wm 待ち p50 63〜144ms、fallback 少 → 既定 on |
| 8957 | - | 111710 | JC=1 (Rust JPEG)。asia5 823 / eu 182 / am 118。GP asia 0 人 (崩壊)、eu 174、am 116。jpeg remote_err 33 |
| 9384 | - | 113234 | IMGV_MEM (画像の有効性をメモリで)。asia5 881 / eu 141 / am 100。GP asia 200 (窓の遅れ、修正前) / eu 130 / am 94。register timeout 1 |
| 10125 | - | 113557 | 窓の修正 + JPEG 委譲 asia2 のみ + JC=1。asia4 408→5 日目 685 (伸び鈍化) / eu 189 / am 88。GP asia 450 (66%、修正の効果) / eu 176 / am 84 |
| 10322 | - | 104859 | STORE=1 + 窓の修正 + JPEG asia2 のみ + JC=1。asia4 436→5 日目 525 (1.2 倍に鈍化) / eu 144 / am 112。GP 464/135/97。write-behind の取りこぼし 0。不採用 |
| 10890 | - | **FAIL 0** | LOGREPL=1 (公式)。「再ログイン前のセッションが無効になっていません」。原因: logrepl のバリアが login を除外していた (login は古いセッションを無効化する)。修正済み。revert 確認済み (スロット 0、sync names 空、watchdog 両方 disabled) |
| 11091 | - | 104839 | FOR KEY SHARE + asia1 CPU 3 本。asia5 967 (最多) / eu 122 / am 100。GP asia 467 / eu 65 / am 55 (eu/am 崩れ)。通知 503 ×7、client cancel |
| 11272 | - | 112194 | 画像 push 停止 (IMG_PEERS 空) + KEY SHARE。asia5 953 / eu 185 / am 115。GP asia 551 (58%) / eu 32 / am 62。eu/am の窓 POST 3.2/5.2s が新しい律速。画像 500 (deadline) ×2 |
| 11493 | - | **129380** | push 停止 + KEY SHARE + FWD_PRIORITY=1。asia5 423 / eu 277 / am 302 (eu/am 激増)。GP asia 178 / eu 261 / am 261。asia ローカルの待ち p50 1.1〜1.9s (枠 32 が狭い) → 既定に FWD_PRIORITY=1・push 停止 |
| 11716 | - | **139085** | FWD_PRIORITY + ローカル枠 64。asia5 823 / eu 224 / am 331。GP 400 / 220 / 244。ローカル待ち p50 1.3〜2.0s (まだ詰まる) → 既定 64 |
| 11877 | - | **141122** | ローカル枠 128。asia5 877 / eu 274 / am 261。GP 226 / 238 / 249 → 既定 128 |
| 12063 | - | **FAIL 0** | PGSSLMODE=allow + MEMORIES_MEM=1。初期検証で PUT /api/me/profile-image が 500 (unexpected EOF、直前に同じ利用者の invalid_image 400)。11877 でも負荷中に同じ 500 が 1 件 → 既存の間欠バグとして調査中 |
| 12395 | - | **FAIL 0** | 12063 と同じ構成の再走。同じ profile-image 500 で FAIL → 再現。PGSSL=prefer + MEMORIES_MEM=0 で切り分け |
| 12488 | - | **FAIL 0** | PGSSL=prefer + MEMORIES_MEM=0 でも同じ FAIL → 原因は asia1 のディスク満杯 (imgcache 33G、nginx の body 一時ファイルが書けず 500)。全台の imgcache を消去、ディスク上限 6GB を実装 |
| 12846 | - | **146736** | ディスク上限 + 既定 + MEMORIES_MEM=1 + admin 専用プール。asia5 915 / eu 269 / am 275。GP 587 / 213 / 232。通知 503 あり → MEMORIES_MEM 既定 on |
| 13083 | - | 133128 | STORE=1 (レーン + fsync 修正) on 既定。asia5 785 / GP asia 437 → 既定 (STORE=0) の 146736 より低い。STORE は不採用 |
| 13426 | - | **147756** | MEMORIES_CACHE=1 + HEAVY_TARGET=127.0.0.1 + SSE 修正。asia 18/47/131/348/781 (d4→d5 2.24 倍、12846 の 2.41 より悪い = asia1 に重い読みを寄せた分)、eu 297 / am 249、GP 565/277/244 (eu GP +64)。503 は 0 件 |
| 13667 | - | 139684 | 既定 + 画像 INSERT 専用プール + login 7→4 + 友達申請 9→5 + ASIA_LOCALIMG=1。asia 18/51/149/419/946 (2.83/2.92/2.81/2.26、d5 過去最高)、eu 269 / am 250。GP asia 291 (31%) に崩壊 → ASIA_LOCALIMG が第一容疑。503 20 件 |
| 13916 | - | 67391 | LOGREPL=1 (eb992f8 修正版、公式) on 既定 (往復削減 3 つ、ASIA_LOCALIMG=0)。asia 18/45/123/322/322、eu 121 / am 89、GP 273/120/84。remote_apply で全体が遅くなり半減 → LOGREPL は不採用で確定。invalid_session 401 が 3 件。後始末は確認済み (slot 0、synchronous_standby_names 空、watchdog 2 つとも inactive) |

- 13:00 asia2 PG: synchronous_commit=off (etc/pg/zz-sync.conf)。コミットは即可視。正常な shutdown/reboot は WAL を flush するので、再起動後のデータ保持は維持 |

### FRONT (asia1 の nginx 撤去) のロールバック
- `FRONT=0 scripts/batch.sh ...` か `FRONT=0 scripts/deploy.sh` で再デプロイ → deploy が nginx を enable して start/reload する。
- 手動: asia1 で `sed -i 's/^ISUREAL_FRONT=1/ISUREAL_FRONT=0/' /home/isucon/env.sh && sudo systemctl restart isureal-go && sudo systemctl enable --now nginx`
- 再起動耐性: nginx は disable、isureal-go は 30-front.conf (CAP_NET_BIND_SERVICE、Restart=always/1s)。最終の全台再起動テストで :80 の応答を必ず確認。

## asia2 論理レプリカ watchdog (pg-logrepl-watchdog)
- 対象: ISUREAL_MEM_LOGREPL=1 の各 app の購読 (application_name = スロット名 = ノード名、mem_logrepl.go の lrSlotName)。既定メンバー `asia1 europe1 america1` (unit の LR_MEMBERS)。
- 1 秒ごとに pg_stat_replication を見る (streaming / reply_time 3s 以内 / sent-replay 差 64MB 未満 / replay_lag 5s 未満 / スロット lost でない)。3s 不健全で外す、2s 健全で戻す。
- 外す: mem_logrepl_members.blocked=true (synchronous_commit=local) → walsender を pg_terminate_backend → 2.5s 待つ → synchronous_standby_names から外して reload。
- 戻す: 追加 → reload → sync_state が quorum/sync を確認 → blocked=false。起動時は全員 blocked → 2.5s → 集合を空にしてから開始。
- conf は `conf.d/zz-zlogrepl.conf` (zz-sync.conf より後に読まれる。synchronous_commit は触らない)。pg-sync-watchdog とは同じ GUC を奪い合うので unit で Conflicts。止めるときは `echo off > /etc/postgresql/16/main/pg-logrepl.mode` (全員 blocked + 集合を空)。
- deploy (asia2):
  ```
  sudo install -m 755 scripts/pg-logrepl-watchdog.sh /usr/local/bin/pg-logrepl-watchdog
  sudo install -m 644 etc/pg/pg-logrepl-watchdog.service /etc/systemd/system/
  sudo systemctl disable --now pg-sync-watchdog   # 併用不可
  sudo systemctl daemon-reload && sudo systemctl enable --now pg-logrepl-watchdog
  journalctl -u pg-logrepl-watchdog -f
  ```
- 注意: 物理スタンバイが同名 (asia1 等) の application_name / スロットで残っていると誤判定する (物理スタンバイは撤去済みの前提)。

## 理論限界との差の洗い出し (13:35、job12846 146,736 時点)
式: 点 ≈ Σ人日 × 1人日の点 + GP + 検証役。上限の目安は人数 2.9 倍/日 (前日 p90 < 0.1s)、1人日 約 60 点、検証役 4〜5 万。
| # | 差 | 今 | 目標 | 打ち手 (担当/状態) |
|---|----|----|------|------|
| 1 | asia の書き込みのスループット (d4 p90 0.74、d5 の友達申請・login・投稿の 8〜9 割が 1 秒超) | asia 4→5 日目 2.4 倍 | 2.9 倍 | STORE=1 の A/B (走行中)、書き込み 1 本の処理時間の実測と削減 (担当)、SIGN_SIMD_MIN (HMAC 13%)、ASIA_LOCALIMG (asia1→asia2 の送信 39%) |
| 2 | asia2 の CPU 100% (PG + JPEG 委譲 + asia の Memories/export) | 100% | <70% | ASIA_LOCALIMG (bytea を PG から外す)、JPEG 委譲を eu/am へ (push 停止で回線が空いた)、asia の重い読みを eu/am へ (検証役の担当) |
| 3 | 検証役 (応答が速いと同時数が約 10 倍) | 約 2 万 | 4〜5 万 | 過去月 Memories / export / 画像を負荷中も 0.1s 以下 (担当) |
| 4 | GP の取れ高 | asia 64% / eu 79% / am 84% | 95% | #1 と同じ。窓の POST は KEY SHARE + admin プールで解決済み |
| 5 | エラー (通知 503、タイムアウト) | 503 が毎回数件 | 0 | 通知 503 の調査 (担当) |
| 6 | eu/am の login (4 日目に 41% が 1 秒超) | 転送 | 地域内 | LOCAL_SESSION (1fdaef9、未マージ) |
| 7 | TB との差の説明 | 7〜13 万が不明 | 帳簿で説明 | 帳簿・検証役の天井・未発見の点源 (担当 3 本) |
| 8 | 最終版の安全 | localOnlyWrite あり | 切替スイッチ | LOCAL_ONLY_WRITE=0 の A/B、ディスク上限、全台 df、再起動試験、2 回 pass |

## 方針: watermark の slack (14:00)
- eu/am の読みの待ちは「wm ≥ 要求の到着時刻」(slack 0) を最終まで守る。slack を点のために詰める A/B はしない (asia の書き込みは commit 直後に応答が返るので、片道ぶんの猶予を前提にできない。「見えるべき」の際を踏む)。
