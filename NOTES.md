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

## バックログ
(問題確認後に記入)

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
