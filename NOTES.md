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
