# 監視機能の実装分担（Windows開発 / RasPi実装）

## 方針
- 監視Webは売買ロジックと分離し、軽量に常駐させる。
- トレード本体への影響を最小化するため、監視側は「DB参照中心 + 低頻度メトリクス取得」で構成する。

## Windowsで実装すること（今回対応）
1. 監視Webサーバーの実装
   - 軽量HTTPサーバー（標準ライブラリ中心）
   - JSON API + シンプルな監視画面（HTML）
2. 監視APIの実装
   - `/api/summary`（勝率/統計/実行サマリ/システム情報）
   - `/api/runs`（実行履歴）
   - `/api/logs`（直近約定ログ）
3. 実行履歴記録（run_history）
   - run開始/終了時刻
   - 実行時間
   - 実行結果（success/skipped/error）
   - 約定件数・売買件数
4. DBスキーマ拡張
   - `run_history` テーブル追加
5. main.pyの連携
   - 実行単位の記録保存
6. Windows上での動作確認
   - 監視Web起動
   - APIレスポンス確認

## RasPiで実施すること（移行時対応）
1. RasPi専用メトリクスの実装/有効化
   - CPU温度（`/sys/class/thermal/thermal_zone0/temp`）
   - CPUクロック（`vcgencmd measure_clock arm`）
   - スロットリング状態（`vcgencmd get_throttled`）
2. systemd常駐化
   - bot本体サービス
   - monitor webサービス
   - 自動再起動/依存順設定
3. 公開経路の保護
   - Nginx + Basic認証 + TLS
   - または Tailscale / Cloudflare Tunnel
4. 実機負荷に応じた調整
   - bot実行間隔
   - 監視更新間隔

## 運用時の推奨分離
- `main.py`（売買実行）: 45〜60秒間隔
- `monitor_web.py`（監視表示）: 常駐 + 5〜10秒更新
- 監視は読み取り中心にし、売買処理をブロックしない

## 注意点
- 監視Webをインターネットへ直接公開しない。
- 認証なし公開は禁止。
- DBはSQLiteのため、読み取り主体を維持し、重いクエリを避ける。
