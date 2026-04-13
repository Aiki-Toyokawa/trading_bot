from __future__ import annotations

from typing import Final

# 注文ログとして保存するイベント種別（DBのCHECK制約と合わせる）
ORDER_EVENT_TYPES: Final[tuple[str, ...]] = (
    "BUY_FILLED",
    "BUY_NOT_FILLED",
    "SELL_FILLED",
)

# 損益ラベル（6段階）
OUTCOME_TIERS: Final[tuple[str, ...]] = (
    "EXTRA_LOSE",
    "BIG_LOSE",
    "LOSE",
    "WIN",
    "BIG_WIN",
    "EXTRA_WIN",
)

# ------------------------------------------------------------
# Runtime / API settings
# ------------------------------------------------------------
USE_PAPER_ACCOUNT: Final[bool] = True  # True: paper口座, False: live口座
ALPACA_BASE_URL_PAPER: Final[str] = "https://paper-api.alpaca.markets"  # paper注文先
ALPACA_BASE_URL_LIVE: Final[str] = "https://api.alpaca.markets"  # live注文先
ALPACA_DATA_URL: Final[str] = "https://data.alpaca.markets"  # 株価データAPI
ALPACA_DATA_FEED: Final[str] = "iex"  # データフィード（iex/sip 等）
HTTP_TIMEOUT_SECONDS: Final[int] = 15  # HTTPタイムアウト秒
ORDER_STATUS_MAX_WAIT_SECONDS: Final[int] = 8  # 発注直後の状態確定待機秒数
ORDER_STATUS_POLL_INTERVAL_SECONDS: Final[float] = 1.0  # 注文状態ポーリング間隔秒

# ------------------------------------------------------------
# Timezone display / aggregation settings
# ------------------------------------------------------------
TIMEZONE_MODE: Final[str] = "JST"  # "UTC" or "JST"（互換で "JTC" もJST扱い）

# ------------------------------------------------------------
# Claude settings
# ------------------------------------------------------------
CLAUDE_MODEL: Final[str] = "claude-sonnet-4-6"  # 利用モデル
CLAUDE_MAX_TOKENS_ENTRY_JSON: Final[int] = 700  # EntryスコアJSONの最大出力token
CLAUDE_MAX_TOKENS_LOG_ANALYSIS: Final[int] = 900  # ログ分析JSONの最大出力token

# ------------------------------------------------------------
# Market time guard settings
# ------------------------------------------------------------
MARKET_HOURS_GUARD_ENABLED: Final[bool] = True  # 市場時間外の実行抑止を有効化
MARKET_HOURS_FALLBACK_ENABLED: Final[bool] = True  # Alpaca clock失敗時にfallback時間判定を使う
MARKET_HOURS_FALLBACK_TZ: Final[str] = "Asia/Tokyo"  # fallback判定のタイムゾーン
MARKET_HOURS_FALLBACK_START: Final[str] = "22:30"  # fallback開始時刻（JST）
MARKET_HOURS_FALLBACK_END: Final[str] = "05:00"  # fallback終了時刻（JST）
MARKET_HOURS_FALLBACK_SESSION_WEEKDAYS: Final[tuple[int, ...]] = (
    0,
    1,
    2,
    3,
    4,
)  # セッション開始日の曜日（0=Mon ... 6=Sun）

# ------------------------------------------------------------
# Entry universe / screening constants
# ------------------------------------------------------------
FINVIZ_CANDIDATE_LIMIT: Final[int] = 40  # Finvizから取得する候補数
CLAUDE_TOP_PICK_LIMIT: Final[int] = 0  # 0以下は上限なし（Claude上位絞り込み無効）
MIN_PRICE_USD: Final[float] = 8.0  # 最低株価フィルタ
MAX_PRICE_USD: Final[float] = 250.0  # 最高株価フィルタ
MIN_AVG_VOLUME: Final[float] = 80_000  # 最低平均出来高フィルタ

# ------------------------------------------------------------
# Core trading / risk constants
# ------------------------------------------------------------
MAX_POSITIONS: Final[int] = 5  # 同時保有上限（0以下は上限なし）
POSITION_SIZE_FRACTION: Final[float] = 0.20  # 1トレード予算の基準比率（フル投下OFF時）
ALLOW_FULL_CASH_DEPLOYMENT: Final[bool] = True  # Trueで残キャッシュを候補数で均等配分
ENABLE_SCORE_WEIGHTED_SIZING: Final[bool] = True  # Trueで候補スコアに比例して資金配分
SCORE_WEIGHT_FLOOR: Final[float] = 0.55  # スコア配分時の下限（過度な極端配分を防ぐ）
MAX_NEW_ORDERS_PER_RUN: Final[int] = 3  # 1回実行あたりの新規発注上限（0以下は上限なし）
FULL_DEPLOYMENT_PER_TRADE_CAP_FRACTION: Final[float] = 0.20  # フル投下モード時でも1銘柄の上限を口座比で制限
MIN_ORDER_NOTIONAL_USD: Final[float] = 100.0  # 最低発注金額（小さすぎる注文を回避）
ENABLE_RISK_CAP_PER_TRADE: Final[bool] = False  # TrueでRISK_PER_TRADEを厳密適用
RISK_PER_TRADE: Final[float] = 0.01  # 1トレード最大許容損失（口座比）
STOP_LOSS_RATE: Final[float] = 0.97  # 初期損切り（エントリー比-3%）
STOP_LIMIT_RATE: Final[float] = 0.96  # ストップリミット下限（エントリー比-4%）
TAKE_PROFIT_RATE: Final[float] = 1.06  # 通常利確トリガー（+6%）
TRAILING_RATE: Final[float] = 0.97  # 通常トレーリング幅（高値比-3%）
TRAILING_RATE_TIGHT: Final[float] = 0.985  # 急落警戒時のタイトトレーリング（高値比-1.5%）
ENABLE_ADAPTIVE_TRAILING_BOOST: Final[bool] = True  # 半利確後、強トレンド時のみトレーリングを緩めて利を伸ばす
TRAILING_RATE_STRONG_TREND: Final[float] = 0.965  # 強トレンド時のトレーリング幅（高値比-3.5%）
TRAILING_BOOST_TRIGGER_RATE: Final[float] = 1.10  # 強トレンド緩和を有効化する含み益水準（エントリー比+10%）
TRAILING_RATE_VERY_STRONG_TREND: Final[float] = 0.955  # 超強トレンド時のトレーリング幅（高値比-4.5%）
TRAILING_VERY_STRONG_TRIGGER_RATE: Final[float] = 1.20  # 超強トレンド緩和を有効化する含み益水準（エントリー比+20%）
REMAINING_HALF_MIN_PROFIT_RATE: Final[float] = 1.01  # 残り半分の最低利益フロア（エントリー比+1%）
REMAINING_HALF_STRONG_FLOOR_TRIGGER_RATE: Final[float] = 1.15  # 含み益が大きい場合に利益フロアを引き上げる閾値（エントリー比+15%）
REMAINING_HALF_MIN_PROFIT_RATE_STRONG: Final[float] = 1.03  # 強含み益時の残り半分最低利益フロア（エントリー比+3%）
DAILY_LOSS_LIMIT: Final[float] = 0.03  # 日次最大損失（口座比-3%）
ENABLE_CRASH_PROTECTION: Final[bool] = True  # 急落時の緊急退出判定を有効化
CRASH_EXIT_FROM_PREV_CLOSE_PCT: Final[float] = -0.06  # 前日終値比-6%で即時退出
CRASH_TIGHTEN_FROM_PREV_CLOSE_PCT: Final[float] = -0.02  # 前日終値比-2%でトレーリングをタイト化
ENABLE_INTRADAY_HIGH_FOR_TRAILING: Final[bool] = True  # 半利確後の高値更新で最新1分足高値を利用

# ------------------------------------------------------------
# Indicator constants
# ------------------------------------------------------------
RSI_PERIOD: Final[int] = 14  # RSI期間
SMA_FAST_PERIOD: Final[int] = 50  # 短期SMA期間
SMA_SLOW_PERIOD: Final[int] = 200  # 長期SMA期間
ATR_PERIOD: Final[int] = 14  # ATR期間
TRAILING_LOOKBACK_BARS: Final[int] = 20  # トレーリング用高値算出の参照本数
RSI_LOW: Final[float] = 40.0  # RSI下限
RSI_HIGH_DEFAULT: Final[float] = 65.0  # RSI上限（通常トレンド）
RSI_HIGH_STRONG_TREND: Final[float] = 75.0  # RSI上限（強トレンド）
STRONG_TREND_THRESHOLD: Final[float] = 0.05  # 強トレンド判定（SMA50とSMA200乖離率）
VOLUME_SURGE_MULTIPLIER: Final[float] = 1.5  # 出来高急増判定倍率
ENABLE_RANGE_ENTRY_GUARD: Final[bool] = True  # レンジ判定時の低信頼エントリーを抑制
RANGE_ENTRY_MIN_CONFIDENCE: Final[float] = 0.72  # レンジ相場で許容する最低confidence
ENTRY_MIN_SCORE: Final[float] = 0.55  # エントリー最終スコア下限（これ未満は見送り）
ENTRY_ORDER_MIN_SCORE: Final[float] = 0.60  # 実発注に進める最小スコア（品質優先）
TREND_MIN_CONFIDENCE: Final[float] = 0.45  # トレンド相場で許容する最低confidence
MIN_ATR_PCT: Final[float] = 0.008  # ATR比率の下限（低すぎる値動きを除外）
MAX_ATR_PCT: Final[float] = 0.12  # ATR比率の上限（過度な高ボラを除外）
ENABLE_FEED_ADAPTIVE_VOLUME_THRESHOLD: Final[bool] = True  # データフィード差を考慮して出来高閾値を補正
IEX_VOLUME_ADJUSTMENT_FACTOR: Final[float] = 0.50  # IEX利用時の出来高閾値補正係数
RECOMMENDED_POLLING_SECONDS: Final[int] = 45  # 定期起動推奨間隔（運用目安）

# Finviz失敗時のフォールバック銘柄
DEFAULT_FALLBACK_SYMBOLS: Final[tuple[str, ...]] = (
    "AAPL",
    "MSFT",
    "NVDA",
    "AMZN",
    "META",
    "GOOGL",
    "TSLA",
)


def classify_outcome_tier(capital_change_pct: float) -> str:
    """口座変化率(%)を6段階ラベルへ分類する。"""
    if capital_change_pct <= -5.0:
        return "EXTRA_LOSE"
    if capital_change_pct <= -2.0:
        return "BIG_LOSE"
    if capital_change_pct < 0.0:
        return "LOSE"
    if capital_change_pct < 2.0:
        return "WIN"
    if capital_change_pct < 5.0:
        return "BIG_WIN"
    return "EXTRA_WIN"


def get_timezone_mode_normalized() -> str:
    mode = str(TIMEZONE_MODE).strip().upper()
    if mode in {"JST", "JTC"}:
        return "JST"
    return "UTC"


def get_runtime_timezone_offset_hours() -> int:
    return 9 if get_timezone_mode_normalized() == "JST" else 0


def get_runtime_timezone_label() -> str:
    return get_timezone_mode_normalized()


def get_sqlite_timezone_shift_modifier() -> str:
    return "+9 hours" if get_timezone_mode_normalized() == "JST" else "+0 hours"


def get_effective_min_avg_volume(data_feed: str) -> float:
    base = float(MIN_AVG_VOLUME)
    if ENABLE_FEED_ADAPTIVE_VOLUME_THRESHOLD:
        feed = str(data_feed or "").strip().lower()
        if feed == "iex":
            base = max(base * float(IEX_VOLUME_ADJUSTMENT_FACTOR), 1.0)
    if USE_PAPER_ACCOUNT and TEST_ENTRY_RELAX_MODE:
        base = max(base * float(TEST_MIN_AVG_VOLUME_FACTOR), 1.0)
    return base


# ------------------------------------------------------------
# Test / Debug settings
# ------------------------------------------------------------
DEBUG_FORCE_RUN_WHEN_MARKET_CLOSED: Final[bool] = False  # Trueで閉場時も強制実行
DEBUG_EXECUTION_MODE: Final[int] = 3  # 0:通常, 1:判定のみ, 2:売判定のみ, 3:買判定のみ
TEST_ENTRY_RELAX_MODE: Final[bool] = True  # paperテスト時にエントリー判定を段階的に緩和
TEST_MIN_AVG_VOLUME_FACTOR: Final[float] = 0.60  # paper緩和時の出来高閾値係数
TEST_ALLOW_TREND_OVERRIDE: Final[bool] = True  # trend_not_up の救済判定を許可
TEST_ALLOW_RSI_SOFT_PASS: Final[bool] = True  # rsi_condition_failed の救済判定を許可
TEST_REQUIRE_TREND_REGIME_FOR_SOFT_PASS: Final[bool] = True  # 救済判定はmarket_regime=trend時のみ有効
TEST_TREND_OVERRIDE_MIN_CONFIDENCE: Final[float] = 0.68  # 救済判定を許可する最低confidence
TEST_RSI_SOFT_LOW: Final[float] = 35.0  # RSI救済判定の下限
TEST_RSI_SOFT_HIGH: Final[float] = 80.0  # RSI救済判定の上限
TEST_MAX_NEW_ORDERS_PER_RUN_OVERRIDE: Final[int] = 4  # paper緩和時の1回あたり新規発注上限（0以下で無効）
TEST_ENTRY_ORDER_MIN_SCORE: Final[float] = 0.56  # paper緩和時の実発注最小スコア

# ------------------------------------------------------------
# DB maintenance settings
# ------------------------------------------------------------
ENABLE_DB_RETENTION_CLEANUP: Final[bool] = True  # Trueで起動時に古いデータを削除
SIGNAL_RETENTION_DAYS: Final[int] = 90  # signals保持日数（超過分を削除）
POSITION_SNAPSHOT_RETENTION_DAYS: Final[int] = 30  # position_snapshots保持日数
