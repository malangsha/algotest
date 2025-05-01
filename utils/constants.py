from enum import Enum, auto
from typing import Dict, Any, List

class MODES(Enum):
    BACKTEST = "backtest"
    LIVE = "live"
    WEB = "web"
    PAPER = "paper"

class OrderType(Enum):
    MARKET = "MARKET"
    LIMIT = "LIMIT"
    SL = "SL"
    SL_M = "SL-M"

class OrderSide(Enum):
    BUY = "BUY"
    SELL = "SELL"

class ProductType(Enum):
    CNC = "CNC"  # Cash and Carry (delivery)
    MIS = "MIS"  # Margin Intraday Square-off
    NRML = "NRML"  # Normal for F&O

class SignalType(Enum):
    ENTRY = "ENTRY"
    EXIT = "EXIT"
    ADJUST = "ADJUST"
    ALERT = "ALERT"

class OrderStatus(Enum):
    PENDING = "PENDING"
    OPEN = "OPEN"
    PARTIALLY_FILLED = "PARTIALLY_FILLED"
    FILLED = "FILLED"
    CANCELLED = "CANCELLED"
    REJECTED = "REJECTED"

class TimeInForce(Enum):
    DAY = "DAY"
    IOC = "IOC"  # Immediate or Cancel

class Exchange(Enum):
    NSE     = "NSE"   # equity
    BSE     = "BSE"   # equity
    NFO     = "NFO"   # NSE futures & options
    BFO     = "BFO"   # BSE futures & options
    MCX     = "MCX"   # MCX futures & options

class TradeMode(Enum):
    BACKTEST = "backtest"
    LIVE = "live"
    PAPER = "paper"

class PositionSizing(Enum):
    FIXED = "fixed"
    PERCENT = "percent"
    KELLY = "kelly"
    VOLATILITY = "volatility"

class ExecutionModel(Enum):
    MARKET = "market"
    LIMIT = "limit"
    VWAP = "vwap"
    TWAP = "twap"

class SlippageModel(Enum):
    FIXED = "fixed"
    PERCENTAGE = "percentage"
    MARKET_IMPACT = "market_impact"

class CommissionModel(Enum):
    FIXED = "fixed"
    PERCENTAGE = "percentage"
    TIERED = "tiered"

class DataResolution(Enum):
    TICK = "tick"
    MIN_1 = "1m"
    MIN_5 = "5m"
    MIN_15 = "15m"
    MIN_30 = "30m"
    HOUR_1 = "1h"
    DAY_1 = "1d"
    WEEK_1 = "1w"
    MONTH_1 = "1M"

class Timeframe(Enum):
    TICK = "TICK"
    MINUTE_1 = "1m"
    MINUTE_5 = "5m"
    MINUTE_15 = "15m"
    MINUTE_30 = "30m"
    HOUR_1 = "1h"
    HOUR_4 = "4h"
    DAY_1 = "1d"
    WEEK_1 = "1w"
    MONTH_1 = "1M"

class DataSource(Enum):
    BROKER = "broker"
    CSV = "csv"
    DATABASE = "database"
    API = "api"

class RebalanceFrequency(Enum):
    NEVER = "never"
    DAILY = "daily"
    WEEKLY = "weekly"
    MONTHLY = "monthly"

class EventType(Enum):
    MARKET_DATA = "market_data"
    ORDER = "order"
    TRADE = "trade"
    POSITION = "position"
    STRATEGY = "strategy"
    SYSTEM = "system"
    SIGNAL = "signal"
    FILL = "fill"
    ACCOUNT = "account"
    CUSTOM = "custom"
    TIMER = "timer"
    RISK_BREACH = "risk_breach"
    EXECUTION = "execution"
    BAR = "bar"
   
 # New Event Types for specific timeframes
class TimeframeEventType(Enum):
    BAR = "bar"
    BAR_1M = "BAR_1M"
    BAR_5M = "BAR_5M"
    BAR_15M = "BAR_15M"
    BAR_30M = "BAR_30M"
    BAR_1H = "BAR_1H"
    BAR_4H = "BAR_4H"
    BAR_1D = "BAR_1D"   

class MarketDataType(Enum):
    QUOTE = "QUOTE"
    TRADE = "TRADE"
    BAR = "BAR"
    BID = "BID"
    ASK = "ASK"
    BID_QUANTITY = "BID_QUANTITY"
    ASK_QUANTITY = "ASK_QUANTITY"
    LAST_PRICE = "LAST_PRICE"
    VOLUME = "VOLUME"
    OPEN = "OPEN"
    HIGH = "HIGH"
    LOW = "LOW"
    CLOSE = "CLOSE"
    TIMESTAMP = "TIMESTAMP"
    OHLC = "OHLC"
    TICK = "TICK"
    GREEKS = "GREEKS"
    IMPLIED_VOLATILITY = "IMPLIED_VOLATILITY"
    OPEN_INTEREST = "OPEN_INTEREST"
    CHANGE_OI = "CHANGE_OI"
    OPTION = "OPTION"
    FUTURE = "FUTURE"
    UNKNOWN = "UNKNOWN"

class InstrumentType(Enum):
    EQUITY = "EQ"
    FUTURE = "FUT"
    OPTION = "OPT"
    INDEX = "IND"
    ETF = "ETF"
    CURRENCY = "CUR"
    COMMODITY = "COM"

class OptionType(Enum):
    """Enum for option types"""
    CALL = "CE"  # Call Option
    PUT = "PE"   # Put Option

class EventPriority(Enum):
    """Priority levels for events"""
    HIGH = 0      # Critical system events, immediate execution required
    NORMAL = 1    # Regular market data and signals
    LOW = 2       # Background tasks, non-critical updates

# NSE/BSE specific constants
NSE_INDICES = [
    "NIFTY INDEX", "NIFTY BANK", "INDIA VIX", "FINNIFTY", "MIDCPNIFTY", "NIFTYNXT50"
]

BSE_INDICES = [
    "SENSEX", "BANKEX"
]

# Indian market timings
MARKET_TIMING = {
    "pre_open_start": "09:00:00",
    "pre_open_end": "09:15:00",
    "market_open": "09:15:00",
    "market_close": "15:30:00",
    "post_close_end": "16:00:00"
}

# Trading days in India
TRADING_DAYS = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday"]

# Common NSE lot sizes for F&O
DEFAULT_LOT_SIZES = {
    "NIFTY": 50,
    "BANKNIFTY": 25,
    "FINNIFTY": 40,
    "RELIANCE": 250,
    "TCS": 150,
    "INFY": 300,
    "HDFCBANK": 550,
    "ICICIBANK": 1375
}
