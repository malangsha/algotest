class AlgoTradingError(Exception):
    """Base exception for the algorithmic trading framework."""
    pass

class ConfigError(AlgoTradingError):
    """Raised when there is an issue with the configuration."""
    pass

class InitializationError(AlgoTradingError):
    """Raised when a component fails to initialize."""
    pass

class MarketDataError(AlgoTradingError):
    """Raised when there is an issue with market data."""
    pass

class OrderError(AlgoTradingError):
    """Raised when there is an issue with order management."""
    pass

class StrategyError(AlgoTradingError):
    """Raised when there is an issue with a trading strategy."""
    pass

class BrokerError(AlgoTradingError):
    """Raised when there is an issue with broker communication."""
    pass

class PositionError(AlgoTradingError):
    """Raised when there is an issue with position management."""
    pass

class ValidationError(AlgoTradingError):
    """Raised when validation fails."""
    pass

class BacktestError(AlgoTradingError):
    """Raised when there is an issue with backtesting."""
    pass

class LiveTradingException(AlgoTradingError):
    """Raised when there is an issue with backtesting."""
    pass

class DatabaseError(AlgoTradingError):
    """Raised when there is an issue with database operations."""
    pass

class DataLoadingError(AlgoTradingError):
    """Raised when there is an issue with database operations."""
    pass

class DataProcessingError(AlgoTradingError):
    """Raised when there is an issue with database operations."""
    pass

class MarketDataTypeError(AlgoTradingError):
    """Raised when there is an issue with database operations."""
    pass

class MarketDataException(AlgoTradingError):
    """Raised when there is an issue with database operations."""
    pass

class RiskError(AlgoTradingError):
    """Raised when a risk limit is breached."""
    pass

class AuthenticationError(AlgoTradingError):
    """Raised when there is an issue with authentication."""
    pass

class ConnectionError(AlgoTradingError):
    """Raised when there is a connection issue."""
    pass

class PortfolioError(AlgoTradingError):
    """Raised when there is a connection issue."""
    pass

class TimeoutError(ConnectionError):
    """Raised when a connection times out."""
    pass

class RateLimitError(ConnectionError):
    """Raised when rate limits are exceeded."""
    pass

class InsufficientFundsError(OrderError):
    """Raised when there are insufficient funds for an order."""
    pass

class CircuitBreakError(RiskError):
    """Raised when a circuit breaker is triggered."""
    pass
