from typing import Dict, Any, Optional, List
import logging
from datetime import datetime

from models.events import SignalEvent, OrderEvent, EventType
from models.order import Order
from .portfolio import Portfolio

class RiskManager:
    """Manages risk and validates trading decisions"""

    def __init__(self, portfolio: Portfolio, config: Dict[str, Any], event_manager=None):
        self.portfolio = portfolio
        self.config = config
        self.event_manager = event_manager
        self.logger = logging.getLogger("RiskManager")

        # Extract risk configuration
        self.risk_config = config.get("risk", {})

        # Position sizing limits
        self.max_position_size = self.risk_config.get("max_position_size", float('inf'))
        self.max_open_positions = self.risk_config.get("max_open_positions", float('inf'))
        self.position_sizing = self.risk_config.get("position_sizing", "percentage")
        self.position_size_percentage = self.risk_config.get("position_size_percentage", 0.02)

        # Drawdown and exposure limits
        self.max_drawdown = self.risk_config.get("max_drawdown", 0.10)  # 10% max drawdown
        self.max_exposure = self.risk_config.get("max_exposure", 1.0)  # 100% exposure
        self.max_instrument_exposure = self.risk_config.get("max_instrument_exposure", 0.20)  # 20% in one instrument

        # Daily limits
        self.max_daily_trades = self.risk_config.get("max_daily_trades", float('inf'))
        self.max_daily_loss = self.risk_config.get("max_daily_loss", float('inf'))

        # Tracking
        self.daily_trades = {}
        self.daily_pnl = {}
        self.last_reset_day = None
        
        # Register with event manager if provided
        if self.event_manager:
            self._register_event_handlers()
        
        self.logger.info("Risk Manager initialized")

    def _register_event_handlers(self):
        """Register for relevant events."""
        self.event_manager.subscribe(
            EventType.SIGNAL, 
            self._on_signal_event, 
            component_name="RiskManager"
        )
        
        self.event_manager.subscribe(
            EventType.FILL, 
            self._on_fill_event, 
            component_name="RiskManager"
        )
        
        self.logger.info("Registered with Event Manager")

    def _on_signal_event(self, event):
        """Handle signal events from the event manager."""
        # No need to do anything as signals are checked when received through check_signal method
        pass

    def _on_fill_event(self, event):
        """Handle fill events from the event manager."""
        # Update daily P&L tracking
        # Placeholder for any specific risk-related actions on fill
        # For example, check if max daily loss was hit after this fill.
        # PnL tracking is primarily handled by PerformanceTracker.
        pass

    def check_signal(self, signal: SignalEvent) -> bool:
        """Check if a signal passes risk management checks"""
        # Reset daily tracking if needed
        self._reset_daily_tracking_if_needed()

        # Instrument level checks
        if not self._check_instrument_limits(signal):
            # Publish risk breach event
            self._publish_risk_breach_event('instrument_exposure', signal.instrument_id, 
                                            f"Instrument exposure exceeds maximum limit of {self.max_instrument_exposure}")
            return False

        # Portfolio level checks
        if not self._check_portfolio_limits(signal):
            # Publish risk breach event
            breach_reason = self._get_portfolio_breach_reason(signal)
            self._publish_risk_breach_event('portfolio_limits', signal.instrument_id, breach_reason)
            return False

        # Daily limits checks
        if not self._check_daily_limits(signal):
            # Publish risk breach event
            breach_reason = self._get_daily_limits_breach_reason(signal)
            self._publish_risk_breach_event('daily_limits', signal.instrument_id, breach_reason)
            return False

        # Strategy specific checks
        if not self._check_strategy_limits(signal):
            # Publish risk breach event
            self._publish_risk_breach_event('strategy_specific', signal.instrument_id, 
                                             f"Strategy-specific risk limits breached for {signal.strategy_id}")
            return False

        return True

    def size_order(self, order: Order) -> Order:
        """Apply position sizing rules to an order"""
        # Apply position sizing method
        if self.position_sizing == "percentage":
            return self._percentage_position_sizing(order)
        elif self.position_sizing == "fixed":
            return self._fixed_position_sizing(order)
        elif self.position_sizing == "volatility":
            return self._volatility_position_sizing(order)
        else:
            # Default to original order
            return order

    def _percentage_position_sizing(self, order: Order) -> Order:
        """Size order based on portfolio percentage"""
        portfolio_value = self.portfolio.total_value
        target_value = portfolio_value * self.position_size_percentage

        # Estimate order value and adjust quantity
        current_price = self.portfolio.get_latest_price(order.instrument_id)
        if current_price and current_price > 0:
            # Calculate new quantity
            new_quantity = target_value / current_price
            if order.side == "SELL":
                new_quantity = min(new_quantity, order.quantity)

            # Ensure within limits
            if new_quantity * current_price > self.max_position_size:
                new_quantity = self.max_position_size / current_price

            order.quantity = new_quantity

        return order

    def _fixed_position_sizing(self, order: Order) -> Order:
        """Size order based on fixed amount"""
        fixed_amount = self.risk_config.get("fixed_position_size", 10000)

        # Estimate order value and adjust quantity
        current_price = self.portfolio.get_latest_price(order.instrument_id)
        if current_price and current_price > 0:
            # Calculate new quantity
            new_quantity = fixed_amount / current_price
            if order.side == "SELL":
                new_quantity = min(new_quantity, order.quantity)

            order.quantity = new_quantity

        return order

    def _volatility_position_sizing(self, order: Order) -> Order:
        """Size order based on volatility (ATR)"""
        # Implementation would typically use ATR (Average True Range)
        # This is a simplified placeholder
        portfolio_value = self.portfolio.total_value
        target_risk = portfolio_value * self.risk_config.get("risk_per_trade", 0.01)  # 1% risk

        # This would use actual volatility calculations
        # For now, we just use a simplified approach
        current_price = self.portfolio.get_latest_price(order.instrument_id)
        if current_price:
            atr_factor = 0.02  # Example: assume ATR is 2% of price
            atr = current_price * atr_factor

            if atr > 0:
                # Size position such that if price moves by 1 ATR against us, we lose target_risk
                new_quantity = target_risk / atr
                if order.side == "SELL":
                    new_quantity = min(new_quantity, order.quantity)

                order.quantity = new_quantity

        return order

    def _check_instrument_limits(self, signal: SignalEvent) -> bool:
        """Check instrument specific risk limits"""
        instrument_id = signal.instrument_id

        # Check existing position size
        current_exposure = self.portfolio.get_instrument_exposure(instrument_id)
        if current_exposure >= self.max_instrument_exposure:
            self.logger.warning(
                f"Signal rejected: Instrument {instrument_id} exposure ({current_exposure:.2f}) "
                f"exceeds limit ({self.max_instrument_exposure:.2f})"
            )
            return False

        return True

    def _check_portfolio_limits(self, signal: SignalEvent) -> bool:
        """Check portfolio level risk limits"""
        # Check max open positions
        if len(self.portfolio.get_open_positions()) >= self.max_open_positions and signal.direction > 0:
            self.logger.warning(
                f"Signal rejected: Max open positions ({self.max_open_positions}) reached"
            )
            return False

        # Check drawdown
        current_drawdown = self.portfolio.get_drawdown()
        if current_drawdown >= self.max_drawdown:
            self.logger.warning(
                f"Signal rejected: Current drawdown ({current_drawdown:.2f}) "
                f"exceeds limit ({self.max_drawdown:.2f})"
            )
            return False

        # Check total exposure
        current_exposure = self.portfolio.get_total_exposure()
        if current_exposure >= self.max_exposure and signal.direction > 0:
            self.logger.warning(
                f"Signal rejected: Total exposure ({current_exposure:.2f}) "
                f"exceeds limit ({self.max_exposure:.2f})"
            )
            return False

        return True

    def _check_daily_limits(self, signal: SignalEvent) -> bool:
        """Check daily risk limits"""
        strategy_id = signal.strategy_id

        # Check max daily trades
        daily_trade_count = self.daily_trades.get(strategy_id, 0)
        if daily_trade_count >= self.max_daily_trades:
            self.logger.warning(
                f"Signal rejected: Max daily trades ({self.max_daily_trades}) reached for strategy {strategy_id}"
            )
            return False

        # Check max daily loss
        daily_pnl = self.daily_pnl.get(strategy_id, 0)
        if daily_pnl <= -self.max_daily_loss:
            self.logger.warning(
                f"Signal rejected: Max daily loss (${self.max_daily_loss}) reached for strategy {strategy_id}"
            )
            return False

        return True

    def _check_strategy_limits(self, signal: SignalEvent) -> bool:
        """Check strategy specific risk limits"""
        # This would check strategy-specific risk parameters
        # that might be defined in the config

        # For now, we simply allow all signals
        return True

    def _reset_daily_tracking_if_needed(self) -> None:
        """Reset daily tracking data if it's a new day"""
        current_day = datetime.now().date()

        if self.last_reset_day != current_day:
            self.daily_trades = {}
            self.daily_pnl = {}

    def _get_portfolio_breach_reason(self, signal: SignalEvent) -> str:
        """Get the reason for portfolio limits breach"""
        if len(self.portfolio.get_open_positions()) >= self.max_open_positions and signal.direction > 0:
            return f"Max open positions ({self.max_open_positions}) reached"
            
        current_drawdown = self.portfolio.get_drawdown()
        if current_drawdown >= self.max_drawdown:
            return f"Current drawdown ({current_drawdown:.2f}) exceeds limit ({self.max_drawdown:.2f})"
            
        current_exposure = self.portfolio.get_total_exposure()
        if current_exposure >= self.max_exposure and signal.direction > 0:
            return f"Total exposure ({current_exposure:.2f}) exceeds limit ({self.max_exposure:.2f})"
            
        return "Unknown portfolio limit breach"
        
    def _get_daily_limits_breach_reason(self, signal: SignalEvent) -> str:
        """Get the reason for daily limits breach"""
        strategy_id = signal.strategy_id
        
        daily_trade_count = self.daily_trades.get(strategy_id, 0)
        if daily_trade_count >= self.max_daily_trades:
            return f"Max daily trades ({self.max_daily_trades}) reached for strategy {strategy_id}"
            
        daily_pnl = self.daily_pnl.get(strategy_id, 0)
        if daily_pnl <= -self.max_daily_loss:
            return f"Max daily loss (${self.max_daily_loss}) reached for strategy {strategy_id}"
            
        return "Unknown daily limit breach"
        
    def _publish_risk_breach_event(self, breach_type: str, symbol: str, message: str, action: str = 'exit_position'):
        """
        Publish a risk breach event to notify the system of a risk limit breach.
        
        Args:
            breach_type: Type of risk breach
            symbol: Symbol involved in the breach
            message: Description of the breach
            action: Recommended action to take
        """
        if not self.event_manager:
            self.logger.warning("No event manager available to publish risk breach")
            return
            
        from models.events import Event, EventType
        import time
        
        # Create risk breach event
        event = Event(
            event_type=EventType.RISK_BREACH,
            timestamp=int(time.time() * 1000)
        )
        
        # Add breach details
        event.breach_type = breach_type
        event.symbol = symbol
        event.message = message
        event.action = action
        
        # If we have position data, include it
        if hasattr(self.portfolio, 'get_position') and symbol:
            position = self.portfolio.get_position(symbol)
            if position:
                event.position = position.quantity
        
        # Publish the event
        self.event_manager.publish(event)
        self.logger.warning(f"Published risk breach event: {breach_type} - {message}")
        
    def monitor_portfolio_risk(self):
        """
        Actively monitor portfolio risk and publish risk breach events if needed.
        Called periodically to check overall portfolio health.
        """
        # Check current drawdown
        current_drawdown = self.portfolio.get_drawdown()
        if current_drawdown >= self.max_drawdown:
            # Serious risk breach - may need to liquidate positions
            self._publish_risk_breach_event(
                'portfolio_drawdown',
                None,
                f"Portfolio drawdown ({current_drawdown:.2f}) exceeds maximum ({self.max_drawdown:.2f})",
                'halt_trading'
            )
            return False
            
        # Check total exposure
        total_exposure = self.portfolio.get_total_exposure()
        if total_exposure >= self.max_exposure:
            self._publish_risk_breach_event(
                'portfolio_exposure',
                None,
                f"Portfolio exposure ({total_exposure:.2f}) exceeds maximum ({self.max_exposure:.2f})",
                'reduce_exposure'
            )
            return False
            
        # Check individual positions for breaches
        for position in self.portfolio.get_open_positions():
            symbol = position.symbol
            exposure = self.portfolio.get_instrument_exposure(symbol)
            
            if exposure > self.max_instrument_exposure:
                self._publish_risk_breach_event(
                    'instrument_exposure',
                    symbol,
                    f"Instrument {symbol} exposure ({exposure:.2f}) exceeds maximum ({self.max_instrument_exposure:.2f})",
                    'reduce_position'
                )
                
        return True

    # To facilitate unit testing
    def register_with_event_manager(self, event_manager):
        """
        Register this position manager with an event manager.
        
        Args:
            event_manager: The event manager to register with
        """
        self.event_manager = event_manager
        self._register_event_handlers()
        self.logger.info("Risk Manager registered with Event Manager")