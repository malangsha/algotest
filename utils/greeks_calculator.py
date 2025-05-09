import numpy as np
from typing import Dict, Any
from scipy.stats import norm
from datetime import datetime
from core.logging_manager import get_logger

logger = get_logger(__name__)

class OptionsGreeksCalculator:
    """
    Calculates option Greeks (Delta, Gamma, Theta, Vega, Rho) using the Black-Scholes model.
    Optimized for Indian options market.
    """
    
    @staticmethod
    def calculate_greeks(
        option_type: str,        # 'CE' for Call or 'PE' for Put
        underlying_price: float,
        strike_price: float,
        time_to_expiry: float,   # in years
        risk_free_rate: float,   # annual rate, e.g., 0.05 for 5%
        volatility: float,       # implied volatility, e.g., 0.2 for 20%
    ) -> Dict[str, float]:
        """
        Calculate option Greeks using Black-Scholes model.
        
        Args:
            option_type: 'CE' for Call or 'PE' for Put
            underlying_price: Current price of the underlying
            strike_price: Strike price of the option
            time_to_expiry: Time to expiry in years
            risk_free_rate: Annual risk-free rate
            volatility: Implied volatility
            
        Returns:
            Dictionary with calculated Greeks (delta, gamma, theta, vega, rho)
        """
        if option_type not in ['CE', 'PE']:
            raise ValueError("Option type must be 'CE' (Call) or 'PE' (Put)")
            
        # Handle expiry day or expired options
        if time_to_expiry <= 0:
            if option_type == 'CE':
                delta = float(1.0) if underlying_price > strike_price else float(0.0)
            else:  # 'PE'
                delta = float(-1.0) if underlying_price < strike_price else float(0.0)
                
            return {
                'delta': delta,
                'gamma': float(0.0),
                'theta': float(0.0),
                'vega': float(0.0),
                'rho': float(0.0)
            }
            
        # Handle invalid volatility or price values
        if volatility <= 0 or underlying_price <= 0:
            return {
                'delta': float(0.0),
                'gamma': float(0.0),
                'theta': float(0.0),
                'vega': float(0.0),
                'rho': float(0.0)
            }
            
        # Black-Scholes formula components
        d1 = (np.log(underlying_price / strike_price) + 
              (risk_free_rate + 0.5 * volatility ** 2) * time_to_expiry) / (volatility * np.sqrt(time_to_expiry))
        d2 = d1 - volatility * np.sqrt(time_to_expiry)
        
        # Standard normal CDF and PDF
        N_d1 = norm.cdf(d1)
        N_neg_d1 = norm.cdf(-d1)
        N_d2 = norm.cdf(d2)
        N_neg_d2 = norm.cdf(-d2)
        n_d1 = norm.pdf(d1)
        
        # Calculate Greeks
        if option_type == 'CE':  # Call option
            delta = N_d1
            gamma = n_d1 / (underlying_price * volatility * np.sqrt(time_to_expiry))
            theta = -(underlying_price * n_d1 * volatility) / (2 * np.sqrt(time_to_expiry)) - \
                    risk_free_rate * strike_price * np.exp(-risk_free_rate * time_to_expiry) * N_d2
            vega = underlying_price * np.sqrt(time_to_expiry) * n_d1 / 100  # per 1% change in volatility
            rho = strike_price * time_to_expiry * np.exp(-risk_free_rate * time_to_expiry) * N_d2 / 100  # per 1% change in rate
        else:  # 'PE' - Put option
            delta = N_d1 - 1  # Same as -N_neg_d1
            gamma = n_d1 / (underlying_price * volatility * np.sqrt(time_to_expiry))
            theta = -(underlying_price * n_d1 * volatility) / (2 * np.sqrt(time_to_expiry)) + \
                    risk_free_rate * strike_price * np.exp(-risk_free_rate * time_to_expiry) * N_neg_d2
            vega = underlying_price * np.sqrt(time_to_expiry) * n_d1 / 100  # per 1% change in volatility
            rho = -strike_price * time_to_expiry * np.exp(-risk_free_rate * time_to_expiry) * N_neg_d2 / 100  # per 1% change in rate
        
        # Convert theta to daily (from annual)
        daily_theta = theta / 365.0
        
        return {
            'delta': float(delta),
            'gamma': float(gamma),
            'theta': float(daily_theta),
            'vega': float(vega),
            'rho': float(rho)
        }
        
    @staticmethod
    def calculate_implied_volatility(
        option_type: str,
        option_price: float,
        underlying_price: float,
        strike_price: float,
        time_to_expiry: float,
        risk_free_rate: float,
        precision: float = 0.00001,
        max_iterations: int = 100
    ) -> float:
        """
        Calculate implied volatility using binary search method.
        
        Args:
            option_type: 'CE' for Call or 'PE' for Put
            option_price: Market price of the option
            underlying_price: Current price of the underlying
            strike_price: Strike price of the option
            time_to_expiry: Time to expiry in years
            risk_free_rate: Annual risk-free rate
            precision: Desired precision of calculation
            max_iterations: Maximum number of iterations
            
        Returns:
            Implied volatility as a float
        """
        from scipy.stats import norm
        
        if time_to_expiry <= 0 or option_price <= 0 or underlying_price <= 0:
            return 0.0
            
        # Initial volatility guesses
        vol_low = 0.001
        vol_high = 5.0  # 500% volatility as upper bound
        
        # Black-Scholes price calculation
        def bs_price(volatility):
            if volatility <= 0:
                return 0.0
                
            d1 = (np.log(underlying_price / strike_price) + 
                 (risk_free_rate + 0.5 * volatility**2) * time_to_expiry) / (volatility * np.sqrt(time_to_expiry))
            d2 = d1 - volatility * np.sqrt(time_to_expiry)
            
            if option_type == 'CE':  # Call
                price = underlying_price * norm.cdf(d1) - strike_price * np.exp(-risk_free_rate * time_to_expiry) * norm.cdf(d2)
            else:  # Put
                price = strike_price * np.exp(-risk_free_rate * time_to_expiry) * norm.cdf(-d2) - underlying_price * norm.cdf(-d1)
                
            return price
        
        # Binary search
        for _ in range(max_iterations):
            vol_mid = (vol_low + vol_high) / 2
            price_mid = bs_price(vol_mid)
            
            if abs(price_mid - option_price) < precision:
                return float(vol_mid)
                
            if price_mid > option_price:
                vol_high = vol_mid
            else:
                vol_low = vol_mid
                
        # Return the best estimate after max iterations
        return float((vol_low + vol_high) / 2)
    
    @staticmethod
    def get_symbol_info(symbol: str) -> Dict[str, Any]:
        """
        Extract option information from symbol name.
        This should be customized based on your actual symbol naming convention.
        
        Args:
            symbol: Option symbol
            
        Returns:
            Dictionary with option details (underlying, type, strike, expiry)
        """
        # This is a placeholder implementation - customize based on your actual symbol format
        # Example symbol format: NIFTY2305025000CE (NIFTY May 2023 25000 Call)
        try:
            # Parse based on expected format - this is highly dependent on your data provider
            if "CE" in symbol:
                option_type = "call"
            elif "PE" in symbol:
                option_type = "put"
            else:
                return None  # Not an option
                
            # Extract other details based on your symbol naming convention
            # This is just a placeholder and should be customized
            return {
                'underlying': 'NIFTY',  # Extract from symbol
                'option_type': option_type,
                'strike': 25000.0,  # Extract from symbol
                'expiry': datetime(2023, 5, 25),  # Extract from symbol
                'is_option': True
            }
        except Exception:
            # Not a valid option symbol or parsing failed
            return None
