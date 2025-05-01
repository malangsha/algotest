import numpy as np
from typing import Dict, Any
from datetime import datetime
from core.logging_manager import get_logger

logger = get_logger(__name__)

class OptionsGreeksCalculator:
    """
    Calculates option Greeks (Delta, Gamma, Theta, Vega, Rho).
    """
    
    @staticmethod
    def calculate_greeks(
        option_type: str,        # 'call' or 'put'
        underlying_price: float,
        strike_price: float,
        time_to_expiry: float,   # in years
        risk_free_rate: float,   # annual rate, e.g., 0.05 for 5%
        volatility: float,       # implied volatility, e.g., 0.2 for 20%
    ) -> Dict[str, float]:
        """
        Calculate option Greeks using Black-Scholes model.
        
        Args:
            option_type: 'call' or 'put'
            underlying_price: Current price of the underlying
            strike_price: Strike price of the option
            time_to_expiry: Time to expiry in years
            risk_free_rate: Annual risk-free rate
            volatility: Implied volatility
            
        Returns:
            Dictionary with calculated Greeks
        """
        if option_type not in ['call', 'put']:
            raise ValueError("Option type must be 'call' or 'put'")
            
        # Handle edge cases
        if time_to_expiry <= 0:
            return {
                'delta': 1.0 if option_type == 'call' and underlying_price > strike_price else 0.0,
                'gamma': 0.0,
                'theta': 0.0,
                'vega': 0.0,
                'rho': 0.0
            }
            
        # Calculate d1 and d2
        if volatility <= 0 or underlying_price <= 0:
            return {
                'delta': 0.0,
                'gamma': 0.0,
                'theta': 0.0,
                'vega': 0.0,
                'rho': 0.0
            }
            
        # Black-Scholes formula components
        d1 = (np.log(underlying_price / strike_price) + (risk_free_rate + 0.5 * volatility ** 2) * time_to_expiry) / (volatility * np.sqrt(time_to_expiry))
        d2 = d1 - volatility * np.sqrt(time_to_expiry)
        
        # Standard normal CDF and PDF
        from scipy.stats import norm
        N_d1 = norm.cdf(d1)
        N_neg_d1 = norm.cdf(-d1)
        N_d2 = norm.cdf(d2)
        N_neg_d2 = norm.cdf(-d2)
        n_d1 = norm.pdf(d1)
        
        # Calculate Greeks
        if option_type == 'call':
            delta = N_d1
            gamma = n_d1 / (underlying_price * volatility * np.sqrt(time_to_expiry))
            theta = -(underlying_price * n_d1 * volatility) / (2 * np.sqrt(time_to_expiry)) - risk_free_rate * strike_price * np.exp(-risk_free_rate * time_to_expiry) * N_d2
            vega = underlying_price * np.sqrt(time_to_expiry) * n_d1 / 100  # divide by 100 to get per 1% change
            rho = strike_price * time_to_expiry * np.exp(-risk_free_rate * time_to_expiry) * N_d2 / 100  # divide by 100 to get per 1% change
        else:  # 'put'
            delta = N_d1 - 1
            gamma = n_d1 / (underlying_price * volatility * np.sqrt(time_to_expiry))
            theta = -(underlying_price * n_d1 * volatility) / (2 * np.sqrt(time_to_expiry)) + risk_free_rate * strike_price * np.exp(-risk_free_rate * time_to_expiry) * N_neg_d2
            vega = underlying_price * np.sqrt(time_to_expiry) * n_d1 / 100  # divide by 100 to get per 1% change
            rho = -strike_price * time_to_expiry * np.exp(-risk_free_rate * time_to_expiry) * N_neg_d2 / 100  # divide by 100 to get per 1% change
        
        return {
            'delta': delta,
            'gamma': gamma,
            'theta': theta / 365.0,  # Convert to daily theta
            'vega': vega,
            'rho': rho
        }
    
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
