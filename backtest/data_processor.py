from typing import Dict, List, Optional, Union, Any
import pandas as pd
import numpy as np
import logging

from utils.exceptions import DataProcessingError

class DataProcessor:
    """
    Process and transform market data for backtesting.
    Includes functionality for data cleaning, feature engineering,
    and technical indicator calculation.
    """

    def __init__(self):
        """Initialize the data processor"""
        self.logger = logging.getLogger('backtest.data_processor')

    def process_data(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Process raw market data, cleaning and preparing it for backtesting.

        Args:
            data: Raw OHLCV data as a pandas DataFrame

        Returns:
            Processed DataFrame ready for backtesting
        """
        try:
            # Make a copy to avoid modifying the original data
            processed = data.copy()

            # Basic data cleaning
            processed = self._clean_data(processed)

            # Fill missing values
            processed = self._fill_missing_values(processed)

            # Handle outliers
            processed = self._handle_outliers(processed)

            # Add returns columns
            processed = self._add_returns(processed)

            return processed

        except Exception as e:
            self.logger.error(f"Error processing data: {str(e)}")
            raise DataProcessingError(f"Failed to process data: {str(e)}")

    def _clean_data(self, data: pd.DataFrame) -> pd.DataFrame:
        """Clean the data by removing duplicates and invalid values"""
        # Remove duplicate entries
        data = data[~data.index.duplicated(keep='first')]

        # Check for invalid OHLC values (e.g., negative prices)
        for col in ['open', 'high', 'low', 'close']:
            if col in data.columns:
                # Replace negative values with NaN
                data.loc[data[col] <= 0, col] = np.nan

        # Ensure high is the highest price of the day
        if all(col in data.columns for col in ['open', 'high', 'low', 'close']):
            # Get the maximum of open, close, high for each row
            data['high'] = data[['open', 'high', 'close']].max(axis=1)

            # Ensure low is the lowest price of the day
            data['low'] = data[['open', 'low', 'close']].min(axis=1)

        return data

    def _fill_missing_values(self, data: pd.DataFrame) -> pd.DataFrame:
        """Fill missing values in the data"""
        # For OHLC data, forward fill is usually appropriate
        for col in ['open', 'high', 'low', 'close']:
            if col in data.columns:
                # Forward fill first
                data[col] = data[col].fillna(method='ffill')

                # If there are still NaNs at the beginning, backward fill
                data[col] = data[col].fillna(method='bfill')

        # For volume, fill NaNs with 0
        if 'volume' in data.columns:
            data['volume'] = data['volume'].fillna(0)

        return data

    def _handle_outliers(self, data: pd.DataFrame) -> pd.DataFrame:
        """Detect and handle outliers in price data"""
        # Copy data to avoid modifying original
        cleaned_data = data.copy()

        # Only apply to OHLC columns
        price_columns = [col for col in ['open', 'high', 'low', 'close'] if col in data.columns]

        for col in price_columns:
            # Calculate rolling median and MAD (Median Absolute Deviation)
            rolling_median = data[col].rolling(window=21, min_periods=5).median()
            rolling_mad = (data[col] - rolling_median).abs().rolling(window=21, min_periods=5).median()

            # Define threshold for outliers (typically 3-5 times MAD)
            threshold = 5

            # Identify outliers
            outliers = (data[col] < (rolling_median - threshold * rolling_mad)) | \
                      (data[col] > (rolling_median + threshold * rolling_mad))

            # Replace outliers with rolling median
            cleaned_data.loc[outliers, col] = rolling_median[outliers]

        return cleaned_data

    def _add_returns(self, data: pd.DataFrame) -> pd.DataFrame:
        """Add return calculations to the data"""
        if 'close' in data.columns:
            # Daily returns (percent change)
            data['returns'] = data['close'].pct_change()

            # Log returns
            data['log_returns'] = np.log(data['close'] / data['close'].shift(1))

            # Cumulative returns
            data['cum_returns'] = (1 + data['returns']).cumprod() - 1

        return data

    def add_technical_indicators(self, data: pd.DataFrame, indicators: List[Dict[str, Any]]) -> pd.DataFrame:
        """
        Add technical indicators to the data.

        Args:
            data: DataFrame with OHLCV data
            indicators: List of dictionaries specifying indicators to add
                Each dict should have 'type' and any parameters needed

        Returns:
            DataFrame with added technical indicators
        """
        try:
            # Make a copy to avoid modifying the original data
            result = data.copy()

            # Process each requested indicator
            for indicator in indicators:
                indicator_type = indicator.get('type', '').lower()

                if indicator_type == 'sma':
                    result = self._add_sma(result, **indicator)
                elif indicator_type == 'ema':
                    result = self._add_ema(result, **indicator)
                elif indicator_type == 'macd':
                    result = self._add_macd(result, **indicator)
                elif indicator_type == 'bollinger':
                    result = self._add_bollinger_bands(result, **indicator)
                elif indicator_type == 'rsi':
                    result = self._add_rsi(result, **indicator)
                elif indicator_type == 'atr':
                    result = self._add_atr(result, **indicator)
                elif indicator_type == 'stochastic':
                    result = self._add_stochastic(result, **indicator)
                elif indicator_type == 'adx':
                    result = self._add_adx(result, **indicator)
                elif indicator_type == 'obv':
                    result = self._add_obv(result, **indicator)
                else:
                    self.logger.warning(f"Unknown indicator type: {indicator_type}")

            return result

        except Exception as e:
            self.logger.error(f"Error adding technical indicators: {str(e)}")
            raise DataProcessingError(f"Failed to add technical indicators: {str(e)}")

    def _add_sma(self, data: pd.DataFrame, period: int = 20, column: str = 'close', **kwargs) -> pd.DataFrame:
        """Add Simple Moving Average indicator"""
        if column in data.columns:
            data[f'sma_{period}'] = data[column].rolling(window=period, min_periods=1).mean()
        return data

    def _add_ema(self, data: pd.DataFrame, period: int = 20, column: str = 'close', **kwargs) -> pd.DataFrame:
        """Add Exponential Moving Average indicator"""
        if column in data.columns:
            data[f'ema_{period}'] = data[column].ewm(span=period, adjust=False).mean()
        return data

    def _add_macd(
        self,
        data: pd.DataFrame,
        fast_period: int = 12,
        slow_period: int = 26,
        signal_period: int = 9,
        column: str = 'close',
        **kwargs
    ) -> pd.DataFrame:
        """Add MACD (Moving Average Convergence Divergence) indicator"""
        if column in data.columns:
            # Calculate fast and slow EMAs
            fast_ema = data[column].ewm(span=fast_period, adjust=False).mean()
            slow_ema = data[column].ewm(span=slow_period, adjust=False).mean()

            # Calculate MACD line and signal line
            data['macd_line'] = fast_ema - slow_ema
            data['macd_signal'] = data['macd_line'].ewm(span=signal_period, adjust=False).mean()

            # Calculate MACD histogram
            data['macd_histogram'] = data['macd_line'] - data['macd_signal']

        return data

    def _add_bollinger_bands(
        self,
        data: pd.DataFrame,
        period: int = 20,
        std_dev: float = 2.0,
        column: str = 'close',
        **kwargs
    ) -> pd.DataFrame:
        """Add Bollinger Bands indicator"""
        if column in data.columns:
            # Calculate the middle band (simple moving average)
            data['bb_middle'] = data[column].rolling(window=period).mean()

            # Calculate the standard deviation
            rolling_std = data[column].rolling(window=period).std()

            # Calculate upper and lower bands
            data['bb_upper'] = data['bb_middle'] + (rolling_std * std_dev)
            data['bb_lower'] = data['bb_middle'] - (rolling_std * std_dev)

            # Calculate bandwidth and %B
            data['bb_bandwidth'] = (data['bb_upper'] - data['bb_lower']) / data['bb_middle']
            data['bb_percent_b'] = (data[column] - data['bb_lower']) / (data['bb_upper'] - data['bb_lower'])

        return data

    def _add_rsi(self, data: pd.DataFrame, period: int = 14, column: str = 'close', **kwargs) -> pd.DataFrame:
        """Add Relative Strength Index indicator"""
        if column in data.columns:
            # Calculate price change
            delta = data[column].diff()

            # Separate positive and negative price movements
            gain = delta.where(delta > 0, 0)
            loss = -delta.where(delta < 0, 0)

            # Calculate average gain and loss over specified period
            avg_gain = gain.rolling(window=period).mean()
            avg_loss = loss.rolling(window=period).mean()

            # Calculate RS (Relative Strength)
            rs = avg_gain / avg_loss

            # Calculate RSI
            data['rsi'] = 100 - (100 / (1 + rs))

        return data

    def _add_atr(self, data: pd.DataFrame, period: int = 14, **kwargs) -> pd.DataFrame:
        """Add Average True Range indicator"""
        if all(col in data.columns for col in ['high', 'low', 'close']):
            # Calculate True Range
            data['tr1'] = data['high'] - data['low']  # Current high - current low
            data['tr2'] = (data['high'] - data['close'].shift()).abs()  # Current high - previous close
            data['tr3'] = (data['low'] - data['close'].shift()).abs()  # Current low - previous close

            data['true_range'] = data[['tr1', 'tr2', 'tr3']].max(axis=1)

            # Calculate ATR (smoothed using EMA)
            data['atr'] = data['true_range'].ewm(span=period, adjust=False).mean()

            # Drop temporary columns
            data = data.drop(['tr1', 'tr2', 'tr3', 'true_range'], axis=1)

        return data

    def _add_stochastic(
        self,
        data: pd.DataFrame,
        k_period: int = 14,
        d_period: int = 3,
        **kwargs
    ) -> pd.DataFrame:
        """Add Stochastic Oscillator indicator"""
        if all(col in data.columns for col in ['high', 'low', 'close']):
            # Calculate %K
            # First find lowest low and highest high over k_period
            lowest_low = data['low'].rolling(window=k_period).min()
            highest_high = data['high'].rolling(window=k_period).max()

            # Calculate %K
            data['stoch_k'] = 100 * ((data['close'] - lowest_low) / (highest_high - lowest_low))

            # Calculate %D (the SMA of %K)
            data['stoch_d'] = data['stoch_k'].rolling(window=d_period).mean()

        return data

    def _add_adx(
        self,
        data: pd.DataFrame,
        period: int = 14,
        **kwargs
    ) -> pd.DataFrame:
        """Add Average Directional Index indicator"""
        if all(col in data.columns for col in ['high', 'low', 'close']):
            # Calculate +DM and -DM
            data['up_move'] = data['high'] - data['high'].shift(1)
            data['down_move'] = data['low'].shift(1) - data['low']

            data['plus_dm'] = np.where(
                (data['up_move'] > data['down_move']) & (data['up_move'] > 0),
                data['up_move'],
                0
            )

            data['minus_dm'] = np.where(
                (data['down_move'] > data['up_move']) & (data['down_move'] > 0),
                data['down_move'],
                0
            )

            # Calculate True Range
            data['tr1'] = data['high'] - data['low']
            data['tr2'] = (data['high'] - data['close'].shift(1)).abs()
            data['tr3'] = (data['low'] - data['close'].shift(1)).abs()
            data['true_range'] = data[['tr1', 'tr2', 'tr3']].max(axis=1)

            # Smooth with EMA
            data['smoothed_tr'] = data['true_range'].ewm(span=period, adjust=False).mean()
            data['smoothed_plus_dm'] = data['plus_dm'].ewm(span=period, adjust=False).mean()
            data['smoothed_minus_dm'] = data['minus_dm'].ewm(span=period, adjust=False).mean()

            # Calculate +DI and -DI
            data['plus_di'] = 100 * (data['smoothed_plus_dm'] / data['smoothed_tr'])
            data['minus_di'] = 100 * (data['smoothed_minus_dm'] / data['smoothed_tr'])

            # Calculate DX
            data['dx'] = 100 * (
                (data['plus_di'] - data['minus_di']).abs() /
                (data['plus_di'] + data['minus_di'])
            )

            # Calculate ADX
            data['adx'] = data['dx'].ewm(span=period, adjust=False).mean()

            # Drop temporary columns
            temp_cols = [
                'up_move', 'down_move', 'plus_dm', 'minus_dm',
                'tr1', 'tr2', 'tr3', 'true_range',
                'smoothed_tr', 'smoothed_plus_dm', 'smoothed_minus_dm'
            ]
            data = data.drop(temp_cols, axis=1)

        return data

    def _add_obv(self, data: pd.DataFrame, **kwargs) -> pd.DataFrame:
        """Add On-Balance Volume indicator"""
        if all(col in data.columns for col in ['close', 'volume']):
            # Calculate price direction
            price_direction = np.sign(data['close'].diff())

            # First OBV value
            obv = [0]

            # Calculate OBV values
            for i in range(1, len(data)):
                if price_direction[i] > 0:
                    obv.append(obv[-1] + data['volume'].iloc[i])
                elif price_direction[i] < 0:
                    obv.append(obv[-1] - data['volume'].iloc[i])
                else:
                    obv.append(obv[-1])

            # Add OBV to dataframe
            data['obv'] = obv

        return data
