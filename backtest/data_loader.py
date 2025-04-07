from typing import Dict, List, Optional, Union, Any
import pandas as pd
import numpy as np
import datetime
import os
import logging
import yfinance as yf
from pathlib import Path

from models.instrument import Instrument
from utils.exceptions import DataLoadingError

class DataLoader:
    """
    Class responsible for loading historical market data from various sources
    for backtesting purposes.
    """

    def __init__(self, config: Dict[str, Any]):
        """
        Initialize the data loader with configuration.

        Args:
            config: Configuration dictionary containing data source settings
        """
        self.config = config
        self.logger = logging.getLogger('backtest.data_loader')
        self.data_cache = {}

        # Data sources settings
        self.default_source = self.config.get('default_source', 'csv')
        self.csv_directory = self.config.get('csv_directory', 'data/historical')
        self.use_cache = self.config.get('use_cache', True)

        # Ensure data directory exists
        if self.default_source == 'csv':
            Path(self.csv_directory).mkdir(parents=True, exist_ok=True)

    def load_data(
        self,
        instrument: Instrument,
        start_date: Union[str, datetime.datetime],
        end_date: Union[str, datetime.datetime],
        source: Optional[str] = None
    ) -> pd.DataFrame:
        """
        Load historical data for an instrument within the specified date range.

        Args:
            instrument: The financial instrument to load data for
            start_date: Start date for historical data
            end_date: End date for historical data
            source: Data source to use (defaults to configured default_source)

        Returns:
            DataFrame containing historical OHLCV data
        """
        # Convert dates to datetime if they are strings
        if isinstance(start_date, str):
            start_date = pd.to_datetime(start_date)
        if isinstance(end_date, str):
            end_date = pd.to_datetime(end_date)

        # Check cache first if enabled
        cache_key = f"{instrument.symbol}_{start_date}_{end_date}"
        if self.use_cache and cache_key in self.data_cache:
            self.logger.info(f"Using cached data for {instrument.symbol}")
            return self.data_cache[cache_key]

        # Use specified source or default
        source = source or self.default_source

        try:
            # Load data from appropriate source
            if source == 'csv':
                data = self._load_from_csv(instrument, start_date, end_date)
            elif source == 'yfinance':
                data = self._load_from_yfinance(instrument, start_date, end_date)
            elif source == 'nse':
                data = self._load_from_nse(instrument, start_date, end_date)
            elif source == 'api':
                data = self._load_from_api(instrument, start_date, end_date)
            else:
                raise ValueError(f"Unsupported data source: {source}")

            # Validate data
            if data.empty:
                raise DataLoadingError(f"No data found for {instrument.symbol}")

            # Ensure data has expected columns
            required_columns = ['open', 'high', 'low', 'close']
            for col in required_columns:
                if col not in data.columns:
                    raise DataLoadingError(f"Required column {col} missing from data")

            # Make sure all required columns are numeric
            for col in required_columns:
                data[col] = pd.to_numeric(data[col], errors='coerce')

            # Add volume column with 0s if not present
            if 'volume' not in data.columns:
                data['volume'] = 0

            # Convert volume to numeric
            data['volume'] = pd.to_numeric(data['volume'], errors='coerce').fillna(0)

            # Ensure index is datetime and sorted
            if not isinstance(data.index, pd.DatetimeIndex):
                data.index = pd.to_datetime(data.index)

            data = data.sort_index()

            # Filter by date range
            data = data[(data.index >= start_date) & (data.index <= end_date)]

            # Cache data if caching is enabled
            if self.use_cache:
                self.data_cache[cache_key] = data

            self.logger.info(f"Loaded {len(data)} records for {instrument.symbol}")
            return data

        except Exception as e:
            self.logger.error(f"Error loading data for {instrument.symbol}: {str(e)}")
            raise DataLoadingError(f"Failed to load data for {instrument.symbol}: {str(e)}")

    def _load_from_csv(
        self,
        instrument: Instrument,
        start_date: datetime.datetime,
        end_date: datetime.datetime
    ) -> pd.DataFrame:
        """Load historical data from CSV files"""
        # Construct filename with symbol and exchange
        filename = f"{instrument.symbol}"
        if instrument.exchange:
            filename += f"_{instrument.exchange}"

        # Check for different file extensions
        extensions = ['.csv', '.CSV']
        file_path = None

        for ext in extensions:
            temp_path = os.path.join(self.csv_directory, filename + ext)
            if os.path.exists(temp_path):
                file_path = temp_path
                break

        if file_path is None:
            raise FileNotFoundError(f"CSV file for {instrument.symbol} not found in {self.csv_directory}")

        # Load data
        try:
            # First, determine the date format in the CSV
            with open(file_path, 'r') as f:
                header = f.readline()
                first_line = f.readline()

            # Try to infer date column
            date_col = None
            columns = header.strip().split(',')

            date_column_names = ['date', 'timestamp', 'datetime', 'time', 'Date', 'Timestamp', 'DateTime', 'Time']
            for i, col_name in enumerate(columns):
                if col_name in date_column_names:
                    date_col = i
                    break

            # If we couldn't find date column by name, check if first column contains date-like data
            if date_col is None:
                first_row = first_line.strip().split(',')
                # Try to parse first column as date
                try:
                    pd.to_datetime(first_row[0])
                    date_col = 0
                except:
                    pass

            # If still can't determine date column, default to first column
            if date_col is None:
                date_col = 0

            # Load data with pandas, using the identified date column as index
            data = pd.read_csv(
                file_path,
                index_col=date_col,
                parse_dates=True,
                infer_datetime_format=True
            )

            # Standardize column names to lowercase
            data.columns = [col.lower() for col in data.columns]

            # Map common column names to standard names
            column_mapping = {
                'o': 'open',
                'h': 'high',
                'l': 'low',
                'c': 'close',
                'v': 'volume',
                'adj close': 'adj_close',
                'adjusted close': 'adj_close'
            }

            data = data.rename(columns=column_mapping)

            return data

        except Exception as e:
            raise DataLoadingError(f"Error parsing CSV file for {instrument.symbol}: {str(e)}")

    def _load_from_yfinance(
        self,
        instrument: Instrument,
        start_date: datetime.datetime,
        end_date: datetime.datetime
    ) -> pd.DataFrame:
        """Load historical data from Yahoo Finance"""
        try:
            # Adjust symbol format for Yahoo Finance (e.g., add .NS for NSE)
            symbol = instrument.symbol
            if instrument.exchange == 'NSE':
                symbol = f"{symbol}.NS"
            elif instrument.exchange == 'BSE':
                symbol = f"{symbol}.BO"

            # Download data from Yahoo Finance
            data = yf.download(
                symbol,
                start=start_date,
                end=end_date,
                progress=False
            )

            # Standardize column names
            data.columns = [col.lower() for col in data.columns]

            # Rename columns to match our standard format
            column_mapping = {
                'adj close': 'adj_close'
            }
            data = data.rename(columns=column_mapping)

            return data

        except Exception as e:
            raise DataLoadingError(f"Error fetching data from Yahoo Finance for {instrument.symbol}: {str(e)}")

    def _load_from_nse(
        self,
        instrument: Instrument,
        start_date: datetime.datetime,
        end_date: datetime.datetime
    ) -> pd.DataFrame:
        """
        Load historical data from NSE India

        Note: This method requires external libraries for NSE data access
        or can be implemented with direct API calls
        """
        try:
            # This is a placeholder for NSE implementation
            # You would need to use a specialized library or API for NSE data

            # Example using a hypothetical NSE data library
            # from nse_india import get_history
            # data = get_history(symbol=instrument.symbol, start=start_date, end=end_date)

            # As a fallback, try using Yahoo Finance with .NS suffix
            symbol = f"{instrument.symbol}.NS"
            data = yf.download(
                symbol,
                start=start_date,
                end=end_date,
                progress=False
            )

            # Standardize column names
            data.columns = [col.lower() for col in data.columns]

            # Rename columns to match our standard format
            column_mapping = {
                'adj close': 'adj_close'
            }
            data = data.rename(columns=column_mapping)

            return data

        except Exception as e:
            raise DataLoadingError(f"Error fetching data from NSE for {instrument.symbol}: {str(e)}")

    def _load_from_api(
        self,
        instrument: Instrument,
        start_date: datetime.datetime,
        end_date: datetime.datetime
    ) -> pd.DataFrame:
        """Load historical data from a configured API endpoint"""
        try:
            import requests

            # Get API configuration
            api_config = self.config.get('api', {})
            base_url = api_config.get('base_url')
            api_key = api_config.get('api_key')

            if not base_url:
                raise ValueError("API base URL not configured")

            # Construct request parameters
            params = {
                'symbol': instrument.symbol,
                'exchange': instrument.exchange,
                'start_date': start_date.strftime('%Y-%m-%d'),
                'end_date': end_date.strftime('%Y-%m-%d')
            }

            # Add API key if provided
            if api_key:
                params['apikey'] = api_key

            # Make request
            response = requests.get(base_url, params=params)
            response.raise_for_status()

            # Parse JSON response
            data_json = response.json()

            # Convert to DataFrame
            data = pd.DataFrame(data_json)

            # Set date as index
            data['date'] = pd.to_datetime(data['date'])
            data = data.set_index('date')

            # Ensure lowercase column names
            data.columns = [col.lower() for col in data.columns]

            return data

        except Exception as e:
            raise DataLoadingError(f"Error fetching data from API for {instrument.symbol}: {str(e)}")

    def bulk_load_data(
        self,
        instruments: List[Instrument],
        start_date: Union[str, datetime.datetime],
        end_date: Union[str, datetime.datetime],
        source: Optional[str] = None
    ) -> Dict[str, pd.DataFrame]:
        """
        Load historical data for multiple instruments at once.

        Args:
            instruments: List of financial instruments to load data for
            start_date: Start date for historical data
            end_date: End date for historical data
            source: Data source to use (defaults to configured default_source)

        Returns:
            Dictionary mapping instrument symbols to their historical data DataFrames
        """
        result = {}

        for instrument in instruments:
            try:
                data = self.load_data(instrument, start_date, end_date, source)
                result[instrument.symbol] = data
            except Exception as e:
                self.logger.error(f"Failed to load data for {instrument.symbol}: {str(e)}")
                # Skip this instrument but continue with others
                continue

        return result

    def clear_cache(self) -> None:
        """Clear the data cache"""
        self.data_cache = {}
        self.logger.info("Data cache cleared")
