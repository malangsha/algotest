"""
AlgoTrading - A modular algorithmic trading framework for Indian markets

This package provides tools for algorithmic trading, backtesting, and
live trading with support for various Indian brokers including Zerodha,
Upstox, Angel Broking, and others.

Features:
- Strategy development and backtesting
- Live trading with multiple broker support
- Advanced risk management
- Portfolio optimization
- Real-time data processing and event handling
- Web dashboard and API for monitoring and control
"""

__version__ = '0.1.0'
__author__ = 'Your Name'

from pathlib import Path

# Package root directory
ROOT_DIR = Path(__file__).parent.absolute()
CONFIG_DIR = ROOT_DIR/'config'
