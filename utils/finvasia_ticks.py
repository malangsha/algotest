import time
import json
import ast # For safe evaluation of dict strings
import threading 
import logging # Added for explicit logger type hint
from collections import defaultdict 
from typing import List, Dict, Any, Optional, Callable, Set, Tuple
from datetime import datetime

# Assuming your project structure allows these imports
# Adjust paths if your structure is different (e.g., using 'src.' prefix)

# --- Imports from your project structure (ensure these paths are correct) ---
# Models
from models.instrument import Instrument, AssetClass 
from models.events import MarketDataEvent, EventType

# Utils
from utils.constants import Exchange, InstrumentType, MarketDataType 
# from utils.config_loader import load_config # Not used in this script directly
# from utils.time_utils import get_current_datetime # REMOVED - Unused import causing error

# Core
from core.event_manager import EventManager 
from core.logging_manager import get_logger 

# The specific market data feed to be tested
from live.market_data_feeds import MarketDataFeed


# --- Minimal DataManager for Simulation ---
class MinimalDataManager:
    """
    A minimal DataManager for the tick player to function.
    It needs to store instruments and provide a way for MarketDataFeed
    to retrieve them for setting up its token-symbol maps.
    """
    def __init__(self, logger: logging.Logger): # Added type hint for logger
        self.logger = logger
        self.instruments: Dict[str, Instrument] = {} # instrument_id -> Instrument

    def add_instrument(self, instrument: Instrument):
        if instrument and instrument.instrument_id:
            self.instruments[instrument.instrument_id] = instrument
            self.logger.debug(f"Added instrument to MinimalDataManager: {instrument.instrument_id}")
        else:
            self.logger.warning(f"Could not add invalid instrument: {instrument}")

    def get_all_active_instruments(self) -> List[Instrument]:
        """Returns all instruments managed by DataManager."""
        return list(self.instruments.values())

    def get_instrument_by_symbol(self, symbol: str, exchange: Optional[Exchange] = None) -> Optional[Instrument]:
        """Retrieves an instrument by its trading symbol and optionally exchange."""
        for inst in self.instruments.values():
            if inst.symbol == symbol:
                if exchange is None or inst.exchange == exchange:
                    return inst
        return None
    
    def get_or_create_instrument(self, symbol_key: str, asset_class: Optional[AssetClass] = None) -> Optional[Instrument]:
        """
        Retrieves an existing instrument or creates a new one if not found.
        Example: symbol_key "NSE:RELIANCE"
        """
        if symbol_key in self.instruments:
            return self.instruments[symbol_key]
        
        parts = symbol_key.split(':')
        if len(parts) == 2:
            exchange_str, sym = parts[0], parts[1]
            try:
                exchange_enum = Exchange[exchange_str.upper()]
                inst = Instrument(
                    instrument_id=symbol_key,
                    symbol=sym,
                    exchange=exchange_enum, 
                    instrument_type=InstrumentType.EQUITY, 
                    asset_class=asset_class if asset_class else AssetClass.EQUITY, 
                    metadata={'symbol_key': symbol_key}
                )
                self.add_instrument(inst)
                return inst
            except KeyError:
                self.logger.error(f"Invalid exchange in symbol_key: {exchange_str}")
                return None
        self.logger.error(f"Invalid symbol_key format: {symbol_key}")
        return None


class TickFileParser:
    """Parses the Finvasia tick log file."""

    def __init__(self, filepath: str, logger: logging.Logger): # Added type hint for logger
        self.filepath = filepath
        self.logger = logger

    def extract_ticks_from_log_line(self, line: str) -> Optional[Dict[str, Any]]:
        """Extracts the tick dictionary from a single log line."""
        try:
            dict_start_index = line.find("tick: {")
            if dict_start_index == -1:
                return None
            
            dict_str = line[dict_start_index + 6:].strip()
            
            tick_dict = ast.literal_eval(dict_str)
            return tick_dict
        except Exception as e:
            self.logger.error(f"Error parsing tick from line: '{line}'. Error: {e}")
            return None

    def parse_ticks(self) -> List[Dict[str, Any]]:
        """Reads the entire file and extracts all tick dictionaries."""
        ticks = []
        self.logger.info(f"Starting to parse ticks from {self.filepath}")
        try:
            with open(self.filepath, 'r') as f:
                for line in f:
                    tick = self.extract_ticks_from_log_line(line)
                    if tick:
                        ticks.append(tick)
            self.logger.info(f"Successfully parsed {len(ticks)} ticks from the file.")
        except FileNotFoundError:
            self.logger.error(f"Tick file not found: {self.filepath}")
        except Exception as e:
            self.logger.error(f"Error reading tick file {self.filepath}: {e}")
        return ticks

    def get_unique_instrument_details(self, ticks: List[Dict[str, Any]]) -> Dict[Tuple[str, str], Dict[str, Any]]:
        """
        Extracts unique instrument details (token, symbol, exchange) from a list of ticks.
        Returns a dictionary keyed by (token, exchange_str) to ensure uniqueness.
        """
        instrument_details = {}
        for tick in ticks:
            token = tick.get('tk')
            exchange_str = tick.get('e')
            symbol = tick.get('ts') 
            
            if token and exchange_str and symbol:
                key = (token, exchange_str)
                if key not in instrument_details:
                    instrument_details[key] = {
                        'token': token,
                        'symbol': symbol,
                        'exchange_str': exchange_str,
                        'instrument_type': InstrumentType.OPTION if exchange_str in ["NFO", "BFO"] and ("CE" in symbol or "PE" in symbol) else InstrumentType.INDEX if exchange_str in ["NSE", "BSE"] and ("Nifty" in symbol or "SENSEX" in symbol or "NIFTY" in symbol.upper()) else InstrumentType.EQUITY
                    }
        self.logger.info(f"Extracted details for {len(instrument_details)} unique instruments.")
        return instrument_details


class TickFilePlayer:
    """
    Simulates feeding ticks from a file to a MarketDataFeed instance.
    """

    def __init__(self, 
                 ticks_to_play: List[Dict[str, Any]], 
                 feed_instance: MarketDataFeed, 
                 logger: logging.Logger, # Added type hint for logger
                 delay_between_ticks: float = 0.001): 
        self.ticks_to_play = ticks_to_play
        self.feed_instance = feed_instance
        self.logger = logger
        self.delay_between_ticks = delay_between_ticks
        self._stop_event = threading.Event()
        self._player_thread = None

    def start_simulation(self):
        """Starts the tick replay in a new thread."""
        if self._player_thread and self._player_thread.is_alive():
            self.logger.warning("Player thread already running.")
            return
            
        self._stop_event.clear()
        self._player_thread = threading.Thread(target=self._play_ticks, name="TickPlayerThread")
        self._player_thread.daemon = True 
        self._player_thread.start()
        self.logger.info("Tick player simulation thread started.")

    def _play_ticks(self):
        """Internal method to iterate and feed ticks."""
        self.logger.info(f"Starting tick simulation. Total ticks: {len(self.ticks_to_play)}. Delay: {self.delay_between_ticks}s")
        
        if not self.feed_instance:
            self.logger.error("MarketDataFeed instance not provided to TickFilePlayer.")
            return

        count = 0
        for tick_dict in self.ticks_to_play:
            if self._stop_event.is_set():
                self.logger.info("Tick player stopping simulation early due to stop event.")
                break
            try:
                self.feed_instance._on_market_data([tick_dict]) 
                count += 1
                if count % 1000 == 0: 
                    self.logger.debug(f"Simulated {count} ticks...")
                if self.delay_between_ticks > 0:
                    time.sleep(self.delay_between_ticks)
            except Exception as e:
                self.logger.error(f"Error feeding tick to market_data_feed: {tick_dict}. Error: {e}", exc_info=True)
        
        self.logger.info(f"Finished tick simulation. Processed {count} ticks.")

    def stop_simulation(self):
        """Stops the tick replay."""
        self.logger.info("Stopping tick player simulation...")
        self._stop_event.set()
        if self._player_thread and self._player_thread.is_alive():
            self._player_thread.join(timeout=5.0)
            if self._player_thread.is_alive():
                 self.logger.warning("Tick player thread did not stop gracefully.")
        self.logger.info("Tick player simulation stopped.")


# --- Main execution for demonstration ---
if __name__ == "__main__":
    main_logger = get_logger("TickPlayerMain") # Use your logging manager
    main_logger.info("Initializing Tick Player Simulation Environment...")

    TICK_FILE_PATH = "finvasia.ticks"
    SIMULATION_DELAY = 0.001 

    event_manager = EventManager()

    tick_parser = TickFileParser(TICK_FILE_PATH, main_logger)
    all_parsed_ticks = tick_parser.parse_ticks()
    
    if not all_parsed_ticks:
        main_logger.error("No ticks parsed from the file. Exiting simulation.")
        exit()
        
    unique_instrument_data = tick_parser.get_unique_instrument_details(all_parsed_ticks)

    data_manager = MinimalDataManager(logger=get_logger("SimDataManager"))
    constructor_instruments: List[Instrument] = []

    for (token, exch_str), details in unique_instrument_data.items():
        try:
            exchange_enum = Exchange[exch_str.upper()]
            instrument_id = f"{exch_str.upper()}:{details['symbol']}" 
            
            # Attempt to get lot_size and tick_size from the first tick for this token
            # This is a heuristic and might not always be accurate or available.
            # A more robust solution would be a separate contract master file.
            first_tick_for_token = next((t for t in all_parsed_ticks if t.get('tk') == token), None)
            lot_size = 1
            tick_size_val = 0.05 # Default tick size
            if first_tick_for_token:
                lot_size = float(first_tick_for_token.get('ls', 1))
                tick_size_val = float(first_tick_for_token.get('ti', 0.05))


            instrument = Instrument(
                instrument_id=instrument_id,
                symbol=details['symbol'],
                exchange=exchange_enum, 
                instrument_type=details['instrument_type'],
                asset_class=AssetClass.EQUITY if details['instrument_type'] == InstrumentType.EQUITY else AssetClass.INDEX if details['instrument_type'] == InstrumentType.INDEX else AssetClass.OPTIONS,
                metadata={'broker_token': token, 'symbol_key': instrument_id}, 
                lot_size = lot_size,
                tick_size = tick_size_val
            )
            data_manager.add_instrument(instrument)
            constructor_instruments.append(instrument)
        except KeyError as e:
            main_logger.error(f"Could not create instrument for token {token}, exch {exch_str} due to missing Exchange enum value: {e}")
        except Exception as e:
            main_logger.error(f"Error creating instrument for {details}: {e}")
            
    main_logger.info(f"Populated DataManager with {len(data_manager.instruments)} unique instruments.")

    processed_events = []
    def simulated_market_data_callback(event: MarketDataEvent):
        # dt_obj = datetime.fromtimestamp(event.timestamp)
        # time_str = dt_obj.strftime("%Y-%m-%d %H:%M:%S.") + f"{dt_obj.microsecond // 1000:03d}"
        # main_logger.info(f"CALLBACK: [{time_str}] Event for {event.instrument.instrument_id if event.instrument else 'N/A'}: LTP {event.data.get(MarketDataType.LAST_PRICE.value)}")
        processed_events.append(event)
        if len(processed_events) % 500 == 0: 
             main_logger.info(f"Callback processed {len(processed_events)} MarketDataEvents.")

    dummy_feed_config = {
        "user_id": "SIMULATOR", 
        "api_credentials": { 
            "user_id": "SIM_USER", "password": "SIM_PASSWORD", "totp_secret": "SIM_TOTP"
        },
        "websocket": {"url": "wss://dummy.finvasia.com/ws"},
        "fetch_all_symbols_on_startup": False, 
        "max_retries": 1, 
        "reconnect_delay": 5, 
        "heartbeat_interval": 30 
    }
    main_sim_config = {"live_config": {"finvasia": dummy_feed_config}}

    finvasia_feed = MarketDataFeed(
        event_manager=event_manager, 
        instruments=constructor_instruments, 
        config=main_sim_config,          
        callback=simulated_market_data_callback,
        user_id=dummy_feed_config["user_id"], 
        api=None 
    )

    try:
        main_logger.info("Setting up symbol-token maps in FinvasiaFeed...")
        finvasia_feed._setup_symbol_token_maps() # Call the method to populate maps
        main_logger.info("Symbol-token maps setup complete.")
    except Exception as e:
        main_logger.error(f"Error setting up symbol-token maps: {e}", exc_info=True)
        exit()

    tick_player = TickFilePlayer(
        ticks_to_play=all_parsed_ticks,
        feed_instance=finvasia_feed,
        logger=get_logger("TickPlayerInstance"), # Use your logging manager
        delay_between_ticks=SIMULATION_DELAY
    )

    main_logger.info("Starting tick simulation player...")
    tick_player.start_simulation()

    try:
        while tick_player._player_thread and tick_player._player_thread.is_alive():
            time.sleep(1) 
    except KeyboardInterrupt:
        main_logger.info("Main: KeyboardInterrupt received. Stopping player...")
    finally:
        tick_player.stop_simulation()
        main_logger.info(f"Simulation finished. Total MarketDataEvents processed by callback: {len(processed_events)}")
        main_logger.info("Tick Player Main: Exiting.")

