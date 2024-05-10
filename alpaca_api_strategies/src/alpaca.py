import os
import pandas as pd
import requests
import json

from datetime import datetime
from datetime import timedelta
from pytz import timezone

tz = timezone('US/Eastern')
ctime = datetime.now(tz)
previous_day = (ctime - timedelta(days=1)).strftime("%Y-%m-%d")
year_ago = (ctime - timedelta(days=365)).strftime("%Y-%m-%d")

from alpaca.common.exceptions import APIError
from alpaca.data import StockHistoricalDataClient
from alpaca.trading.client import TradingClient
from alpaca.data.timeframe import TimeFrame
from alpaca.trading.requests import MarketOrderRequest, LimitOrderRequest
from alpaca.trading.enums import OrderSide, TimeInForce
from alpaca.data.enums import DataFeed
from alpaca.data.requests import StockSnapshotRequest

from dotenv import load_dotenv
load_dotenv()

class AlpacaAPI:
    def __init__(self):
        self.api_key = os.getenv('APCA_API_KEY_ID')
        self.api_secret = os.getenv('APCA_API_SECRET_KEY')
        self.paper = bool(os.getenv('APCA_PAPER'))
        
        if self.paper == True:
            self.trade_url = 'https://paper-api.alpaca.markets/v2/'
        else:
            self.trade_url = 'https://api.alpaca.markets/v2/'
            
        self.stock_url = 'https://data.alpaca.markets/v2/stocks/'
        self.crypto_url = 'https://data.alpaca.markets/v1beta3/'

        self.headers = {
            'APCA-API-KEY-ID': self.api_key,
            'APCA-API-SECRET-KEY': self.api_secret,
            'accept': 'application/json'
        }

        self.trade_client = TradingClient(api_key=self.api_key, secret_key=self.api_secret, paper=self.paper)
        self.data_client = StockHistoricalDataClient(api_key=self.api_key, secret_key=self.api_secret)

    # ############################
    # # Get Stock Snapshot
    # ############################
    # def get_stock_snapshot(self, symbol, feed='iex', currency='USD'):
    #     '''
    #     Get stock snapshot
    #     :param symbol: str: stock symbol
    #     :param feed: str: 'iex' or 'sip', default 'iex'
    #     :param currency: str: 'usd' or 'cad', default 'usd'
    #     '''
    #     match feed:
    #         case 'iex':
    #             feed = DataFeed.IEX
    #         case 'sip':
    #             feed = DataFeed.SIP
    #         case 'otc':
    #             feed = DataFeed.OTC
    #         case _:
    #             raise ValueError('Invalid feed. Must be "iex" or "sip"')
        
    #     try:
    #         params = StockSnapshotRequest(
    #             symbol_or_symbols=symbol,
    #             feed=feed,
    #             currency=currency
    #         )
    #         snapshot = self.data_client.get_stock_snapshot(params)
    #         return snapshot[symbol]
    #     except APIError as e:
    #         raise Exception(e)

    ############################
    # Get Account Information
    ############################
    def get_account(self):
        '''
        Get account information from Alpaca API
        :return: dict: account information
        dict keys: 'id', 'admin_configurations', 'user_configurations', 'account_number', 'status', 'crypto_status', 'options_approved_level', 'options_trading_level', 'currency'
                   'buying_power', 'regt_buying_power', 'daytrading_buying_power', 'effective_buying_power', 'non_marginable_buying_power', 'options_buying_power', 'bod_dtbp', 'cash' 
                   'accrued_fees', 'pending_transfer_in', 'portfolio_value', 'pattern_day_trader', 'trading_blocked', 'transfers_blocked', 'account_blocked', 'created_at' 
                   'trade_suspended_by_user', 'multiplier', 'shorting_enabled', 'equity', 'last_equity', 'long_market_value', 'short_market_value', 'position_market_value', 'initial_margin'
                    'maintenance_margin', 'last_maintenance_margin', 'sma', 'daytrade_count', 'balance_asof', 'crypto_tier', 'intraday_adjustments', 'pending_reg_taf_fees'
        '''        
        url = f'{self.trade_url}account'
        
        response = requests.get(url, headers=self.headers)
        if response.status_code == 200:
            res_dict = json.loads(response.text)
            return dict(res_dict)
        else:
            raise Exception(response.text)
    
    ############################
    # Get Stock Historical Data
    ############################
    def get_historical_data(self, symbol, start=year_ago, end=previous_day, timeframe='1d', currency='USD', limit=1000, adjustment='raw', feed='iex', sort='asc'):
        '''
        Get historical stock data from Alpaca API for a given stock symbol
        :param symbol: str: stock symbol
        :param start: str: start date in format 'YYYY-MM-DD', default one year ago
        :param end: str: end date in format 'YYYY-MM-DD', default previous day
        :param timeframe: str: '1m', '5m', '15m', '30m', '1h', '4h', '1d', '1w', or '1m', default '1d'
        :param currency: str: 'usd' or 'cad', default 'usd'
        :param limit: int: number of data points, default 1000
        :param adjustment: str: 'raw' or 'split', default 'raw'
        :param feed: str: 'iex' or 'sip', default 'iex'
        :param sort: str: 'asc' or 'desc', default 'asc'
        :return: DataFrame: historical stock data
        '''
        try:
            asset = self.get_asset(symbol)
        except Exception as e:
            raise Exception(e)
        else:
            if asset['class'] != 'us_equity':
                raise Exception(f'{symbol} is not a Stock.')
        
        # URL for historical stock data request
        url = f'{self.stock_url}{symbol}/bars'
        # Set timeframe
        match timeframe:
            case '1m':
                timeframe = '1Min'
            case '5m':
                timeframe = '5Min'
            case '15m':
                timeframe = '15Min'
            case '30m':
                timeframe = '30Min'
            case '1h':
                timeframe = '1Hour'
            case '4h':
                timeframe = '4Hour'
            case '1d':
                timeframe = '1Day'
            case '1w':
                timeframe = '1Week'
            case '1m':
                timeframe = '1Month'
            case _:
                raise ValueError('Invalid timeframe. Must be "1m", "5m", "15m", "30m", "1h", "4h", "1d", "1w", or "1m"')

        # Parameters for historical stock data request
        params = {
            'timeframe': timeframe,
            'start': start,
            'end': end,
            'currency': currency,
            'limit': limit,
            'adjustment': adjustment,
            'feed': feed,
            'sort': sort,
        }
        # Get historical stock data from Alpaca API
        response = requests.get(url, headers=self.headers, params=params)
        # Check if response is successful
        if response.status_code != 200:
            # Raise exception if response is not successful
            raise Exception(json.loads(response.text)['message'])
        
        res_json = json.loads(response.text)['bars']

        if not res_json:
            raise Exception(f'No data available for the requested symbol.')
        
        # Normalize JSON response and convert to DataFrame
        bar_data_df = pd.json_normalize(res_json)
        # Add symbol column to DataFrame
        bar_data_df.insert(0, 'symbol', symbol)
        # Reformat date column
        bar_data_df['t'] = pd.to_datetime(bar_data_df['t'].replace('[A-Za-z]', ' ', regex=True))
        # Drop columns that are not needed
        bar_data_df.drop(columns=['n', 'vw'], inplace=True)
        # Rename columns for consistency
        bar_data_df.rename(columns={'t': 'date', 'o': 'open', 'h': 'high', 'l': 'low', 'c': 'close', 'v': 'volume'}, inplace=True)
        # Convert columns to appropriate data types
        bar_data_df = bar_data_df.astype({'open': 'float', 'high': 'float', 'low': 'float', 'close': 'float', 'volume': 'int', 'symbol': 'str', 'date': 'datetime64[ns]'})
        # Return historical stock data as a DataFrame
        return bar_data_df

    ############################
    # Get Stock Asset
    ############################
    def get_asset(self, symbol):
        '''
        Get stock asset data from Alpaca API
        :param symbol: str: stock symbol
        :return: dict: stock asset data
        :keys: 'id', 'class', 'exchange', 'symbol', 'name', 'status', 'tradable', 'marginable', 'shortable', 'easy_to_borrow', 'fractionable'
        '''
        url = f'{self.trade_url}assets/{symbol}'

        response = requests.get(url, headers=self.headers)
        if response.status_code == 200:
            res_dict = json.loads(response.text)    
            return dict(res_dict)
        else:
            raise Exception(json.loads(response.text)['message'])

    ############################
    # Close Position on a single stock
    ############################
    def liquidate_position(self, symbol, qty=None):
        '''
        Close a position
        :param symbol: str: stock symbol
        :return: dict: close response
        '''
        url = f'{self.trade_url}positions/{symbol}'
        if qty:
            params = {'qty': qty}
        else:
            params = {}
        
        response = requests.delete(url, headers=self.headers, params=params)

        if response.status_code != 200:
            raise Exception(response.text)
        
        return json.loads(response.text)

        # try:
        #     # Close position and return response from Alpaca API
        #     return self.trade_client.close_position(symbol_or_asset_id=symbol)
        # # Handle APIError
        # except APIError as e:
        #     raise Exception(e)

    ############################
    # Submit Market Order
    ############################
    def market_order(self, symbol, qty=None, notional=None, side='buy', time_in_force='day'):
        '''
        Submit a market order
        :param symbol: str: stock symbol
        :param qty: int: number of shares to buy or sell
        :param notional: float: total value of shares to buy or sell
        :param side: str: 'buy' or 'sell', default 'buy'
        :param time_in_force: str: 'day' or 'gtc', default 'day'
        :return: dict: order response
        '''
        try:
            # Create MarketOrderRequest object
            order = MarketOrderRequest(
                symbol=symbol,
                notional=round(notional, 2) if notional else None,
                qty=qty if qty else None,
                side=OrderSide.BUY if side == 'buy' else OrderSide.SELL,
                time_in_force=TimeInForce.DAY if time_in_force == 'day' else TimeInForce.GTC
            )
            # Submit order and return response from Alpaca API
            return self.trade_client.submit_order(order)
        # Handle APIError
        except APIError as e:
            raise Exception(e)
    
    ############################
    # Submit Limit Order
    ############################
    def limit_order(self, symbol, limit_price=None, notional=None, side='buy', time_in_force='day'):
        '''
        Submit a limit order
        :param symbol: str: stock symbol
        :param limit_price: float: limit price for order
        :param notional: float: total value of shares to buy or sell
        :param side: str: 'buy' or 'sell', default 'buy'
        :param time_in_force: str: 'day' or 'gtc', default 'day'
        :return: dict: order response
        '''
        try:
            order = LimitOrderRequest(
                symbol=symbol,
                limit_price=round(limit_price, 2) if limit_price else None,
                notional=round(notional, 2) if notional else None,
                side=OrderSide.BUY if side == 'buy' else TimeInForce.GTC,
                time_in_force=TimeInForce.DAY if time_in_force == 'day' else TimeInForce.GTC
            )
            # Submit order and return response from Alpaca API
            return self.trade_client.submit_order(order)
        # Handle APIError
        except APIError as e:
            raise Exception(e)

    ############################
    # Get a Position
    ############################
    def get_position(self, symbol_or_id):
        '''
        Get a position from Alpaca API
        :param symbol_or_id: str: stock symbol or asset ID
        :return: dict: position data
        '''
        try:
            # Get position by symbol or asset ID
            position = self.trade_client.get_open_position(symbol_or_asset_id=symbol_or_id)
            return position
        # Handle APIErrorS
        except APIError as e:
            raise Exception(e)

    ############################
    # Close all Positions
    ############################
    def close_all_positions(self, cancel_orders=False):
        '''
        Close all positions
        :param cancel_orders: bool: cancel open orders, default False
        :return: dict: close response
        '''
        try:
            # Call close_all_positions method from TradingClient class
            close = self.trade_client.close_all_positions(cancel_orders=cancel_orders)
            return close
        # Handle APIError
        except APIError as e:
            raise Exception(e)
        
    ############################
    # Check If Market Open
    ############################    
    def market_open(self):
        '''
        Check if the market is open
        :return: bool: market open status, True if open, False if closed
        '''
        try:
            clock = self.trade_client.get_clock()
            return clock.is_open
        except APIError as e:
            raise Exception(e)
    
    ############################
    # Get Current Positions
    ############################
    def get_current_positions(self):
        '''
        Get current positions from Alpaca API
        :return: DataFrame: current positions
        '''
        # URL for current positions request
        url = f'{self.trade_url}positions'
        # Get current positions from Alpaca API
        response = requests.get(url, headers=self.headers)
        # Check if response is successful
        if response.status_code != 200:
            # Raise exception if response is not successful
            raise Exception(response.text)
        # Normalize JSON response and convert to DataFrame    
        pos_data_df = pd.json_normalize(json.loads(response.text))

        if pos_data_df.empty:
            return pos_data_df
        
        # Drop columns that are not needed
        pos_data_df.drop(columns=['exchange', 'asset_class', 'asset_marginable', 'lastday_price', 'change_today', 'qty', 'asset_id', 'cost_basis', 'unrealized_intraday_pl', 'unrealized_intraday_plpc', 'avg_entry_price'], inplace=True)
        # Set data types for DataFrame columns
        pos_data_df = pos_data_df.astype({'symbol': 'str', 'side': 'str', 'market_value': 'float', 'unrealized_pl': 'float', 'unrealized_plpc': 'float', 'current_price': 'float', 'qty_available': 'float'})
        # Rename columns for consistency
        pos_data_df.rename(columns={'symbol': 'asset', 'unrealized_pl': 'profit_dol', 'unrealized_plpc': 'profit_pct', 'qty_available': 'qty'}, inplace=True)
        # Calculate portfolio percentage
        asset_sum = pos_data_df['market_value'].sum()
        pos_data_df['portfolio_pct'] = pos_data_df['market_value'] / asset_sum
        # Convert portfolio percentage to float
        pos_data_df = pos_data_df.astype({'portfolio_pct': 'float'})

        round_2 = ['market_value', 'profit_dol']
        round_4 = ['profit_pct']
        # Round columns to appropriate decimal places
        pos_data_df[round_2] = pos_data_df[round_2].apply(lambda x: pd.Series.round(x, 2))
        pos_data_df[round_4] = pos_data_df[round_4].apply(lambda x: pd.Series.round(x, 4))
        # Return current positions as a DataFrame
        return pos_data_df


    