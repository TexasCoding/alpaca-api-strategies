import os
import pandas as pd

from alpaca.common.exceptions import APIError

from alpaca.data import StockHistoricalDataClient
from alpaca.trading.client import TradingClient
from alpaca.data.timeframe import TimeFrame
from alpaca.trading.requests import MarketOrderRequest, LimitOrderRequest
from alpaca.trading.enums import OrderSide, TimeInForce
from alpaca.data.requests import StockBarsRequest

from dotenv import load_dotenv
load_dotenv()

class AlpacaAPI:
    def __init__(self):
        self.api_key = os.getenv('APCA_API_KEY_ID')
        self.api_secret = os.getenv('APCA_API_SECRET_KEY')
        self.paper = os.getenv('APCA_PAPER')
        self.trade_client = TradingClient(api_key=self.api_key, secret_key=self.api_secret, paper=self.paper)
        self.data_client = StockHistoricalDataClient(api_key=self.api_key, secret_key=self.api_secret)

    def get_account(self):
        '''
        Get account information
        :return: dict: account information
        '''
        try:
            account = self.trade_client.get_account()
            return account
        except APIError as e:
            raise Exception(e)

    ############################
    # Get Historical Stock Data
    ############################
    def get_historical_data(self, symbols, start, end, timeframe='day'):
        '''
        Get historical data for a stock
        :param symbol: str: stock symbol
        :param start: str: start date in format 'YYYY-MM-DD'
        :param end: str: end date in format 'YYYY-MM-DD'
        :param timeframe: str: 'minute', 'hour', or 'day', default 'day'
        :return: DataFrame: historical stock data
        '''
        # Set timeframe
        match timeframe:
            case 'minute':
                timeframe = TimeFrame.Minute
            case 'hour':
                timeframe = TimeFrame.Hour
            case 'day':
                timeframe = TimeFrame.Day
            case 'week':
                timeframe = TimeFrame.Week
            case _:
                raise ValueError('Invalid timeframe. Must be "minute", "hour", "day" or "week"')
        # Get historical data, handle APIError
        try:
            # Create StockBarsRequest object
            bars = StockBarsRequest(
                symbol_or_symbols=symbols,
                start=start,
                end=end,
                timeframe=timeframe
            )
            # Get historical data and return as a DataFrame
            data = self.data_client.get_stock_bars(bars)

            data_df = data.df.reset_index()

            #data_df = data_df.sort_values(by=['timestamp'], ascending=False)
            
            # Drop columns that are not needed
            try:
                data_df.drop(columns=['trade_count', 'vwap'], inplace=True)
                # Reformat date column
                data_df['timestamp'] = data_df['timestamp'].dt.strftime('%Y-%m-%d %H:%M:%S')
                # # Convert date column to datetime
                data_df['timestamp'] = pd.to_datetime(data_df['timestamp'])
                
            except KeyError:
                pass
            # Rename columns for consistency
            data_df.rename(columns={'symbol': 'Symbol', 'timestamp': 'Date', 'open': 'Open', 'high': 'High', 'low': 'Low', 'close': 'Close', 'volume': 'Volume'}, inplace=True)
            return data_df
        # Handle APIError
        except APIError as e:
            raise Exception(e)

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
    # Get Asset Data
    ############################
    def get_asset(self, symbol):
        '''
        Get asset data from Alpaca API
        :param symbol: str: stock symbol
        :return: dict: asset data
        '''
        try:
            asset = self.trade_client.get_asset(symbol)
            return asset
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
        :return: DataFrame: current positions, including cash
        '''
        positions = self.trade_client.get_all_positions()
        # Create cash position
        cash = pd.DataFrame({
            'asset': 'Cash',
            'current_price': self.trade_client.get_account().cash,
            'qty': self.trade_client.get_account().cash,
            'market_value': self.trade_client.get_account().cash,
            'profit_dol': 0,
            'profit_pct': 0
        }, index=[0])
        
        if not positions:
            assets = cash
        else:
            # Create DataFrame of investments
            investments = pd.DataFrame({
                'asset': [x.symbol for x in positions],
                'current_price': [x.current_price for x in positions],
                'qty': [x.qty for x in positions],
                'market_value': [x.market_value for x in positions],
                'profit_dol': [x.unrealized_pl for x in positions],
                'profit_pct': [x.unrealized_plpc for x in positions]
            })
            # Concatenate investments and cash DataFrames
            assets = pd.concat([investments, cash], ignore_index=True)
            # Format DataFrames
            float_format = ['current_price', 'qty', 'market_value', 'profit_dol', 'profit_pct']
            string_format = ['asset']

            for col in float_format:
                assets[col] = assets[col].astype(float)
            for col in string_format:
                assets[col] = assets[col].astype(str)

            round_2 = ['market_value', 'profit_dol']
            round_4 = ['profit_pct']

            assets[round_2] = assets[round_2].apply(lambda x: pd.Series.round(x, 2))
            assets[round_4] = assets[round_4].apply(lambda x: pd.Series.round(x, 4))

            asset_sum = assets['market_value'].sum()
            assets['portfolio_pct'] = assets['market_value'] / asset_sum

        return assets



    