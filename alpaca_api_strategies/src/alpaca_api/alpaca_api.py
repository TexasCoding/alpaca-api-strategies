import os
import pandas as pd
import requests
import json

from src.alpaca_api.data_classes import AccountClass, AssetClass, MarketClockClass, MarketOrderClass

from dataclasses import dataclass

from datetime import datetime
from datetime import timedelta
from pytz import timezone

tz = timezone('US/Eastern')
ctime = datetime.now(tz)
previous_day = (ctime - timedelta(days=1)).strftime("%Y-%m-%d")
year_ago = (ctime - timedelta(days=365)).strftime("%Y-%m-%d")

from dotenv import load_dotenv
load_dotenv()

class AlpacaAPI:
    def __init__(self):
        self.api_key = os.getenv('APCA_API_KEY_ID')
        self.api_secret = os.getenv('APCA_API_SECRET_KEY')
        self.paper = os.getenv('APCA_PAPER')
        
        if self.paper == 'True':
            self.trade_url = 'https://paper-api.alpaca.markets/v2/'
        else:
            self.trade_url = 'https://api.alpaca.markets/v2/'
            
        self.stock_url = 'https://data.alpaca.markets/v2/stocks/'

        self.headers = {
            'APCA-API-KEY-ID': self.api_key,
            'APCA-API-SECRET-KEY': self.api_secret,
            'accept': 'application/json'
        }
    
    ############################
    # Get Market Status
    ############################
    def market_status(self):
        '''
        Get market status from Alpaca API
        :return: MarketClockClass: market status, see data_classes.py for details
        :raises Exception: if response is not successful
        '''
        # URL for market status request
        url = f'{self.trade_url}clock'
        # Get market status from Alpaca API
        response = requests.get(url, headers=self.headers)
        res = json.loads(response.text)
        # Check if response is successful
        if response.status_code != 200:
            # Raise exception if response is not successful
            raise Exception(res['message'])
        # Return market status as a MarketClockClass object
        return MarketClockClass(
            timestamp=res['timestamp'].split('.')[0].replace('T', ' '),
            is_open=res['is_open'],
            next_open=res['next_open'].split('.')[0].replace('T', ' ').replace('-04:00', ''),
            next_close=res['next_close'].split('.')[0].replace('T', ' ').replace('-04:00', '')
        )   

    ############################
    # Get Stock Historical Data
    ############################
    def get_stock_historical_data(self, symbol, start=year_ago, end=previous_day, timeframe='1d', currency='USD', limit=1000, feed='iex', sort='asc', adjustment='raw'):
        '''
        Get historical stock data from Alpaca API for a given stock symbol
        :param symbol: str: stock symbol
        :param start: str: start date in format 'YYYY-MM-DD', default one year ago
        :param end: str: end date in format 'YYYY-MM-DD', default previous day
        :param timeframe: str: '1m', '5m', '15m', '30m', '1h', '4h', '1d', '1w', or '1m', default '1d'
        :param currency: str: 'usd' or 'cad', default 'usd'
        :param limit: int: number of data points, default 1000
        :param feed: str: 'iex' or 'sip', default 'sip' must have SIP subscription for current day data. SIP works for previous day data
        :param sort: str: 'asc' or 'desc', default 'asc'
        :param adjustment: str: 'raw', 'split', or 'dividend', default 'raw'
        :return: DataFrame: historical stock data for the given symbol
        :raises ValueError: if symbol is not a valid symbol
        :raises ValueError: if not a us_equity asset 
        :raises ValueError: if timeframe is not '1m', '5m', '15m', '30m', '1h', '4h', '1d', '1w', or '1m'
        :raises ValueError: if no data is available for the given symbol
        :raises Exception: if response is not successful
        '''
        try:
            asset = self.get_asset(symbol)
        except Exception as e:
            raise ValueError(f'Error getting asset: {e}')
        else:
            if asset.class_type != 'us_equity':
                raise ValueError(f'{symbol} is not a stock.')
        
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
            raise ValueError(f'No data available for {symbol}.')
        
        # Normalize JSON response and convert to DataFrame
        bar_data_df = pd.json_normalize(res_json)
        # Add symbol column to DataFrame
        bar_data_df.insert(0, 'symbol', symbol)
        # Reformat date column
        bar_data_df['t'] = pd.to_datetime(bar_data_df['t'].replace('[A-Za-z]', ' ', regex=True))
        # Drop columns that are not needed
        #bar_data_df.drop(columns=['n', 'vw'], inplace=True)
        # Rename columns for consistency
        bar_data_df.rename(columns={'t': 'date', 'o': 'open', 'h': 'high', 'l': 'low', 'c': 'close', 'v': 'volume', 'n': 'trade_count', 'vw': 'vwap'}, inplace=True)
        # Convert columns to appropriate data types
        bar_data_df = bar_data_df.astype({'open': 'float', 'high': 'float', 'low': 'float', 'close': 'float', 'volume': 'int', 'symbol': 'str', 'date': 'datetime64[ns]', 'trade_count': 'int', 'vwap': 'float'})
        # Return historical stock data as a DataFrame
        return bar_data_df

    ############################
    # Get Stock Asset
    ############################
    def get_asset(self, symbol):
        '''
        Get asset information from Alpaca API for a given stock symbol
        :param symbol: str: stock symbol
        :return: AssetClass: asset information, see data_classes.py for details
        :raises Exception: if response is not successful
        '''
        url = f'{self.trade_url}assets/{symbol}'

        response = requests.get(url, headers=self.headers)
        if response.status_code == 200:
            res = json.loads(response.text)    
            return AssetClass(
                id=res['id'],
                class_type=res['class'],
                easy_to_borrow=res['easy_to_borrow'],
                exchange=res['exchange'],
                fractionable=res['fractionable'],
                maintenance_margin_requirement=res['maintenance_margin_requirement'],
                marginable=res['marginable'],
                name=res['name'],
                shortable=res['shortable'],
                status=res['status'],
                symbol=res['symbol'],
                tradable=res['tradable']
            )
        else:
            raise Exception(json.loads(response.text)['message'])

    ############################
    # Market Order
    ############################
    def market_order(self, symbol, qty=None, notional=None, side='buy', time_in_force='day', extended_hours=False):
        '''
        Place a market order
        :param symbol: str: stock symbol
        :param qty: int: number of shares to buy or sell
        :param notional: float: dollar amount to buy or sell
        :param side: str: 'buy' or 'sell', default 'buy'
        :param time_in_force: str: 'day', 'gtc', 'opg', 'ioc', 'fok', default 'day'
        :param extended_hours: bool: True or False, default False
        :return: dict: order response
        :raises ValueError: if qty or notional is not provided
        :raises ValueError: if both qty and notional are provided
        :raises Exception: if response is not successful
        '''
        try:
            asset = self.get_asset(symbol)
        except Exception as e:
            raise ValueError(f'Error getting asset: {e}')
        
        if not asset.tradable:
            raise ValueError(f'{symbol} is not tradable.')
        
        # Check if qty or notional is provided has to be one or the other
        if qty is None and notional is None:
            raise ValueError('Must provide either qty or notional.')
        # Check if qty and notional are both provided has to be one or the other
        elif qty is not None and notional is not None:
            raise ValueError('Must provide only one of qty or notional.')
        # URL for order request
        url = f'{self.trade_url}orders'
        # Data for order request
        data = {
            'symbol': symbol,
            'qty': qty if qty else None,
            'notional': round(notional, 2) if notional else None,
            'side': side if side =='buy' else 'sell',
            'type': 'market',
            'time_in_force': time_in_force,
            'extended_hours': extended_hours
        }
        # Post order request to Alpaca API
        response = requests.post(url, headers=self.headers, json=data)
        # Check if response is successful
        if response.status_code == 200:
            # Return order response
            res = json.loads(response.text)
            return MarketOrderClass(
                status=res['status'],
                symbol=res['symbol'],
                qty=res['qty'],
                notional=res['notional'],
                side=res['side']
            )
        else:
            # Raise exception if response is not successful
            raise Exception(json.loads(response.text)['message'])

    ############################
    # Close Position on a single stock
    ############################
    def liquidate_position(self, symbol):
        '''
        Close a position
        :param symbol: str: stock symbol
        :return: dict: close response
        '''
        url = f'{self.trade_url}positions/{symbol}'

        response = requests.delete(url, headers=self.headers)

        if response.status_code != 200:
            raise Exception(response.text)
        
        return json.loads(response.text)

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
        pos_data_df.drop(columns=['exchange', 'asset_class', 'asset_marginable', 'lastday_price', 'change_today', 'qty', 'asset_id', \
                                  'cost_basis', 'unrealized_intraday_pl', 'unrealized_intraday_plpc', 'avg_entry_price'], inplace=True)
        # Set data types for DataFrame columns
        pos_data_df = pos_data_df.astype({'symbol': 'str', 'side': 'str', 'market_value': 'float', 'unrealized_pl': 'float', 'unrealized_plpc': 'float', \
                                          'current_price': 'float', 'qty_available': 'float'})
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

    ############################
    # Get Account Information
    ############################
    def get_account(self):
        '''
        Get account information from Alpaca API
        :return: AccountClass: account information, see data_classes.py for details
        ''' 
        # URL for account information request       
        url = f'{self.trade_url}account'
        # Get account information from Alpaca API
        response = requests.get(url, headers=self.headers)
        # Check if response is successful
        if response.status_code == 200:
            res = json.loads(response.text)
            # Return account information as an AccountClass object
            return AccountClass(
                account_blocked=res['account_blocked'],
                account_number=res['account_number'],
                accrued_fees=res['accrued_fees'],
                admin_configurations=res['admin_configurations'],
                balance_asof=res['balance_asof'],
                bod_dtbp=res['bod_dtbp'],
                buying_power=res['buying_power'],
                cash=res['cash'],
                created_at=res['created_at'],
                crypto_status=res['crypto_status'],
                crypto_tier=res['crypto_tier'],
                currency=res['currency'],
                daytrade_count=res['daytrade_count'],
                daytrading_buying_power=res['daytrading_buying_power'],
                effective_buying_power=res['effective_buying_power'],
                equity=res['equity'],
                id=res['id'],
                initial_margin=res['initial_margin'],
                intraday_adjustments=res['intraday_adjustments'],
                last_equity=res['last_equity'],
                last_maintenance_margin=res['last_maintenance_margin'],
                long_market_value=res['long_market_value'],
                maintenance_margin=res['maintenance_margin'],
                multiplier=res['multiplier'],
                non_marginable_buying_power=res['non_marginable_buying_power'],
                options_approved_level=res['options_approved_level'],
                options_buying_power=res['options_buying_power'],
                options_trading_level=res['options_trading_level'],
                pattern_day_trader=res['pattern_day_trader'],
                pending_reg_taf_fees=res['pending_reg_taf_fees'],
                pending_transfer_in=res['pending_transfer_in'],
                portfolio_value=res['portfolio_value'],
                position_market_value=res['position_market_value'],
                regt_buying_power=res['regt_buying_power'],
                short_market_value=res['short_market_value'],
                shorting_enabled=res['shorting_enabled'],
                sma=res['sma'],
                status=res['status'],
                trade_suspended_by_user=res['trade_suspended_by_user'],
                trading_blocked=res['trading_blocked'],
                transfers_blocked=res['transfers_blocked'],
                user_configurations=res['user_configurations']
            )
        else:
            raise Exception(response.text)
    