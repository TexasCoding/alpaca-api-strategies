import os
import time
from src.alpaca import AlpacaAPI
from src.yahoo import Yahoo
from src.slack import Slack

import pandas as pd
from requests_html import HTMLSession
from tqdm import tqdm

from pprint import pprint

from dotenv import load_dotenv
load_dotenv()

class DailyLosers:
    def __init__(self):
        """
        The Daily Losers class
        This class will buy the stocks based on the previous days market losers and openai market sentiment, and sell the stocks based on the criteria
        Should only be run at market open, and will be run at 9:30am EST
        """
        # Initialize the Alpaca API
        self.alpaca = AlpacaAPI()
        self.slack = Slack()
        self.production = os.getenv('PRODUCTION')
        self.slack_username = os.getenv('SLACK_USERNAME')
    
    ########################################################
    # Define the run function
    ########################################################
    def run(self):
        self.sell_positions_from_criteria()
        self.liquidate_positions_for_capital()
        self.buy_orders()

    ########################################################
    # Define the buy_orders function
    ########################################################
    def buy_orders(self):
        """
        Buy the stocks based on the market losers
        The strategy is to buy the stocks that are losers that are oversold based on RSI and Bollinger Bands
        return: True if the function is successful
        return: False if market is closed
        """
        
        # Get the tickers from the get_ticker_info function and convert symbols to a list
        tickers = self.get_buy_opportunities()

        # Get the current positions and available cash
        df_current_positions = self.alpaca.get_current_positions()
        available_cash = df_current_positions[df_current_positions['asset'] == 'Cash']['qty'].values[0]

        # Calculate the notional value for each stock
        # Divide the available cash by the number of tickers
        # This is the amount to buy for each stock
        # First few days will create large positions, but will be rebalanced in the future (hopefully :D)
            
        notional = available_cash / len(tickers)

        bought_positions = []
        # Iterate through the tickers and buy the stocks
        for ticker in tickers:
            # Market buy the stock
            try:
                if self.alpaca.market_open():
                    self.alpaca.market_order(symbol=ticker, notional=notional)
            except Exception as e:
                if self.production == 'False':
                    print(f"Error buying {ticker}: {e}")
                else:
                    self.slack.send_message(channel='#app-development', message=f"Error buying {ticker}:\n {e}", username=self.slack_username)
                continue
            else:
                bought_positions.append({'symbol': ticker, 'notional': round(notional, 2)})

        # Print or send slack messages of the bought positions
        if not bought_positions:
            # If no positions were bought, create the message
            bought_message = "No positions bought"
        else:
            # If positions were bought, create the message
            bought_message = "Successfully{} bought the following positions:\n".format(" pretend" if not self.alpaca.market_open() else "")
            for position in bought_positions:
                bought_message += "{qty} shares of {symbol}\n".format(qty=position['notional'], symbol=position['symbol'])

        # Print or send the message
        if self.production == 'False':
            print(bought_message)
        else:
            self.slack.send_message(channel='#app-development', message=bought_message, username=self.slack_username)

    ########################################################
    # Define the liquidate_positions_for_capital function
    ########################################################
    def liquidate_positions_for_capital(self):
        """
        Liquidate the positions to make cash 10% of the portfolio
        The strategy is to sell the top 25% of performing stocks evenly to make cash 10% of total portfolio
        return: True if the function is successful
        return: False if the market is closed or there are no stocks to sell
        """
        current_positions = self.alpaca.get_current_positions()

        # If no current positions or market is closed, exit the function by returning
        if current_positions.iloc[0]['asset'] == 'Cash':
            print("No current positions")
            return

        cash_row        = current_positions[current_positions['asset'] == 'Cash']
        total_holdings  = current_positions['market_value'].sum()

        if cash_row['market_value'].values[0] / total_holdings < 0.1:
            # Remove the cash row
            curpositions = current_positions[current_positions['asset'] != 'Cash']
            # Sort the positions by profit percentage
            curpositions = curpositions.sort_values(by='profit_pct', ascending=False) 

            # Sell the top 25% of performing stocks evenly to make cash 10% of total portfolio
            top_performers              = curpositions.iloc[:int(len(curpositions) // 2)]
            top_performers_market_value = top_performers['market_value'].sum()
            cash_needed                 = total_holdings * 0.1 - cash_row['market_value'].values[0]

            sold_positions = []
            # Sell the top performers to make cash 10% of the portfolio
            for index, row in top_performers.iterrows():
                print(f"Selling {row['asset']} to make cash 10% portfolio cash requirement")
                # Calculate the amount to sell in USD
                amount_to_sell = int((row['market_value'] / top_performers_market_value) * cash_needed)
                
                # If the amount to sell is 0, continue to the next stock
                if amount_to_sell == 0:
                    continue

                # Market sell the stock
                try:
                    if self.alpaca.market_open():
                        self.alpaca.market_order(symbol=row['asset'], notional=amount_to_sell, side='sell')
                except Exception as e:
                        if self.production == 'False':
                            print(f"Error selling {row['asset']}: {e}")
                        else:
                            self.slack.send_message(channel='#app-development', message=f"Error selling {row['asset']}:\n {e}", username=self.slack_username)
                        continue
                else:
                    sold_positions.append({'symbol': row['asset'], 'notional': round(amount_to_sell, 2)})

            # Print or send slack messages of the sold positions
            if not sold_positions:
                # If no positions were sold, create the message
                sold_message = "No positions liquidated for capital"
            else:
                # If positions were sold, create the message
                sold_message = "Successfully{} liquidated the following positions:\n".format(" pretend" if not self.alpaca.market_open() else "")
                for position in sold_positions:
                    sold_message += "{qty} shares of {symbol}\n".format(qty=position['notional'], symbol=position['symbol'])
            # Print or send the message
            if self.production == 'False':
                print(sold_message)
            else:
                self.slack.send_message(channel='#app-development', message=sold_message, username=self.slack_username)

    def sell_positions_from_criteria(self):
        """
        Sell the positions based on the criteria
        The strategy is to sell the stocks that are overbought based on RSI and Bollinger Bands, or based on the criteria
        return: True if the function is successful
        return: False if market is closed or there are no stocks to sell
        """
        sell_opportunities = self.get_sell_opportunities()
        current_positions = self.alpaca.get_current_positions()

        sold_positions = [] 
        if sell_opportunities != []:
            # Iterate through the sell opportunities and sell the stocks
            for symbol in sell_opportunities:
                # Try to sell the stock
                try:
                    qty = current_positions[current_positions['asset'] == symbol]['qty'].values[0]
                    if self.alpaca.market_open():
                        self.alpaca.market_order(symbol=symbol, qty=qty, side='sell')

                except Exception as e:
                    if self.production == 'False':
                        print(f"Error selling {symbol}: {e}")
                    else:
                        self.slack.send_message(channel='#app-development', message=f"Error selling {symbol}:\n {e}", username=self.slack_username)
                # If the order was successful, append the sold position to the sold_positions list
                else:
                    sold_positions.append({'symbol': symbol, 'qty': qty})

        # Print or send slack messages of the sold positions
        if not sold_positions:
            # If no positions were sold, create the message
            sold_message = "No positions to sell"
        else:
            # If positions were sold, create the message
            sold_message = "Successfully{} sold the following positions:\n".format(" pretend" if not self.alpaca.market_open() else "")
            for position in sold_positions:
                sold_message += "{qty} shares of {symbol}\n".format(qty=position['qty'], symbol=position['symbol'])
        # Print or send the message
        if self.production == 'False':
            print(sold_message)
        else:
            self.slack.send_message(channel='#app-development', message=sold_message, username=self.slack_username)


    def get_sell_opportunities(self):
        current_positions = self.alpaca.get_current_positions()

        # If no current positions, exit the function by returning False
        if current_positions.iloc[0]['asset'] == 'Cash':
            print("No current positions")
            return []
        
        current_positions_symbols = current_positions[current_positions['asset'] != 'Cash']['asset'].tolist()

        sell_opportunities = []
        for i, symbol in tqdm(
            enumerate(current_positions_symbols),
            desc="Processing {position_count} positions for sell signals.".format(position_count=len(current_positions_symbols)),
        ):
            sell_opportunities.append(Yahoo(symbol).get_daily_ticker_info())
        
        current_positions_hist = pd.concat(sell_opportunities, axis=0).reset_index(drop=True)

        sell_criteria = ((current_positions_hist[['rsi14', 'rsi30', 'rsi50', 'rsi200']] >= 70).any(axis=1)) | \
                            ((current_positions_hist[['bbhi14', 'bbhi30', 'bbhi50', 'bbhi200']] == 1).any(axis=1))
        
        sell_filtered_df = current_positions_hist[sell_criteria]

        symbols = sell_filtered_df['Symbol'].tolist()

        if not symbols:
            print("No sell opportunities")
            return []
        else:
            return symbols

    def get_buy_opportunities(self):
        """
        Get the buy opportunities
        return: List of buy opportunities
        """
        # Get the market losers
        symbols = self.get_market_losers()
        # Get the buy opportunities
        buy_opportunities = []

        print("Processing {loser_count} tickers from Yahoo Losers.\nThis will take appromatly {total_time} minutes to complete.".format(loser_count=len(symbols), total_time=(len(symbols) * 22) / 60))
        for i, symbol in tqdm(
            enumerate(symbols),
            desc="Processing {loser_count} tickers for trading signals".format(loser_count=len(symbols)),
        ):
            asset = self.alpaca.get_asset(symbol)
            if asset.fractionable and asset.tradable:
                time.sleep(1)
                ticker = Yahoo(symbol).get_daily_loser_ticker_info()
                if not ticker.empty:
                    if ticker.sentiment.values[0] == 'bull':
                        buy_opportunities.append(ticker)

        market_losers = pd.concat(buy_opportunities, axis=0).reset_index(drop=True)

        # Filter the losers based on indicators
        buy_criteria = ((market_losers[['rsi14', 'rsi30', 'rsi50', 'rsi200']] <= 30).any(axis=1)) | \
                          ((market_losers[['bblo14', 'bblo30', 'bblo50', 'bblo200']] == 1).any(axis=1))
        
        buy_filtered_df = market_losers[buy_criteria]
            
        return buy_filtered_df['Symbol'].tolist()

    ########################################################
    # Define the get_raw_info function
    ########################################################
    def get_raw_info(self, site):
        """
        Get the raw information from the given site
        :param site: Site URL
        return: DataFrame with the raw information
        """
        # Create a HTMLSession object
        session = HTMLSession()
        response = session.get(site)
        # Get the tables from the site
        tables = pd.read_html(response.html.raw_html)
        df = tables[0].copy()
        df.columns = tables[0].columns
        # Close the session
        session.close()
        return df

    ########################################################
    # Define the get_symbols function
    ########################################################
    def get_market_losers(self, yahoo_url='https://finance.yahoo.com/losers?offset=0&count=100', asset_type='stock', top=60):
        """
        Get the symbols from the given Yahoo URL
        :param yahoo_url: Yahoo URL
        :param asset_type: Asset type (stock, etf, etc.)
        :param top: Number of top symbols to get
        return: List of symbols from the Yahoo URL
        """
        df_stock = self.get_raw_info(yahoo_url)
        df_stock["asset_type"] = asset_type
        df_stock = df_stock.head(top)

        df_opportunities = pd.concat([df_stock], axis=0).reset_index(drop=True)

        return list(df_opportunities['Symbol'])