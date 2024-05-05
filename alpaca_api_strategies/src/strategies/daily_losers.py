import os
import time

from src.global_functions import *
from src.alpaca import AlpacaAPI
from src.yahoo import Yahoo

from dotenv import load_dotenv
load_dotenv()

class DailyLosers:
    """
    The Daily Losers class
    This class will buy the stocks based on the previous days market losers and openai market sentiment, and sell the stocks based on the criteria
    Should only be run at market open, and will be run at 9:30am EST
    """
    def __init__(self):
        self.yahoo = Yahoo()
        # Initialize the Alpaca API
        self.alpaca = AlpacaAPI()
        # self.slack = Slack()
        self.production = os.getenv('PRODUCTION')
        # self.slack_username = os.getenv('SLACK_USERNAME')
    
    #######################################################
    #Define the run function
    #######################################################
    def run(self):
        """
        Run the daily losers strategy, buy the stocks based on the market losers and openai market sentiment, and sell the stocks based on the criteria
        Run at 9:30am EST when the market opens
        Should only be run at market open
        """
        # Sleep for 60 seconds to make sure the market is open
        if self.production == 'True':
            time.sleep(60)
        # Sell the positions based on the criteria
        self.sell_positions_from_criteria()
        # Liquidate the positions to make cash 10% of the portfolio
        self.liquidate_positions_for_capital()
        # Buy stocks based on the market losers, limit to 5 stocks by default
        self.buy_orders()

    #######################################################
    # Define the get_buy_opportunities function
    #######################################################
    def get_buy_opportunities(self):
        """
        Get the buy opportunities based on the market losers and openai market sentiment
        return: List of buy opportunities
        """
        # Get the tickers from the Yahoo API
        tickers                 = self.yahoo.get_loser_tickers().tickers
        # Get the ticker data from the Yahoo API
        tickers_data            = self.yahoo.get_ticker_data(tickers)
        # Filter the buy tickers based on the buy criteria
        buy_tickers             = self.yahoo.buy_criteria(tickers_data)
        # Get news and recommendations for the buy tickers from the Yahoo API
        news_recommendations    = self.yahoo.get_articles(buy_tickers)
        # Get the openai sentiment for the news articles
        buy_recommedations       = self.yahoo.get_openai_sentiment(news_recommendations)
        # Return the buy recommendations, as a list
        return buy_recommedations
    
    ########################################################
    # Define the buy_orders function
    ########################################################
    def buy_orders(self, limit=5):
        """
        Buy the stocks based on the buy opportunities, limit to 5 stocks by default
        Should only be run at market open
        Send a slack message with the bought positions, or print the bought positions
        """
        
        # Get the tickers from the get_ticker_info function and convert symbols to a list
        tickers = self.get_buy_opportunities()

        # Get the current positions and available cash
        df_current_positions = self.alpaca.get_current_positions()
        available_cash = df_current_positions[df_current_positions['asset'] == 'Cash']['qty'].values[0]

        # This is the amount to buy for each stock
        notional = float(available_cash) / int(len(tickers))

        bought_positions = []
        # Iterate through the tickers and buy the stocks
        for ticker in tickers[:limit]:
            # Market buy the stock
            try:
                if self.alpaca.market_open():
                    self.alpaca.market_order(symbol=ticker, notional=notional)
            # If there is an error, print or send a slack message
            except Exception as e:
                send_message(f"Error buying {ticker}: {e}")
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
                bought_message += "${qty} of {symbol}\n".format(qty=position['notional'], symbol=position['symbol'])

        # Print or send the message
        send_message(bought_message)

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

        cash_row        = current_positions[current_positions['asset'] == 'Cash']
        total_holdings  = current_positions['market_value'].sum()

        sold_positions = []
        if float(cash_row['market_value'].values[0]) / float(total_holdings) < 0.1:
            # Remove the cash row
            curpositions = current_positions[current_positions['asset'] != 'Cash']
            # Sort the positions by profit percentage
            curpositions = curpositions.sort_values(by='profit_pct', ascending=False) 

            # Sell the top 25% of performing stocks evenly to make cash 10% of total portfolio
            top_performers              = curpositions.iloc[:int(len(curpositions) // 2)]
            top_performers_market_value = top_performers['market_value'].sum()
            cash_needed                 = total_holdings * 0.1 - cash_row['market_value'].values[0]

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
                        send_message(f"Error selling {row['asset']}: {e}")
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
        send_message(sold_message)

    ########################################################
    # Define the sell_positions_from_criteria function
    ########################################################
    def sell_positions_from_criteria(self):
        """
        Sell the positions based on the criteria
        The strategy is to sell the stocks that are overbought based on RSI and Bollinger Bands, or based on the criteria
        return: True if the function is successful
        return: False if market is closed or there are no stocks to sell
        """
        # Get the sell opportunities
        sell_opportunities = self.get_sell_opportunities()
        # Get the current positions
        current_positions = self.alpaca.get_current_positions()

        sold_positions = [] 
        # Iterate through the sell opportunities and sell the stocks
        for symbol in sell_opportunities:
            # Try to sell the stock
            try:
                qty = current_positions[current_positions['asset'] == symbol]['qty'].values[0]
                if self.alpaca.market_open():
                    self.alpaca.market_order(symbol=symbol, qty=qty, side='sell')
            # If there is an error, print or send a slack message
            except Exception as e:
                send_message(f"Error selling {symbol}: {e}")
                continue
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
        send_message(sold_message)

    ########################################################
    # Define the get_sell_opportunities function
    ########################################################
    def get_sell_opportunities(self):
        """
        Get the sell assets opportunities based on the RSI and Bollinger Bands
        return: List of sell opportunities
        """
        current_positions = self.alpaca.get_current_positions()

        current_positions_symbols   = current_positions[current_positions['asset'] != 'Cash']['asset'].tolist()
        yahoo_tickers               = self.yahoo.get_tickers(['EXPE', 'BILL', 'ATMU', 'TNC', 'TRMB', 'IR', 'CYBR'])
        assets_history              = self.yahoo.get_ticker_data(yahoo_tickers.tickers)

        sell_criteria = ((assets_history[['rsi14', 'rsi30', 'rsi50', 'rsi200']] >= 70).any(axis=1)) | \
                            ((assets_history[['bbhi14', 'bbhi30', 'bbhi50', 'bbhi200']] == 1).any(axis=1))
        # Get the filtered positions
        sell_filtered_df = assets_history[sell_criteria]
        # Get the symbol list from the filtered positions
        return sell_filtered_df['Symbol'].tolist()