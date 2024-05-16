import os
from re import T
import time
import csv

import pandas as pd
from tqdm import tqdm

from src.global_functions import *
from src.yahoo import Yahoo
from src.openai import OpenAIAPI

from py_alpaca_api.alpaca import PyAlpacaApi

from ta.volatility import BollingerBands
from ta.momentum import RSIIndicator


from datetime import datetime
from datetime import timedelta
from pytz import timezone

tz = timezone('US/Eastern')
ctime = datetime.now(tz)
previous_day = (ctime - timedelta(days=1)).strftime("%Y-%m-%d")
year_ago = (ctime - timedelta(days=365)).strftime("%Y-%m-%d")

from dotenv import load_dotenv
load_dotenv()

api_key=str(os.getenv('APCA_API_KEY_ID'))
api_secret=str(os.getenv('APCA_API_SECRET_KEY'))
api_paper=bool(os.getenv('APCA_PAPER'))

class DailyLosers:
    """
    The Daily Losers class
    This class will buy the stocks based on the previous days market losers and openai market sentiment, and sell the stocks based on the criteria
    Should only be run at market open, and will be run at 9:30am EST
    """
    def __init__(self):
        self.yahoo = Yahoo()
        # Initialize the Alpaca API
        self.alpaca = PyAlpacaApi(api_key=api_key, api_secret=api_secret, api_paper=api_paper)
        # self.slack = Slack()
        self.production = True if os.getenv('PRODUCTION') == 'True' else False
    
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
        if self.production == True:
            print("Sleeping for 60 seconds to make sure the market is open")
            time.sleep(60)
        # Check for sell opportunities from the criteria
        self.sell_positions_from_criteria()
        # Liquidate the positions for capital to make cash 10% of the portfolio
        self.liquidate_positions_for_capital()
        # Buy the stocks based on the buy opportunities and openai sentiment
        self.buy_orders()

    def tradeable_stock(self, symbol):
        '''
        Get stock asset data from Alpaca API and return True if stock is tradeable and fractionable and active and not OTC and not empty response
        :param symbol: str: stock symbol
        :return: bool: True if stock is tradeable and fractionable and active and not OTC and not empty response
        '''
        try:
            asset = self.alpaca.asset.get(symbol)
        except Exception:
            return False

        otc = asset.exchange == 'OTC'
        tradeable = asset.tradable
        fractionable = asset.fractionable
        active = asset.status == 'active'
        return False if not tradeable or not fractionable or not active or otc else True

        
    #######################################################
    # Define the save_previous_day_losers function
    #######################################################
    def save_previous_day_losers(self):
        """
        Save the previous day losers to a CSV file
        """
        scraped_symbols         = self.yahoo.yahoo_scrape_symbols(yahoo_url='https://finance.yahoo.com/losers?offset=0&count=100', asset_type='stock', top=100)
        # Check if the asset is fractionable and tradable
        for ticker in scraped_symbols:
            asset = self.tradeable_stock(ticker)
            if not asset:
                scraped_symbols.remove(ticker)
                continue
        # Save the scraped symbols to a CSV file
        with open('alpaca_api_strategies/src/strategies/previous_day_losers.csv', 'w') as f:
            writer = csv.writer(f)
            for symbol in scraped_symbols:
                asset = self.tradeable_stock(symbol)
                if not asset:
                    scraped_symbols.remove(symbol)
                    continue
                writer.writerow([symbol])

        print("Saved {} previous day losers to CSV file".format(len(scraped_symbols)))

    #######################################################
    # Define the get_previous_day_losers function
    #######################################################
    def get_previous_day_losers(self):
        """
        Get the previous day losers from the CSV file
        return: List of previous day losers
        """
        # Get the previous day losers from the CSV file
        with open('alpaca_api_strategies/src/strategies/previous_day_losers.csv', 'r') as f:
            reader = csv.reader(f)
            symbols = list(reader)

        return [x for xs in symbols for x in xs]

    #######################################################
    # Define the get_buy_opportunities function
    #######################################################
    def get_buy_opportunities(self):
        """
        Get the buy opportunities based on the market losers and openai market sentiment
        return: List of buy opportunities
        """
        # Get the scraped symbols from the Yahoo Finance
        scraped_symbols = self.get_previous_day_losers()
        # Get the tickers from the Yahoo API, using the scraped symbols from Yahoo Finance
        tickers         = self.yahoo.get_tickers(scraped_symbols)
        # Get the ticker data from the Yahoo API
        tickers_data    = self.get_ticker_data(tickers.tickers)
        # Filter the buy tickers based on the buy criteria
        buy_tickers     = self.buy_criteria(tickers_data)
        # remove the tickers that are not in the buy_tickers list
        for key in list(tickers.tickers):
            if key not in buy_tickers:
                del tickers.tickers[key]
        # Get recommendations for the buy tickers
        recommended_tickers     = self.get_recommended_tickers(tickers)
        # remove the tickers that are not in the recommended_tickers list
        for key in list(tickers.tickers):
            if key not in recommended_tickers:
                del tickers.tickers[key]
        # Get the articles content for the buy tickers, as a list from the Yahoo API
        articles_content    = self.yahoo.get_articles(tickers)
        # Get the openai sentiment for the news articles
        buy_recommedations       = self.get_openai_sentiment(articles_content)
        # Return a list of buy recommendations
        return buy_recommedations
    
    #######################################################
    # Define the get_recommended_tickers function
    #######################################################
    def get_recommended_tickers(self, tickers):
        """
        Get the recommended tickers based on the recommendations summary from the Yahoo API
        :param tickers: DataFrame: tickers data
        :return: list: recommended_tickers
        """
        # Get the recommendations summary from the Yahoo API
        recommendations  = self.yahoo.get_recommendations_summary(tickers)

        recommended_tickers = []
        # Iterate through the tickers and get the recommended tickers
        for symbol in tickers.tickers:
            recommended_row = list(recommendations[recommendations['Symbol'] == symbol]['Recommendations'])
            # If the recommended row is empty, continue to the next symbol
            try:
                recommended = recommended_row[0]
                bulls = recommended['buy'] + recommended['strongBuy']
                bears = recommended['sell'] + recommended['strongSell'] + recommended['hold']
                # If the number of BULLISH recommendations is greater than the number of BEARISH recommendations, add the symbol to the recommended_tickers list
                if bulls > bears:
                    recommended_tickers.append(symbol)
            except TypeError:
                continue 
        # Return the recommended tickers list
        return recommended_tickers
    
    ########################################################
    # Define the buy_orders function
    ########################################################
    def buy_orders(self, limit=8):
        """
        Buy the stocks based on the buy opportunities, limit to 5 stocks by default
        Should only be run at market open
        Send a slack message with the bought positions, or print the bought positions
        """
        print("Buying orders based on buy opportunities and openai sentiment. Limit to 5 stocks by default")
        # Get the tickers from the get_ticker_info function and convert symbols to a list
        tickers = self.get_buy_opportunities()
        # Get the available cash from the Alpaca API
        available_cash = self.alpaca.account.get().cash
        # This is the amount to buy for each stock
        if len(tickers) == 0:
            notional = 0
        else:
            notional = (available_cash / len(tickers[:limit])) - 1
            
        bought_positions = []
        # Iterate through the tickers and buy the stocks
        for ticker in tickers[:limit]:
            # Market buy the stock
            try:
                if self.alpaca.market.clock().is_open:
                    #print(f"Buying {ticker} with notional amount of {notional}")
                    self.alpaca.order.market(symbol=ticker, notional=notional)
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
            bought_message = "Successfully{} bought the following positions:\n".format(" pretend" if not self.alpaca.market.clock().is_open else "")
            for position in bought_positions:
                bought_message += "${qty} of {symbol}\n".format(qty=position['notional'], symbol=position['symbol'])
        # Print or send the message
        send_message(bought_message)

    ########################################################
    # Define the buy_criteria function
    ########################################################
    def buy_criteria(self, data):
        """
        Get the buy criteria for the stock
        :param data: DataFrame: stock data
        :return: list: tickers
        """
        # Filter the DataFrame based on the buy criteria
        buy_criteria = (
            (data[["bblo14", "bblo30", "bblo50", "bblo200"]] == 1).any(axis=1)
        ) | ((data[["rsi14", "rsi30", "rsi50", "rsi200"]] <= 30).any(axis=1))
        # Get the filtered data based on the buy criteria
        buy_filtered_data = data[buy_criteria]
        # Return the list of tickers that meet the buy criteria
        return list(buy_filtered_data["symbol"])
    
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
        print("Liquidating positions for capital to make cash 10% of the portfolio")
        # Get the current positions from the Alpaca API
        current_positions = self.alpaca.position.get_all()
        if current_positions.empty:
            sold_message = "No positions available to liquidate for capital"
            send_message(sold_message)
            return
        # Get the cash available from the Alpaca API
        #cash_available = float(self.alpaca.get_account().cash)
        cash_row = current_positions[current_positions['symbol'] == 'Cash']
   
        # Get the total holdings from the current positions and cash available
        total_holdings  = current_positions['market_value'].sum()

        sold_positions = []
        # If the cash is less than 10% of the total holdings, liquidate the top 25% of performing stocks to make cash 10% of the portfolio
        if cash_row['market_value'][0] / total_holdings < 0.1:
            # Sort the positions by profit percentage
            current_positions = current_positions[current_positions['symbol'] != 'Cash'].sort_values(by='profit_pct', ascending=False) 
            # Sell the top 25% of performing stocks evenly to make cash 10% of total portfolio
            top_performers              = current_positions.iloc[:int(len(current_positions) // 2)]
            top_performers_market_value = top_performers['market_value'].sum()
            cash_needed                 = total_holdings * 0.1 - cash_row['market_value'][0]

            # Sell the top performers to make cash 10% of the portfolio
            for index, row in top_performers.iterrows():
                print(f"Selling {row['symbol']} to make cash 10% portfolio cash requirement")
                # Calculate the quantity to sell from the top performers
                #amount_to_sell = float((row['market_value'] / top_performers_market_value) * cash_needed)
                amount_to_sell = int((row['market_value'] / top_performers_market_value) * cash_needed)
                # If the amount to sell is 0, continue to the next stock
                if amount_to_sell == 0:
                    continue

                # Market sell the stock
                try:
                    # Market sell the stock if the market is open
                    if self.alpaca.market.clock().is_open:
                        self.alpaca.order.market(symbol=row['symbol'], notional=amount_to_sell, side='sell')
                # If there is an error, print or send a slack message
                except Exception as e:
                        send_message(f"Error selling {row['symbol']}: {e}")
                        continue
                # If the order was successful, append the sold position to the sold_positions list
                else:
                    sold_positions.append({'symbol': row['symbol'], 'notional': round(amount_to_sell, 2)})
        # Print or send slack messages of the sold positions
        if not sold_positions:
            # If no positions were sold, create the message
            sold_message = "No positions liquidated for capital"
        else:
            # If positions were sold, create the message
            # Pretend trades if the market is closed
            sold_message = "Successfully{} liquidated the following positions:\n".format(" pretend" if not self.alpaca.market.clock().is_open else "")
            for position in sold_positions:
                sold_message += "Sold ${qty} of {symbol}\n".format(qty=position['notional'], symbol=position['symbol'])
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
        print("Selling positions based on sell criteria")
        # Get the sell opportunities
        sell_opportunities = self.get_sell_opportunities()
        # Get the current positions
        current_positions = self.alpaca.position.get_all()
        sold_positions = [] 
        # Iterate through the sell opportunities and sell the stocks
        for symbol in sell_opportunities:
            # Try to sell the stock
            try:
                # Get the quantity of the stock to sell
                qty = current_positions[current_positions['symbol'] == symbol]['qty'].values[0]
                if self.alpaca.market.clock().is_open:
                    self.alpaca.order.market(symbol=symbol, qty=qty, side='sell')
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
            sold_message = "Successfully{} sold the following positions:\n".format(" pretend" if not self.alpaca.market.clock().is_open else "")
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
        # Get the current positions from the Alpaca API
        current_positions = self.alpaca.position.get_all()
        if current_positions[current_positions['symbol'] != 'Cash'].empty:
            return []
        # Get the symbols from the current positions that are not cash
        current_positions_symbols   = current_positions[current_positions['symbol'] != 'Cash']['symbol'].tolist()
        # Get the Yahoo tickers from the symbols
        yahoo_tickers               = self.yahoo.get_tickers(current_positions_symbols)
        # Get the assets history from the Yahoo API
        assets_history              = self.get_ticker_data(yahoo_tickers.tickers)
        # Get the sell criteria
        sell_criteria = ((assets_history[['rsi14', 'rsi30', 'rsi50', 'rsi200']] >= 70).any(axis=1)) | \
                            ((assets_history[['bbhi14', 'bbhi30', 'bbhi50', 'bbhi200']] == 1).any(axis=1))
        # Get the filtered positions based on the sell criteria
        sell_filtered_df = assets_history[sell_criteria]
        # Get the symbol list from the filtered positions
        return sell_filtered_df['symbol'].tolist()
    
    ########################################################
    # Define the get_ticker_data function
    ########################################################
    def get_ticker_data(self, tickers):
        """
        Get the daily stock data, RSI, and Bollinger Bands
        this function is used for the daily stock data, RSI, and Bollinger Bands
        there is no need to add the sentiment of the news articles
        :return: DataFrame: stock data
        """
        ticker_list = list(tickers.keys())

        df_tech = []
        # Get the daily stock data, RSI, and Bollinger Bands for the stock
        for i, ticker in tqdm(
            enumerate(ticker_list),
            desc="• Analizing ticker data for "
            + str(len(ticker_list))
            + " symbols from Alpaca API",
        ):
            try:
                history = self.alpaca.history.get_stock_data(symbol=ticker, start=year_ago, end=previous_day) 
            except Exception:
                del tickers[ticker]
                continue

            try:
                for n in [14, 30, 50, 200]:
                    # Initialize RSI Indicator
                    history["rsi" + str(n)] = RSIIndicator(
                        close=history["close"], window=n
                    ).rsi()
                    # Initialize Hi BB Indicator
                    history["bbhi" + str(n)] = BollingerBands(
                        close=history["close"], window=n, window_dev=2
                    ).bollinger_hband_indicator()
                    # Initialize Lo BB Indicator
                    history["bblo" + str(n)] = BollingerBands(
                        close=history["close"], window=n, window_dev=2
                    ).bollinger_lband_indicator()
                # Get the last 16 days of data
                df_tech_temp = history.tail(1)
                # Append the DataFrame to the list
                df_tech.append(df_tech_temp)
            except KeyError:
                pass

        # If the list is not empty, concatenate the DataFrames
        if df_tech != []:
            df_tech = [x for x in df_tech if not x.empty]
            df_tech = pd.concat(df_tech)
        # If the list is empty, create an empty DataFrame
        else:
            df_tech = pd.DataFrame()
        # Return the DataFrame
        return df_tech
    
    ########################################################
    # Define the get_openai_sentiment function
    ########################################################
    def get_openai_sentiment(self, article_contents):
        """
        Get the sentiment of the symbols based on news articles
        :param symbols: List of symbols
        return: List of symbols to buy
        """
        openai = OpenAIAPI()
        buy_opportunities = []
        # Iterate through the symbols and get the sentiment of the news articles
        for i, symbol in tqdm(
            enumerate(article_contents),
            desc="• OpenAI is analyzing the sentiment of "
            + str(len(article_contents))
            + " symbols based on news articles",
        ):
            sentiments = []
            # Get the sentiment of the news articles for the stock
            for article in symbol['Articles']:
                title = article['Title']
                sym = symbol['Symbol']
                article_text = article['Article']
                # Get the sentiment of the news article using the OpenAI API
                sentiment = openai.get_sentiment_analysis(title, sym, article_text)
                sentiments.append(sentiment)
            # If the number of BULLISH sentiments is greater than the number of BEARISH and NEUTRAL sentiments, add the symbol to the buy_opportunities list
            if sentiments.count('BULLISH') > (sentiments.count('BEARISH') + sentiments.count('NEUTRAL')):
                buy_opportunities.append(symbol['Symbol'])
        # Return the list of symbols to buy
        return buy_opportunities