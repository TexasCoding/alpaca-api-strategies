import os
import time

from tqdm import tqdm

from src.global_functions import *
from src.alpaca import AlpacaAPI
from src.yahoo import Yahoo
from src.openai import OpenAIAPI

from dotenv import load_dotenv
load_dotenv()

from pprint import pprint

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
            print("Sleeping for 60 seconds to make sure the market is open")
            time.sleep(60)
        # Check for sell opportunities from the criteria
        self.sell_positions_from_criteria()
        # Liquidate the positions for capital to make cash 10% of the portfolio
        self.liquidate_positions_for_capital()
        # Buy the stocks based on the buy opportunities and openai sentiment
        self.buy_orders()

    #######################################################
    # Define the get_buy_opportunities function
    #######################################################
    def get_buy_opportunities(self):
        """
        Get the buy opportunities based on the market losers and openai market sentiment
        return: List of buy opportunities
        """
        scraped_symbols         = self.yahoo.yahoo_scrape_symbols(yahoo_url='https://finance.yahoo.com/losers?offset=0&count=100', asset_type='stock', top=100)
        # Get the tickers from the Yahoo API
        tickers                 = self.yahoo.get_tickers(scraped_symbols)
        # Get the ticker data from the Yahoo API
        tickers_data            = self.yahoo.get_ticker_data(tickers.tickers)
        # Filter the buy tickers based on the buy criteria
        buy_tickers             = self.buy_criteria(tickers_data)
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
    def buy_orders(self, limit=5):
        """
        Buy the stocks based on the buy opportunities, limit to 5 stocks by default
        Should only be run at market open
        Send a slack message with the bought positions, or print the bought positions
        """
        print("Buying orders based on buy opportunities and openai sentiment. Limit to 5 stocks by default")
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
        return list(buy_filtered_data["Symbol"])
    
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
        current_positions = self.alpaca.get_current_positions()
        # Get the cash row from the current positions
        cash_row        = current_positions[current_positions['asset'] == 'Cash']
        # Get the total holdings from the current positions and cash row
        total_holdings  = current_positions['market_value'].sum()

        sold_positions = []
        # If the cash is less than 10% of the total holdings, liquidate the top 25% of performing stocks to make cash 10% of the portfolio
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
                    # Market sell the stock if the market is open
                    if self.alpaca.market_open():
                        self.alpaca.market_order(symbol=row['asset'], notional=amount_to_sell, side='sell')
                # If there is an error, print or send a slack message
                except Exception as e:
                        send_message(f"Error selling {row['asset']}: {e}")
                        continue
                # If the order was successful, append the sold position to the sold_positions list
                else:
                    sold_positions.append({'symbol': row['asset'], 'notional': round(amount_to_sell, 2)})

        # Print or send slack messages of the sold positions
        if not sold_positions:
            # If no positions were sold, create the message
            sold_message = "No positions liquidated for capital"
        else:
            # If positions were sold, create the message
            # Pretend trades if the market is closed
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
        print("Selling positions based on sell criteria")
        # Get the sell opportunities
        sell_opportunities = self.get_sell_opportunities()
        # Get the current positions
        current_positions = self.alpaca.get_current_positions()

        sold_positions = [] 
        # Iterate through the sell opportunities and sell the stocks
        for symbol in sell_opportunities:
            # Try to sell the stock
            try:
                # Get the quantity of the stock to sell
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
        # Get the current positions from the Alpaca API
        current_positions = self.alpaca.get_current_positions()
        # Get the symbols from the current positions that are not cash
        current_positions_symbols   = current_positions[current_positions['asset'] != 'Cash']['asset'].tolist()
        # Get the Yahoo tickers from the symbols
        yahoo_tickers               = self.yahoo.get_tickers(current_positions_symbols)
        # Get the assets history from the Yahoo API
        assets_history              = self.yahoo.get_ticker_data(yahoo_tickers.tickers)

        # If the assets history is empty, return an empty list
        if assets_history.empty:
            return []
        # Get the sell criteria
        sell_criteria = ((assets_history[['rsi14', 'rsi30', 'rsi50', 'rsi200']] >= 70).any(axis=1)) | \
                            ((assets_history[['bbhi14', 'bbhi30', 'bbhi50', 'bbhi200']] == 1).any(axis=1))
        # Get the filtered positions based on the sell criteria
        sell_filtered_df = assets_history[sell_criteria]
        # Get the symbol list from the filtered positions
        return sell_filtered_df['Symbol'].tolist()
    
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
                article_text = article['Article']
                # Get the sentiment of the news article using the OpenAI API
                sentiment = openai.get_sentiment_analysis(title, article_text)
                sentiments.append(sentiment)
            # If the number of BULLISH sentiments is greater than the number of BEARISH and NEUTRAL sentiments, add the symbol to the buy_opportunities list
            if sentiments.count('BULLISH') > (sentiments.count('BEARISH') + sentiments.count('NEUTRAL')):
                buy_opportunities.append(symbol['Symbol'])
        # Return the list of symbols to buy
        return buy_opportunities