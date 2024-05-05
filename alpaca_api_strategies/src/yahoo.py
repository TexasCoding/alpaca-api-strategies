import os
import time

import yfinance as yf
import pandas as pd

from requests import Session
from requests_cache import CacheMixin, SQLiteCache
from requests_ratelimiter import LimiterMixin, MemoryQueueBucket
from pyrate_limiter import Duration, RequestRate, Limiter

from ta.volatility import BollingerBands
from ta.momentum import RSIIndicator
from ta.utils import dropna

from requests_html import HTMLSession

from openai import OpenAI

from tqdm import tqdm

from dotenv import load_dotenv
load_dotenv()

# Define the CachedLimiterSession class
class CachedLimiterSession(CacheMixin, LimiterMixin, Session):
    pass

# Define the Yahoo class
class Yahoo:
    def __init__(self):
        pass
    
    ########################################################
    # Define the get_openai_sentiment function
    ########################################################
    def get_openai_sentiment(self, symbols):
        """
        Get the sentiment of the symbols based on news articles
        :param symbols: List of symbols
        return: List of symbols to buy
        """
        buy_opportunities = []
        # Get the news articles for the symbols
        for i, symbol in tqdm(
            enumerate(symbols),
            desc="• OpenAI is analyzing the sentiment of "
            + str(len(symbols))
            + " symbols based on news articles",
        ):
            sentiments = []
            # Get the sentiment of the news articles for the stock
            for article in symbol['Articles']:
                title = article['Title']
                article_text = article['Article']
                # Get the sentiment of the news article using the OpenAI API
                sentiment = self.get_openai_market_sentiment(title, article_text)
                sentiments.append(sentiment)
            # If the number of BULLISH sentiments is greater than the number of BEARISH and NEUTRAL sentiments, add the symbol to the buy_opportunities list
            if sentiments.count('BULLISH') > (sentiments.count('BEARISH') + sentiments.count('NEUTRAL')):
                buy_opportunities.append(symbol['Symbol'])
        # Return the list of symbols to buy
        return buy_opportunities

    ########################################################
    # Define the OpenAi chat function
    ########################################################
    def chat(self, msgs):
        """
        Chat with the OpenAI API
        :param msgs: List of messages
        return: OpenAI response
        """
        openai = OpenAI(api_key=os.getenv('OPENAI_API_KEY'))
        response = openai.chat.completions.create(
            model="gpt-3.5-turbo",
            messages=msgs
        )
        message = response
        return message
    
    ########################################################
    # Define the get_market_sentiment function
    ########################################################
    def get_openai_market_sentiment(self, title, article):
        """
        Get the sentiment of the article using OpenAI API sentiment analysis
        :param article: Article
        return: Sentiment of the article (BULLISH, BEARISH, NEUTRAL)
        """
        message_history = []
        sentiments = []
        # Send the system message to the OpenAI API
        system_message = 'You will work as a Sentiment Analysis for Financial news. I will share news headline and article. You will only answer as:\n\n BEARISH,BULLISH,NEUTRAL. No further explanation. \n Got it?'
        message_history.append({'content': system_message, 'role': 'user'})
        response = self.chat(message_history)

        # Send the article to the OpenAI API
        user_message = '{}\n{}'.format(title, article)
        
        message_history.append({'content': user_message, 'role': 'user'})
        response = self.chat(message_history)
        sentiments.append(
            {'title': title, 'article': article, 'signal': response.choices[0].message.content})
        message_history.pop()
        # Return the sentiment
        return sentiments[0]['signal']
    
    def get_articles(self, tickers):
        """
        Get the news articles for the given tickers
        :param tickers: list: tickers
        :return: list: news articles
        """

        # Create a CachedLimiterSession object, which is a Session object with caching and rate limiting, to avoid getting blocked
        # Makes the request to the Yahoo Finance API slower, but avoids getting blocked
        # On Heroku it seems to run much slower, so it might be better to run it locally if possible
        session = CachedLimiterSession(
            limiter=Limiter(RequestRate(2, Duration.SECOND*5)),  # max 2 requests per 5 seconds
            bucket_class=MemoryQueueBucket,
            backend=SQLiteCache("yfinance.cache"),
        )
        # Get the tickers from Yahoo Finance
        filtered_tickers = yf.Tickers(tickers, session=session)
        tickers_list = list(filtered_tickers.tickers.keys())

        articles = []
        # Get the news articles for the stock from Yahoo Finance, and add the article text to the list
        for i, ticker in tqdm(
            enumerate(tickers_list),
            desc="• Grabbing recommendations and news for "
            + str(len(tickers_list))
            + " assets",
        ):
            # Get the recommendations summary for the stock
            try:
                summary = filtered_tickers.tickers[ticker].recommendations_summary
                summary = summary.dropna()
            except Exception:
                continue    
            # If the summary is not empty, get the recommedations of the stock and add the news articles to the list, from Yahoo Finance
            if not summary.empty:
                total_buys = summary['strongBuy'].sum() + summary['buy'].sum()
                total_sells = summary['strongSell'].sum() + summary['sell'].sum() + summary['hold'].sum()
                # If the total buys are greater than the total sells, the sentiment is bullish, otherwise it is bearish
                if total_buys > total_sells:
                    sentiment = 'bull'
                else:    
                    sentiment = 'bear'
                # If the sentiment is bullish, get the news articles for the stock
                if sentiment == 'bull':
                    news = filtered_tickers.tickers[ticker].news[:3]
                    #article_urls = [news['link'] for news in news]
                    article_info = []
                    for n in news:
                        article_info.append({'Title': n['title'], 'Link': n['link']})
                    # Add the stock symbol and the news articles to the list
                    articles.append({'Symbol': ticker, 'Articles': article_info})

        symbols = []        
        # Get the news articles for the stock from Yahoo Finance, and add the article text to the list
        for i, yahoo_news in tqdm(
            enumerate(articles),
            desc="• Getting news article text for "
            + str(len(articles))
            + " symbols",
        ):
            articles_text = []
            # Get the news articles for the stock from Yahoo Finance, and add the article text to the list
            # Throttle the requests to 1 request per second to avoid getting blocked
            for article in yahoo_news['Articles']:
                session = HTMLSession()
                response = session.get(article['Link'])
                article_text = response.html.find('.caas-body', first=True).text
                articles_text.append({'Title': article['Title'], 'Article': article_text})
                session.close()
                time.sleep(1)
            # Add the stock symbol and the news articles to the list
            symbols.append({'Symbol': yahoo_news['Symbol'], 'Articles': articles_text})
        # Return the list of symbols with the news articles
        return symbols
    
    ########################################################
    # Define the get_loser_tickers function
    ########################################################
    def get_tickers(self, tickers):
        """
        Get the list of tickers
        :return: list: tickers
        """
        session = CachedLimiterSession(
            limiter=Limiter(RequestRate(2, Duration.SECOND*5)),  # max 2 requests per 5 seconds
            bucket_class=MemoryQueueBucket,
            backend=SQLiteCache("yfinance.cache"),
        )

        return yf.Tickers(tickers, session=session)

    ########################################################
    # Define the get_loser_tickers function
    ########################################################
    def get_loser_tickers(self):
        """
        Get the list of tickers
        :return: list: tickers
        I will change this at some point to remove some redundancy
        """
        session = CachedLimiterSession(
            limiter=Limiter(RequestRate(2, Duration.SECOND*5)),  # max 2 requests per 5 seconds
            bucket_class=MemoryQueueBucket,
            backend=SQLiteCache("yfinance.cache"),
        )
        # Get the top 100 losers from Yahoo Finance
        raw_tickers = self.get_market_losers()
        # Get the tickers from Yahoo Finance, and return the tickers as a Tickers object
        return yf.Tickers(raw_tickers, session=session)
    
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
        for ticker in ticker_list:
            history = tickers[ticker].history(period="1y", interval="1d")

            for n in [14, 30, 50, 200]:
                # Initialize RSI Indicator
                history["rsi" + str(n)] = RSIIndicator(
                    close=history["Close"], window=n
                ).rsi()
                # Initialize Hi BB Indicator
                history["bbhi" + str(n)] = BollingerBands(
                    close=history["Close"], window=n, window_dev=2
                ).bollinger_hband_indicator()
                # Initialize Lo BB Indicator
                history["bblo" + str(n)] = BollingerBands(
                    close=history["Close"], window=n, window_dev=2
                ).bollinger_lband_indicator()
            # Get the last 16 days of data
            df_tech_temp = history.iloc[-1:, -16:].reset_index(drop=True)
            # Add the stock symbol to the DataFrame
            df_tech_temp.insert(0, "Symbol", ticker)
            # Append the DataFrame to the list
            df_tech.append(df_tech_temp)
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
    # Define the buy_criteria function
    ########################################################
    def buy_criteria(self, data):
        """
        Get the buy criteria for the stock
        :param data: DataFrame: stock data
        :return: list: tickers
        """
        # Get the buy criteria for the stock
        buy_criteria = (
            (data[["bblo14", "bblo30", "bblo50", "bblo200"]] == 1).any(axis=1)
        ) | ((data[["rsi14", "rsi30", "rsi50", "rsi200"]] <= 30).any(axis=1))

        # Filter the DataFrame
        buy_filtered_data = data[buy_criteria]

        # Return the list of tickers that meet the buy criteria
        return list(buy_filtered_data["Symbol"])

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
    def get_market_losers(self, yahoo_url='https://finance.yahoo.com/losers?offset=0&count=100', asset_type='stock', top=100):
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
    