import os

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

from dotenv import load_dotenv
load_dotenv()

class CachedLimiterSession(CacheMixin, LimiterMixin, Session):
    pass

class Yahoo:
    def __init__(self, symbol):
        """
        Initialize Yahoo Finance API
        :param symbol: str: stock symbol
        """
        # Create a session with a rate limiter and cache
        self.openai = OpenAI(api_key=os.getenv('OPENAI_API_KEY'))
        session = CachedLimiterSession(
            limiter=Limiter(RequestRate(2, Duration.SECOND*5)),  # max 2 requests per 5 seconds
            bucket_class=MemoryQueueBucket,
            backend=SQLiteCache("yfinance.cache"),
        )
        self.ticker = yf.Ticker(symbol, session=session)
    
    ########################################################
    # Define the OpenAi chat function
    ########################################################
    def chat(self, msgs):
        """
        Chat with the OpenAI API
        :param msgs: List of messages
        return: OpenAI response
        """
        response = self.openai.chat.completions.create(
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

    ########################################################
    # Define the get_stock_news_sentiment function
    ########################################################
    def get_stock_news_sentiment(self):
        '''
        Get stock sentiment based on news articles, using OpenAI API and Yahoo Finance
        :return: str: stock sentiment, Bullish or Bearish
        '''
        articles = []
        link_count = 0
        # Get the news articles for the stock from Yahoo Finance
        for news in self.ticker.news:
            session = HTMLSession()
            response = session.get(news['link'])
            news_text = response.html.find('.caas-body', first=True).text
            link_count += 1
            # Limit the number of articles to 3
            if link_count > 3:
                break
            # Append the article to the list
            articles.append({'Title': news['title'], 'Article': news_text})

        sentiments = []
        # Get the sentiment of the articles using OpenAI API
        for article in articles:
            sentiment = self.get_openai_market_sentiment(article['Title'], article['Article'])
            sentiments.append({'Sentiment': sentiment})
            
        bulls = 0
        bears = 0
        # Calculate the overall sentiment
        for sentiment in sentiments:
            if sentiment['Sentiment'] == 'BULLISH':
                bulls += 1
            elif sentiment['Sentiment'] == 'BEARISH':
                bears += 1

        if bulls > bears:
            openai_sentiment = 'bull'
        else:
            openai_sentiment = 'bear'
        
        # Return the overall sentiment, Bullish or Bearish, based on the articles
        if openai_sentiment == 'bull' and self.get_yahoo_sentiment() == 'bull':
            return 'bull'
        elif openai_sentiment == 'bear' and self.get_yahoo_sentiment() == 'bear':
            return 'bear'
        else:
            return 'neutral'

    ########################################################
    # Define the get_daily_stock_data function
    ########################################################
    def get_daily_stock_data(self):
        """
        Get stock data from Yahoo Finance API
        :return: DataFrame: stock data
        """
        # Get the stock data, drop the columns, and reset the index
        df = self.ticker.history(period="6mo", interval="1d")
        df.reset_index(inplace=True)
        df.drop(columns=['Dividends', 'Stock Splits'], inplace=True)
        df['Date'] = df['Date'].dt.strftime('%Y/%m/%d')
        df['Date'] = pd.to_datetime(df['Date'])
        df = dropna(df)
        df.insert(0, 'Symbol', self.ticker.ticker)
        return df
    
    ########################################################
    # Define the get_yahoo_sentiment function
    ########################################################
    def get_yahoo_sentiment(self):
        '''
        Get stock sentiment based on recommendations from Yahoo Finance
        :return: str: stock sentiment, Bullish or Bearish
        '''
        # Get the stock recommendations, calculate the total buys and sells, and return the sentiment
        total_buys = self.ticker.recommendations['strongBuy'].sum() + self.ticker.recommendations['buy'].sum()
        total_sells = self.ticker.recommendations['strongSell'].sum() + self.ticker.recommendations['sell'].sum() + self.ticker.recommendations['hold'].sum()
        if total_buys > total_sells:
            sentiment = 'bull'
        else:    
            sentiment = 'bear'
        return sentiment

    ########################################################
    # Define the get_daily_loser_ticker_info function
    ########################################################
    def get_daily_loser_ticker_info(self):
        """
        Get the daily loser stock data, RSI, and Bollinger Bands
        And add the sentiment of the news articles
        :return: DataFrame: stock data
        """
        # Get the daily stock data
        ticker_history = self.get_daily_stock_data()

        df_ticker = []
        # Get the RSI and Bollinger Bands for the stock, and add the sentiment of the news articles
        try:
            for n in [14, 30, 50, 200]:
                ticker_history["sentiment"] = self.get_stock_news_sentiment()
                ticker_history["rsi" + str(n)] = RSIIndicator(close=ticker_history["Close"], window=n, fillna=True).rsi()
                ticker_history["bbhi" + str(n)] = BollingerBands(close=ticker_history["Close"], window=n, window_dev=2, fillna=True).bollinger_hband_indicator()
                ticker_history["bblo" + str(n)] = BollingerBands(close=ticker_history["Close"], window=n, window_dev=2, fillna=True).bollinger_lband_indicator()
            df_ticker_temp = ticker_history.iloc[-1:, -30:].reset_index(drop=True)
            df_ticker.append(df_ticker_temp)
            df_ticker = [x for x in df_ticker if not x.empty]
        # if there is an exception, return an empty DataFrame
        except Exception:
            df_ticker = pd.DataFrame()
        # if there is no exception, return the DataFrame with the stock data
        else:
            df_ticker = pd.concat(df_ticker)
        
        return df_ticker
    
    ########################################################
    # Define the get_daily_ticker_info function
    ########################################################
    def get_daily_ticker_info(self):
        """
        Get the daily stock data, RSI, and Bollinger Bands
        this function is used for the daily stock data, RSI, and Bollinger Bands
        there is no need to add the sentiment of the news articles
        :return: DataFrame: stock data
        """
        # Get the daily stock data
        ticker_history = self.get_daily_stock_data()

        df_ticker = []
        # Get the RSI and Bollinger Bands for the stock
        try:
            for n in [14, 30, 50, 200]:
                ticker_history["rsi" + str(n)] = RSIIndicator(close=ticker_history["Close"], window=n, fillna=True).rsi()
                ticker_history["bbhi" + str(n)] = BollingerBands(close=ticker_history["Close"], window=n, window_dev=2, fillna=True).bollinger_hband_indicator()
                ticker_history["bblo" + str(n)] = BollingerBands(close=ticker_history["Close"], window=n, window_dev=2, fillna=True).bollinger_lband_indicator()
            df_ticker_temp = ticker_history.iloc[-1:, -30:].reset_index(drop=True)
            df_ticker.append(df_ticker_temp)
            df_ticker = [x for x in df_ticker if not x.empty]
        # if there is an exception, return an empty DataFrame
        except Exception:
            df_ticker = pd.DataFrame()
        # if there is no exception, return the DataFrame with the stock data
        else: 
            df_ticker = pd.concat(df_ticker)
            
        return df_ticker
    