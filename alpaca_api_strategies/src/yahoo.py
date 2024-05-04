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

    def get_stock_news_sentiment(self):
        articles = []
        link_count = 0
        for news in self.ticker.news:
            session = HTMLSession()
            response = session.get(news['link'])
            news_text = response.html.find('.caas-body', first=True).text
            link_count += 1
            if link_count > 3:
                break
            articles.append({'Title': news['title'], 'Article': news_text})

        sentiments = []
        for article in articles:
            sentiment = self.get_openai_market_sentiment(article['Title'], article['Article'])
            sentiments.append({'Sentiment': sentiment})
            
        bulls = 0
        bears = 0

        for sentiment in sentiments:
            if sentiment['Sentiment'] == 'BULLISH':
                bulls += 1
            elif sentiment['Sentiment'] == 'BEARISH':
                bears += 1

        if bulls > bears:
            openai_sentiment = 'bull'
        else:
            openai_sentiment = 'bear'
        
        if openai_sentiment == 'bull' and self.get_yahoo_sentiment() == 'bull':
            return 'bull'
        elif openai_sentiment == 'bear' and self.get_yahoo_sentiment() == 'bear':
            return 'bear'
        else:
            return 'neutral'

    def get_daily_stock_data(self):
        """
        Get stock data from Yahoo Finance API
        :return: DataFrame: stock data
        """
        df = self.ticker.history(period="6mo", interval="1d")
        df.reset_index(inplace=True)
        df.drop(columns=['Dividends', 'Stock Splits'], inplace=True)
        df['Date'] = df['Date'].dt.strftime('%Y/%m/%d')
        df['Date'] = pd.to_datetime(df['Date'])
        df = dropna(df)
        df.insert(0, 'Symbol', self.ticker.ticker)
        return df
        
    def get_yahoo_sentiment(self):
        '''
        Get stock sentiment based on recommendations
        :return: str: stock sentiment, Bullish or Bearish
        '''
        total_buys = self.ticker.recommendations['strongBuy'].sum() + self.ticker.recommendations['buy'].sum()
        total_sells = self.ticker.recommendations['strongSell'].sum() + self.ticker.recommendations['sell'].sum() + self.ticker.recommendations['hold'].sum()
        if total_buys > total_sells:
            sentiment = 'bull'
        else:    
            sentiment = 'bear'
        return sentiment

    def get_daily_loser_ticker_info(self):
        ticker_history = self.get_daily_stock_data()

        df_ticker = []
        try:
            for n in [14, 30, 50, 200]:
                ticker_history["sentiment"] = self.get_stock_news_sentiment()
                ticker_history["rsi" + str(n)] = RSIIndicator(close=ticker_history["Close"], window=n, fillna=True).rsi()
                ticker_history["bbhi" + str(n)] = BollingerBands(close=ticker_history["Close"], window=n, window_dev=2, fillna=True).bollinger_hband_indicator()
                ticker_history["bblo" + str(n)] = BollingerBands(close=ticker_history["Close"], window=n, window_dev=2, fillna=True).bollinger_lband_indicator()
            df_ticker_temp = ticker_history.iloc[-1:, -30:].reset_index(drop=True)
            df_ticker.append(df_ticker_temp)
            df_ticker = [x for x in df_ticker if not x.empty]
            df_ticker = pd.concat(df_ticker)
        except Exception:
            KeyError
        pass

        
        return df_ticker
    
    def get_daily_ticker_info(self):
        ticker_history = self.get_daily_stock_data()

        df_ticker = []
        try:
            for n in [14, 30, 50, 200]:
                ticker_history["rsi" + str(n)] = RSIIndicator(close=ticker_history["Close"], window=n, fillna=True).rsi()
                ticker_history["bbhi" + str(n)] = BollingerBands(close=ticker_history["Close"], window=n, window_dev=2, fillna=True).bollinger_hband_indicator()
                ticker_history["bblo" + str(n)] = BollingerBands(close=ticker_history["Close"], window=n, window_dev=2, fillna=True).bollinger_lband_indicator()
            df_ticker_temp = ticker_history.iloc[-1:, -30:].reset_index(drop=True)
            df_ticker.append(df_ticker_temp)
        except Exception:
            KeyError
        pass

        df_ticker = [x for x in df_ticker if not x.empty]
        df_ticker = pd.concat(df_ticker)
        return df_ticker
    