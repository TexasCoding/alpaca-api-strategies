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

from requests_html import HTMLSession

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
    # Define the get_recommendations_summary function
    ########################################################
    def get_recommendations_summary(self, tickers):
        """
        Get the recommendations summary for the given tickers
        :param tickers: Dataframe: tickers
        :return: DataFrame: recommendations summary
        """
        print("Getting recommendations summary from Yahoo Finance.")
        tickers_list = list(tickers.tickers)

        recommendations = []
        for i, ticker in tqdm(
            enumerate(tickers_list),
            desc="• Downloading recommendations for "
            + str(len(tickers_list))
            + " symbols from Yahoo Finance",
        ):
            # Get the recommendations summary for the stock
            
            summary = tickers.tickers[ticker].recommendations_summary
            summary = summary.dropna()
  
            # If the summary is not empty, get the recommedations of the stock and add the news articles to the list, from Yahoo Finance
            if not summary.empty:
                recommendations.append({'Symbol': ticker, 'Recommendations': {'strongBuy': summary['strongBuy'].sum(), 'buy': summary['buy'].sum(), 'hold': summary['hold'].sum(), 'sell': summary['sell'].sum(), 'strongSell': summary['strongSell'].sum()}})
            else:
                recommendations.append({'Symbol': ticker, 'Recommendations': None})
        
        recommendations_df = pd.DataFrame(recommendations)
        return recommendations_df
    
    ########################################################
    # Define the get_articles function
    ########################################################
    def get_articles(self, tickers):
        """
        Get the news articles for the given tickers
        :param tickers: list: tickers
        :return: list: news articles
        """
        print("Getting news articles from Yahoo Finance.")
        tickers_list = list(tickers.tickers)

        articles = []
        # Get the news articles for the stock from Yahoo Finance, and add the article text to the list
        for i, ticker in tqdm(
            enumerate(tickers_list),
            desc="• Downloading news articles for "
            + str(len(tickers_list))
            + " symbols from Yahoo Finance",
        ):
            news = tickers.tickers[ticker].news[:3]
            #article_urls = [news['link'] for news in news]
            article_info = []
            for n in news:
                article_info.append({'Title': n['title'], 'Link': n['link']})
            # Add the stock symbol and the news articles to the list
            articles.append({'Symbol': ticker, 'Articles': article_info})

        scraped_articles = []        
        # Get the news articles for the stock from Yahoo Finance, and add the article text to the list

        for yahoo_news in articles:
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
            scraped_articles.append({'Symbol': yahoo_news['Symbol'], 'Articles': articles_text})
        # Return the list of symbols with the news articles
        return scraped_articles
    
    ########################################################
    # Define the get_loser_tickers function
    ########################################################
    def get_tickers(self, tickers):
        """
        Get the list of tickers
        :return: list: tickers
        """
        # Create a CachedLimiterSession object, which is a Session object with caching and rate limiting, to avoid getting blocked
        session = CachedLimiterSession(
            limiter=Limiter(RequestRate(2, Duration.SECOND*5)),  # max 2 requests per 5 seconds
            bucket_class=MemoryQueueBucket,
            backend=SQLiteCache("yfinance.cache"),
        )
        # Get the tickers from Yahoo Finance
        return yf.Tickers(tickers, session=session)

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
            + " symbols from Yahoo Finance",
        ):
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
    # Losers: https://finance.yahoo.com/losers?offset=0&count=100
    ########################################################
    def yahoo_scrape_symbols(self, yahoo_url, top=100, asset_type='stock'):
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
    