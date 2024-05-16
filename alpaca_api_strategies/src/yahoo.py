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

# from requests_html import HTMLSession

import requests
from bs4 import BeautifulSoup as bs

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
        # Iterate over the list of tickers and get the recommendations summary
        for i, ticker in tqdm(
            enumerate(tickers_list),
            desc="• Downloading recommendations for "
            + str(len(tickers_list))
            + " symbols from Yahoo Finance",
        ):
            summary = tickers.tickers[ticker].recommendations_summary
            summary = summary.dropna()
  
            # If the summary is not empty, get the recommedations of the stock and add the news articles to the list, from Yahoo Finance
            if not summary.empty:
                recommendations.append({'Symbol': ticker, 'Recommendations': {'strongBuy': summary['strongBuy'].sum(), 'buy': summary['buy'].sum(), 'hold': summary['hold'].sum(), 'sell': summary['sell'].sum(), 'strongSell': summary['strongSell'].sum()}})
            else:
                recommendations.append({'Symbol': ticker, 'Recommendations': None})
        # Create a DataFrame with the recommendations
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
            # Get the news articles for the stock from Yahoo Finance and get the first 3 articles
            news = tickers.tickers[ticker].news[:3]
            #article_urls = [news['link'] for news in news]
            article_info = []
            # Add the news article links and symbols to the list
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

                request = requests.get(article['Link'])
                soup = bs(request.content, 'lxml')
                article_text = soup.find('div', class_='caas-body').get_text(separator=' ', strip=True) # type: ignore
                
                articles_text.append({'Title': article['Title'], 'Symbol': yahoo_news['Symbol'], 'Article': article_text})
                #session.close()
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
    # Define the get_raw_info function
    ########################################################
    def get_raw_info(self, site):
        """
        Get the raw information from the given site
        :param site: Site URL
        return: DataFrame with the raw information
        """
        # Create a HTMLSession object
        # session = HTMLSession()
        # response = session.get(site)
        request = requests.get("https://finance.yahoo.com/losers?offset=0&count=100")
        soup = bs(request.content, 'html.parser')
        tables = pd.read_html(soup.prettify())
        df = tables[0].copy()
        df.columns = tables[0].columns
        # Get the tables from the site
        # tables = pd.read_html(response.html.raw_html)
        # df = tables[0].copy()
        # df.columns = tables[0].columns
        # Close the session
        #session.close()
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
    