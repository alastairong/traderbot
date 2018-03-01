"""
Reddit Data Collection and Processing
For a selected subreddit. Collect posts and comments along with timestamps,
feed them to Google NLP API for sentiment/entity analysis, and summarize a
score for each time period.
"""

# Import basic packages
import os
from urllib.request import urlretrieve, urlopen
import time
from datetime import datetime, timedelta
import pytz
import tqdm

# Import data science packages
import numpy as np
import pandas as pd

# Import reddit related packages
import praw
import pdb
import re

# Import Google NLP API
from google.cloud import language
from google.cloud.language import enums
from google.cloud.language import types
from google.oauth2 import service_account

from traderbot.Preprocessing.base_class import Preprocessor
from traderbot.Preprocessing.helpers import date_to_iso8601, date_to_interval

# Define
class Reddit_Scanner(Preprocessor):
    """
    For a given subreddit:
    1. Collect all comments made in a time period
    2. Send them to Google Natural Language Processing AP for entity-sentiment analysis
    3. Summarise results in a dataframe. Preprocessor should account for both the level of sentiment append
    the number of comments being made (magnitude/vocality)
    """

    def __init__(self, interval, start_time, end_time):
        """
        :interval: Interval in minutes
        :start_time: How far back to collect data, as a datetime object
        :end_time: Latest datapoint, as a datetime object
        """
        self.interval = interval
        self.start_time = start_time
        self.end_time = end_time

    def authenticate(self, client_ID, client_secret, include_sentiment_analysis=False):
        """
        Authenticate with Reddit and Google Cloud
        """
        self.reddit = praw.Reddit(user_agent='Comment Extraction (by /u/kibbl3)',
             client_id=client_ID, client_secret=client_secret)

        # Initialize Google NLP API credentials
        self.include_sentiment_analysis = include_sentiment_analysis
        if self.include_sentiment_analysis:
            creds = service_account.Credentials.from_service_account_file(
            './Traderbot-5d2e0a1af0a9.json')
            self.NLP_client = language.LanguageServiceClient(credentials=creds)

    def get_training_data(self, topic):
        """
        Call API to collect target dataset over the defined time period. Returns fully formatted data as a
        dataframe and summarized into intervals.
        :topic: Subreddit to collect data for
        Note that training data here has not yet been split into data vs. targets
        """
        start_timestamp = self.start_time.replace(tzinfo=pytz.utc).timestamp()
        end_timestamp = self.end_time.replace(tzinfo=pytz.utc).timestamp()

        raw_comments = self.get_raw_comments(topic)
        raw_comments = self.scrub_reddit_comments(raw_comments, start_timestamp, end_timestamp)

        # Calls GCloud NLP API for sentiment analysis
        if self.include_sentiment_analysis:
            for i, row in raw_comments.iterrows():
                text = row['Comment_Text']
                document = types.Document(
                    content=text,
                    type=enums.Document.Type.PLAIN_TEXT)
                sentiment = self.NLP_client.analyze_sentiment(document=document)
                row['Sentiment_Score'] = sentiment.document_sentiment.score
                row['Sentiment_Magnitude'] = sentiment.document_sentiment.magnitude

        # Convert dataframe from objects to float for numerical analysis
        raw_comments[['Sentiment_Score','Sentiment_Magnitude', "ETH_Score", "ETH_Magnitude","BTC_Score", "BTC_Magnitude", "LTC_Score", "LTC_Magnitude"]] = raw_comments[['Sentiment_Score','Sentiment_Magnitude',"ETH_Score", "ETH_Magnitude", "BTC_Score", "BTC_Magnitude", "LTC_Score", "LTC_Magnitude"]].apply(pd.to_numeric)
        print("just before groupby: /n", raw_comments.head())
        # Group comments into periods to aggregate sentiment
        reddit_sentiment = raw_comments.groupby('period')
        reddit_sentiment = reddit_sentiment.agg({
            'Comment_ID': 'count', # Gonna cheat and use this existing column as the count
            'Sentiment_Score': np.mean,
            'Sentiment_Magnitude': np.mean, # Average magnitude because don't want to skew to many low magnitude comments
            'BTC_Score': np.mean,
            'BTC_Magnitude': np.mean,
            'ETH_Score': np.mean,
            'ETH_Magnitude': np.mean,
            'LTC_Score': np.mean,
            'LTC_Magnitude': np.mean,
        })

        reddit_sentiment = reddit_sentiment.rename(columns={'Comment_ID': 'Volume'})

        return reddit_sentiment

    def get_test_data(self):
        """
        Call API to collect data for 1 time period only starting from now. Returns fully formatted data in dataframe.
        Note that this function will be significantly simpler than get_training_data since there is no need to loop through
        multiple time periods and aggregate multiple API calls
        """
        # Get all comments for rising or controversial posts
        raise NotImplementedError("{} must override step()".format(self.__class__.__name__))

    def get_raw_comments(self, subreddit):
        """
        Creates and populates a dataframe of raw_comments for a given subreddit
        """

        subreddit = self.reddit.subreddit(subreddit)

        # Define the raw comment output DataFrame structure
        columns = ["Post_ID", "Post_Date", "Post_Score",
            "Comment_ID", "Comment_Text", "Comment_Date",
            "Comment_Score", "Replying_to_ID",
            "Sentiment_Score", "Sentiment_Magnitude",
            "ETH_Score", "ETH_Magnitude",
            "BTC_Score", "BTC_Magnitude",
            "LTC_Score", "LTC_Magnitude"]
        raw_comments = pd.DataFrame([], columns=columns, dtype=float)

        all_posts = subreddit.hot(limit=5000)
        counter = 0
        for post in all_posts:
                # TODO: look into how praw manages connection timeout
            post.comments.replace_more(limit=None)
            for comment in post.comments.list():
                #print(datetime.fromtimestamp(comment.created_utc))
                new_line = [[
                    post.id, post.created_utc, post.score,
                    comment.id, comment.body, comment.created_utc,
                    comment.score, comment.parent_id,
                    0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0]] # 0.0 placeholders until NLP results returned
                raw_comments = raw_comments.append(pd.DataFrame(new_line, columns=columns),ignore_index=True)
            counter += 1
            print('post {}'.format(counter))
        return raw_comments

    def scrub_reddit_comments(self, raw_comments, start_timestamp, end_timestamp):
        """
        Scrubs a reddit dataframe defined by get_raw_comments to only the time period, and also cleans up naming, time periods, etc
        """

        # Scrub data to remove low value comments
        #print("raw shape is ", raw_comments.shape)
        raw_comments = raw_comments[raw_comments['Comment_Date'] > start_timestamp]
        #print("shape after removing comments before start date is ", raw_comments.shape)
        raw_comments = raw_comments[raw_comments['Comment_Date'] < end_timestamp]
        #print("shape after removing comments after end date is ", raw_comments.shape)
        discarded_comments = raw_comments[raw_comments['Comment_Text'].map(len) < 100]
        raw_comments = raw_comments[raw_comments['Comment_Text'].map(len) >= 100]
        #print("final shape is ", raw_comments.shape)
        #print(discarded_comments[:50]['Comment_Text']) # Check what's being discarded_comments

        # Add periods for later aggregation
        raw_comments['datetime'] = pd.to_datetime(raw_comments['Comment_Date'], unit='s') # Reformat unix timestamp as datetime
        raw_comments['period'] = raw_comments['datetime'].map(lambda x: date_to_interval(x, self.interval))

        # Replace synonyms for ETH, BTC, and LTC. If multiple terms comes up (e.g. ETH and ether)
        raw_comments = raw_comments.apply(lambda x: x.astype(str).str.lower())
        raw_comments = raw_comments.replace("ethereum", "ETH")
        raw_comments = raw_comments.replace("ethereum's", "ETH")
        raw_comments = raw_comments.replace("eth's", "ETH")
        raw_comments = raw_comments.replace("ether's", "ETH")
        raw_comments = raw_comments.replace("ether", "ETH")
        raw_comments = raw_comments.replace("ethers", "ETH")
        raw_comments = raw_comments.replace("etherium", "ETH")
        raw_comments = raw_comments.replace("eth/usd", "ETH")
        raw_comments = raw_comments.replace("eth/eur", "ETH")
        raw_comments = raw_comments.replace("eth/cny", "ETH")
        raw_comments = raw_comments.replace("bitcoin", "BTC")
        raw_comments = raw_comments.replace("bitcoin's", "BTC")
        raw_comments = raw_comments.replace("btc's", "BTC")
        raw_comments = raw_comments.replace("bitc", "BTC")
        raw_comments = raw_comments.replace("bitcoins", "BTC")
        raw_comments = raw_comments.replace("btc/usd", "BTC")
        raw_comments = raw_comments.replace("btc/eur", "BTC")
        raw_comments = raw_comments.replace("btc/cny", "BTC")
        raw_comments = raw_comments.replace("litecoin", "LTC")
        raw_comments = raw_comments.replace("litcoin", "LTC")
        raw_comments = raw_comments.replace("litecoin's", "LTC")
        raw_comments = raw_comments.replace("litcoin's", "LTC")
        raw_comments = raw_comments.replace("ltc's", "LTC")
        raw_comments = raw_comments.replace("ltc/usd", "LTC")
        raw_comments = raw_comments.replace("ltc/eur", "LTC")
        raw_comments = raw_comments.replace("ltc/cny", "LTC")

        return raw_comments
