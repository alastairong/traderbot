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

# Import data science packages
import pandas as pd

# Import reddit related packages
import praw
import pdb
import re
import timesearch

# TODO Import Google NLP API

from base_class import Preprocessor

# Define
class Reddit_Scanner(Preprocessor):
    """
    For a given subreddit:
    1. Collect all comments made in a time period
    2. Send them to Google Natural Language Processing AP for entity-sentiment analysis
    3. Summarise results in a dataframe. Preprocessor should account for both the level of sentiment append
    the number of comments being made (magnitude/vocality)
    """

    def __init__(self, client_ID, client_secret, topic, interval=5, start_time, end_time):
        """
        :topic: Subreddit to collect data for
        :interval: Interval in minutes
        :start_time: How far back to collect data, as a datetime object
        :end_time: Latest datapoint, as a datetime object
        """
        self.reddit = praw.Reddit(user_agent='Comment Extraction (by /u/kibbl3)',
                     client_id=client_ID, client_secret=client_secret)
        self.subreddit = self.reddit.subreddit(topic)
        self.interval = interval
        self.start_time = start_time
        self.end_time = end_time

        # TODO: Initialize Google NLP API credentials here

    def get_training_data(self):
        """
        Call API to collect target dataset over the defined time period. Returns fully formatted data as a
        dataframe and summarized into intervals.
        Note that training data here has not yet been split into data vs. targets
        """

        # Define the raw comment output DataFrame structure
        raw_comments = pd.DataFrame([],
            col=["Post_ID", "Post_Date", "Post_Score",
            "Comment_ID", "Comment_Text", "Comment_Date",
            "Comment_Score", "Replying_to_ID",
            "sentiment_score", "sentiment_magnitude",
            "ETH_score", "ETH_magnitude",
            "BTC_score", "BTC_magnitude",
            "LTC_score", "LTC_magnitude"])

        # Get a dataframe of all comments for TOP subreddits. Top subreddits have the most upvote and downvotes and therefore
        # low-bias high traffic representation of sentiment...or maybe it needs to be new because I need ALL comments to properly calibrate / train
        all_posts = self.subreddit.new(limit=5000)
        for post in top_posts:
             comment_tree = post.comments.replace_more()
             for comment in comment_tree:
                 new_line = [
                    post.id, post.date, post.score,
                    comment.id, comment.body, comment.date,
                    comment.score, comment.parent_id,
                    0.0, 0.0, 0.0, 0.0, 0.0, 0.0] # 0.0 placeholders until NLP results returned
                raw_comments = raw_comments.append(new_line)

        # Scrub data to remove low value comments
        raw_comments = raw_comments.where.comment_date >= self.start_time
        raw_comments = raw_comments.where.comment_date <= self.end_time
        raw_comments, discarded_comments = raw_comments.where.comment_text.len > 10, <= 10
        print(discarded_comments[:50]) # Check what's being discarded_comments
        print(raw_comments[:50]) # Check if there's still anything we can remove

        # Feed list to Google NLP API to return scores
        for comment in raw_comments
            # Get results from sentiment analysis API
            sentiment_analysis = NLP_API.sentiment_analysis(comment.body)
            entity_sentiment_analysis = NLP_API.entity_sentiment_analysis(comment.body)

            # Replace synonyms for ETH, BTC, and LTC. If multiple terms comes up (e.g. ETH and ether)
            # we will automatically take the FIRST term as that will be the highest salience value
            entity_sentiment_analysis.tolowercase().replace("ethereum", "ETH")
            entity_sentiment_analysis.tolowercase().replace("ether", "ETH")
            entity_sentiment_analysis.tolowercase().replace("ethers", "ETH")
            entity_sentiment_analysis.tolowercase().replace("etherium", "ETH")
            entity_sentiment_analysis.tolowercase().replace("bitcoin", "BTC")
            entity_sentiment_analysis.tolowercase().replace("bitc", "BTC")
            entity_sentiment_analysis.tolowercase().replace("bitcoins", "BTC")
            entity_sentiment_analysis.tolowercase().replace("litecoin", "LTC")
            entity_sentiment_analysis.tolowercase().replace("litcoin", "LTC")

            # Update raw_comments dataframe with sentiment analysis score
            comment["sentiment_score"] = sentiment_analysis.sentiment.score
            comment["sentiment_magnitude"] = sentiment_analysis.sentiment.magnitude
            try:
                comment["ETH_score"] = entity_sentiment_analysis.ETH.sentiment.score
                comment["ETH_magnitude"] = entity_sentiment_analysis.ETH.sentiment.magnitude
            try:
                comment["BTC_score"] = entity_sentiment_analysis.BTC.sentiment.score
                comment["BTC_magnitude"] = entity_sentiment_analysis.BTC.sentiment.magnitude

        # Batch comments and scores into interval results (i.e. an interval "score")


        return training_data

    def get_test_data(self):
        """
        Call API to collect data for 1 time period only starting from now. Returns fully formatted data in dataframe.
        Note that this function will be significantly simpler than get_training_data since there is no need to loop through
        multiple time periods and aggregate multiple API calls
        """
        # Get all comments for rising or controversial posts
        raise NotImplementedError("{} must override step()".format(self.__class__.__name__))
