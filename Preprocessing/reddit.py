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
            "Comment_Score", "Replying_to_ID"])

        # Get a dataframe of all comments for TOP subreddits. Top subreddits have the most upvote and downvotes and therefore
        # low-bias high traffic representation of sentiment...or maybe it needs to be new because I need ALL comments to properly calibrate / train
        all_posts = self.subreddit.new(limit=5000)
        for post in top_posts:
             comment_tree = post.comments()
             for comment in comment_tree:
                 new_line = [
                    post.id, post.date, post.score,
                    comment.id, comment.text, comment.date,
                    comment.score, comment.parent_id]
                raw_comments = raw_comments.append(new_line)
        # Feed list to Google NLP API to return scores


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
