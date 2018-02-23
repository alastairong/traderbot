"""Generic base class for data preprocessing functions."""

class Preprocessor:

    def __init__(self, interval, start_time, end_time):
        """
        Initialise shared parameters.
        :interval: the time interval at which the training data will be collected and batched
        :start_time: earliest point from which data will be collected, as a datetime object
        :end_time: final point at which data will be collected, as a datetime object
        """
        pass

    def get_training_data(self, topic):
        """
        Call API to collect target dataset over the defined time period. Returns fully formatted data as a
        dataframe and summarized into intervals.
        Note that training data here has not yet been split into data vs. targets
        :topic: this will be the API specific target. E.g. a reddit subreddit or GDAX currency pair
        """
        raise NotImplementedError("{} must override step()".format(self.__class__.__name__))

    def get_test_data(self, topic):
        """
        Call API to collect data for 1 time period only starting from now. Returns fully formatted data in dataframe.
        Note that this function will be significantly simpler than get_training_data since there is no need to loop through
        multiple time periods and aggregate multiple API calls
        :topic: this will be the API specific target. E.g. a reddit subreddit or GDAX currency pair
        """
        raise NotImplementedError("{} must override step()".format(self.__class__.__name__))
