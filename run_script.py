#!/Users/alastairong/anaconda3/bin/python

"""
Run live prediction model
"""

# Import packages
import numpy as np
import pandas as pd
import sys
from datetime import datetime, timedelta
import time
sys.path.append('../..')
import pickle
import krakenex
from pykrakenapi import KrakenAPI

import h5py
from keras import optimizers
from keras.models import Sequential
from keras.layers import Dense, Dropout, LSTM, MaxPooling2D, Conv2D
from keras.callbacks import ModelCheckpoint, Callback
import matplotlib.pyplot as plt
import missingno as msno
import config

from Classifier.data_processing import processor
from Classifier.prediction_model import LSTM_net

# Define global variables
interval = 1440 # 1440 minutes = 1 day
sequence_length = 4
input_size = 35
learning_rate = 0.00001 # Only needed to define optimiser. Not used in prediction
input_size = (sequence_length, input_size)

# Initialise model
LSTM_network = LSTM_net(input_size, learning_rate)

# Load the model weights with the best validation loss.
LSTM_network.model.load_weights('/Users/alastairong/Documents/GitHub/traderbot/saved_models/LSTM_weights.hdf5')

live_trading = False

# Load normalisation data
with open('/Users/alastairong/Documents/GitHub/traderbot/pickles/normalisation.pickle', 'rb') as f:
    x_mean, x_std, y_mean, y_std = pickle.load(f)

# Load latest trade positions from log
try:
    log = pd.read_csv('trade_log.csv')
    cash = log[-1]['Cash_Position']
    position = log[-1]['BTC_Position']
    print("loaded")
except:
    cash = 1000
    position = 0
    print(cash)
    print(position)

# Run prediction script
Date = datetime.now()
input_data, current_price = processor.live_download(interval, sequence_length, x_mean, x_std)
raw_prediction = LSTM_network.model.predict(input_data)
expected_growth = raw_prediction.item() * y_std + y_mean
predicted_price = current_price * (1 + expected_growth)

# Simple trading algorithm.
if expected_growth > 0:
    action = "buy"
    position += cash / current_price * 0.997 # 0.997 to account for fees
    volume = position
    cash = 0
if expected_growth < 0:
    action = "sell"
    cash += position * current_price * 0.997
    volume = position
    position = 0

if live_trading:
    # call Kraken API to make trades here
    api = krakenex.API(config.key, config.secret)
    k = KrakenAPI(api)
    k.add_standard_order('XXBTZUSD', action, 'market', volume)

# Reporting and logging
log_data = np.array([Date, action, current_price, predicted_price, cash, position, (cash + position * current_price)]).reshape(-1,7)
log = pd.DataFrame(log_data, columns=["Date", "Action", "Current Price", "Predicted_Price", "Cash_Position", "BTC_Position", "Portfolio_Value"])
log.to_csv('/Users/alastairong/Documents/GitHub/traderbot/test_log.csv', encoding='utf-8', index=True)

print("{}: {}. Price expected to change from {} to {}. Portfolio value of {}".format(Date, action, current_price, predicted_price, (current_price * position + cash)))
