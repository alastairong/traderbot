"""
Run live prediction model
"""

import config
import h5py
from data_processing import processor
from prediction_model import Neural_Net

# Download datasets
window = 5 # Prime the model with the last 5 intervals
predict = 6 # Predict the next 6 time periods (30 min)
seed_data = processor.live_download(config, window)

# Initialize Neural Net
weights_file = #
learning_rate = #
batch_size = #
network = Neural_Net()
network.model.load_weights(weights_file)

# Start predicting
prediction = network.predict(seed_data)
Wait 1 interval
while True: # Need to think through how to deal with the loop, data seeding, and LSTM data feed
    data_feed = processor.live_download(config, 1)
    prediction = network.predict(data_feed)
    Wait 1 interval

# Save prediction data for analysis and future training
