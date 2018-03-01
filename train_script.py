"""
Run training script
"""

import config
from data_processing import processor
from prediction_model import Neural_Net

# Set parameters from command line arguments
redownload = redownload

# Download datasets
if redownload or not os.path.isfile(pickle_filepath):
    train_data = processor.historical_download(config, start_time, end_time)
    valid_data = processor.historical_download(config, start_time, end_time)
    test_data = processor.historical_download(config, start_time, end_time)

    # Split train, valid, and test data and targets and convert to numpy arrays
    train_data, train_targets = processor.generate_x_y(training_data)
    valid_data, valid_targets = processor.generate_x_y(valid_data)
    test_data, test_targets = processor.generate_x_y(test_data)

    # Save data in pickle format
    with open('training.pickle', 'a') as f:
        pickle.dump((train_data, train_targets), f)

    with open('valid.pickle', 'a') as f:
        pickle.dump((valid_data, valid_targets), f)

    with open('test.pickle', 'a') as f:
        pickle.dump((test_data, test_targets), f)

else:
    with open('training.pickle', 'rb') as f:
        train_data, train_targets = pickle.load(f)

    with open('valid.pickle', 'rb') as f:
        valid_data, valid_targets = pickle.load(f)

    with open('test.pickle', 'rb') as f:
        test_data, test_targets = pickle.load(f)

# Initialize Neural Net
learning_rate = #
batch_size = #
network = Neural_Net(learning_rate, batch_size)

# Start training
network.train()

# Print training logs and charts
