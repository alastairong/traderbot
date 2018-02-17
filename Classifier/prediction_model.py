"""
Classifier Neural Network
Defines the model structure and key functions
"""


class Neural_Net:
    """
    Convolutional recurrent neural network setup. Rationale:
    Convolutional network scans each time interval to recognise patterns across the dataset
    LSTM cells recall sequence of convolutional patterns
    """

    def __init__(self, learning_rate=0.001, batch_size=64):
        # Sets up the network in Keras, including optimisers
        # Starting point is a 4 layer CNN + 2 layers of LSTM
        pass


    def train(self):
        # Call model train function and initiate data logging and weight saving
        pass

    def predict(self):
        # Call the model to predict next 5 intervals based on historical data
        # TODO: A probability / confidence score would be very interesting...
        pass


class Model:
    """
    Actual model structure. Keep is separate to keep code clean?
    """
