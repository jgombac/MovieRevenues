import tensorflow as tf
import pandas as pd


estimator = None

def initialize():
    feature_columns = []
    hidden_units = [40, 60, 20]
    # po potrebi priredit optimizer
    estimator = tf.estimator.DNNRegressor(
        feature_columns=feature_columns,
        hidden_units=hidden_units,
    )


def label_split(data, label_column):
    label = data[label_column]
    data.drop(label_column, axis=1, inplace=True)
    return data, label


def build_input(features, labels, epochs, shuffle):
    input_fn = None
    if labels:
        input_fn = tf.estimator.inputs.pandas_input_fn(
            x=features,
            y=labels,
            num_epochs=epochs,
            shuffle=shuffle
        )
    else:
        if labels:
            input_fn = tf.estimator.inputs.pandas_input_fn(
                x=features,
                num_epochs=epochs,
                shuffle=shuffle
            )
    return input_fn


def train(input):
    if not estimator:
        initialize()
    features, label = label_split(input, "revenue")
    estimator.fit(input_fn=build_input(features, label, None, True), steps=10000)
    return True

def predict(input):
    if not estimator:
        initialize()
    return estimator.predict(input_fn=build_input(input, None, 1, False))



# INPUTS
# keywords: categorical
# actors: categorical
# director: categorical
# scenario: categorical
# budget: numerical

# ker je kategoričnih vrednosti veliko, se zna zgodit da bo input layer imel nad 20000 nevronov
# v tem primeru treba testirat ce bo numerical vredu (najverjetneje ne)
# drugače pa vektorizirat pred tem


# OUTPUTS
# revenue: numerical


def prepare_data(raw_data):
    pass

