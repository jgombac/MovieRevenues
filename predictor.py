import tensorflow as tf
import pandas as pd
import numpy as np

estimator = None

def initialize(features, labels):
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


def split_data(data):
    features = data.drop(["ID_TMDB", "REVENUE"], axis=1)
    labels = data["REVENUE"]
    return features, labels


def read_train_data():
    data = pd.read_feather("train_save.feather", nthreads=8)
    return data


def init():
    data = read_train_data()
    features, labels = split_data(data)
    print("FEATURES", features)
    print("LABELS", labels)

if __name__ == "__main__":
    init()

