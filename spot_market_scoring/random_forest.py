import pandas as pd
import numpy as np
import logging
import sklearn.ensemble as skl_ens
import sklearn.metrics as skl_met

logger = logging.getLogger(__name__)

# transform a time series dataset into a supervised learning dataset
def series_to_supervised(data, n_in=1, n_out=1, drop_na=True):
    # n_vars = 1 if isinstance(data, list) else data.shape[1]
    df = pd.DataFrame(data)

    cols = list()
    # input sequence (t-n, ... t-1)
    for i in range(n_in, 0, -1):
        cols.append(df.shift(i))
    # forecast sequence (t, t+1, ... t+n)
    for i in range(0, n_out):
        cols.append(df.shift(-i))
    # put it all together
    agg = pd.concat(cols, axis=1)
    # drop rows with NaN values
    if drop_na:
        agg.dropna(inplace=True)

    return agg.values


# split a univariate dataset into train/test sets
def train_test_split(data, n_test):
    return data[:-n_test, :], data[-n_test:, :]


# fit an random forest model and make a one step prediction
def random_forest_forecast(train, test_X):
    # transform list into array
    train = np.asarray(train)
    # split into input and output columns
    train_X, train_y = train[:, :-1], train[:, -1]

    # fit model
    model = skl_ens.RandomForestRegressor(n_estimators=50)
    model.fit(train_X, train_y)
    # make a one-step prediction
    y_hat = model.predict([test_X])
    return y_hat[0], model


# walk-forward validation for univariate data
def walk_forward_validation(data, n_test):
    predictions = list()
    # split dataset
    train, test = train_test_split(data, n_test)
    # seed history with training dataset
    history = [x for x in train]
    models = []
    # step over each time-step in the test set
    for i in range(len(test)):
        # split test row into input and output columns
        test_X, test_y = test[i, :-1], test[i, -1]
        # print('test_X ->', list(test_X))
        # fit model on history and make a prediction
        y_hat, model = random_forest_forecast(history, test_X)
        # store forecast in list of predictions
        predictions.append(y_hat)
        # add actual observation to history for the next loop
        history.append(test[i])
        models.append(model)
        # summarize progress
        # print('> exp = %.4f, pred = %.4f, acc = %.6f' %
        #       (test_y, y_hat, 1 - abs((test_y - y_hat) / test_y)))
    # estimate prediction error
    error = skl_met.mean_absolute_error(test[:, -1], predictions)

    return error, test[:, -1], predictions, models


def train_spot_price_prediction_model(df: pd.DataFrame, tag: str = None):
    # load the dataset

    # transform the time series data into supervised learning
    data = series_to_supervised(df.values, n_in=6)

    # evaluate
    mae, y, y_pred, rfs = walk_forward_validation(data, 12)

    return rfs

def predict(index,model):
    # construct an input for a new prediction
    row = index[-6:]
    # make a one-step prediction
    yhat = model.predict(np.asarray([row]))
    # print('Input: %s, Predicted: %.5f' % (row, yhat[0]))
    return yhat[0]

def get_predicted_price(ins_dict=None,year:int=2021):
    #output in one file
    #read every file

    ins_pred={}
    for ins,df in ins_dict.items():

        #for each availability zone
        az_pred={}
        for az in df.columns:
            if df[az].var() <= 0.001:
                az_pred[az] = df[az][-1]
                continue
            data = df[az].dropna()
            try:
                rfs = train_spot_price_prediction_model(data)
                new_price = predict(data, rfs[-1])
                az_pred[az] = new_price
            except Exception as e:
                logger.error(f'[ERR] {ins}: {e}')
                az_pred[az] = df[az][-1]
                continue

        ins_pred[ins] = az_pred

    return ins_pred