# forecast monthly births with random forest
import pandas as pd
import numpy as np
import sklearn.ensemble as skl_ens
import sklearn.metrics as skl_met


from config import conf
from mappings import *

# pd.set_option('display.max_columns', None)
# pd.set_option('display.width', 1000)


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
    # print('MAE: %.6f' % mae)
    # plot expected vs predicted
    # plt.plot(y, label='Expected')
    # plt.plot(y_pred, label='Predicted')
    # plt.legend()
    # plt.show()

    # m_path = os.path.join(conf.DATA_PATH, 'spot_price_predictions')
    # if not os.path.exists(m_path):
    #     os.mkdir(m_path)
    #
    # if tag is not None:
    #     m_path = os.path.join(m_path, tag)
    #     if not os.path.exists(m_path):
    #         os.mkdir(m_path)
    #
    # idx = 0
    # for rf in rfs:
    #     idx += 1
    #     joblib.dump(rf, filename=os.path.join(
    #         m_path,
    #         'RF' + '_' + '_'.join([ParquetTranscoder.encode(str(e)) for e in params]) + '_' +
    #         Dtm.now(fmt=Fmt.YMD_MHS_CONCAT).to_str() + '_' + str(idx) + '.joblib'
    #     ))

    return rfs

def predict(index,model):
    # construct an input for a new prediction
    row = index[-6:]
    # make a one-step prediction
    yhat = model.predict(np.asarray([row]))
    # print('Input: %s, Predicted: %.5f' % (row, yhat[0]))
    return yhat[0]

def get_predicted_price(ins_dict=None,path:str=None,year:int=2021):
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
                print(f'[ERR] {ins}: {e}')
                az_pred[az] = df[az][-1]
                continue

        ins_pred[ins] = az_pred

    return ins_pred





if __name__ == '__main__':

    # d_path = os.path.join(conf.ROOT_PATH, 'data', 'samples')
    #
    # params = 'us-east-1b', 'Linux/UNIX (Amazon VPC)', 'r5.xlarge', 2018
    #
    # a_df = pd.read_csv(os.path.join(d_path, 'r5.xlarge.18-19.us-east-1a.csv')).drop(labels=['Unnamed: 0'], axis=1)
    # b_df = pd.read_csv(os.path.join(d_path, 'r5.xlarge.18-19.us-east-1b.csv')).drop(labels=['Unnamed: 0'], axis=1)
    # c_df = pd.read_csv(os.path.join(d_path, 'r5.xlarge.18-19.us-east-1c.csv')).drop(labels=['Unnamed: 0'], axis=1)
    # d_df = pd.read_csv(os.path.join(d_path, 'r5.xlarge.18-19.us-east-1d.csv')).drop(labels=['Unnamed: 0'], axis=1)
    # f_df = pd.read_csv(os.path.join(d_path, 'r5.xlarge.18-19.us-east-1f.csv')).drop(labels=['Unnamed: 0'], axis=1)

    # a_df = a_df.drop_duplicates(subset=['Timestamp']).reset_index(drop=True)
    # a_df['Timestamp'] = a_df['Timestamp'].apply(lambda x: Dtm(x).to_utm().to_int())

    # dss = (ss[1:].reset_index(drop=True) - ss[:-1].reset_index(drop=True))
    # print(dss)
    #
    # plt.plot(dss.index, dss.values)
    # plt.xticks(rotation=45)
    # # plt.gca().xaxis.set_major_locator(ticker.MultipleLocator(50))
    # plt.show()

    # for i_df in (a_df,b_df):
        #plt.plot(i_df['Timestamp'], i_df['Price'])
        # plt.xticks(rotation=45)
        # plt.gca().xaxis.set_major_locator(ticker.MultipleLocator(50))
        # plt.gca().xaxis.set_major_locator(mdates.HourLocator(byhour=[0,1]))
        #plt.show()
        # train_spot_price_prediction_model(i_df)

    data_path = conf.DATA_PATH
    import spot_price_history as sph
    import boto3
    region='ap-east-1'
    system=SYSTEM_LIST[0]
    s3client = boto3.client('s3', **conf.SUB_CREDENTIALS)
    res_avg, res_std, ins_dict = sph.get_savings_statistics(region=region,
                                                            system=system, s3client=s3client, year=2021,
                                                            period="Month")

    ins_pred = get_predicted_price(ins_dict, 2021)

    print(ins_pred)
    # import json
    # with open('price_prediction_instance.json','w') as f:
    #     json.dump(ins_pred,f)
    # f.close()

    # df = read_spot_price_history(data_path, 'us-east-1', 'Linux/UNIX (Amazon VPC)',
    #                               'r5.xlarge', 2021)
    #
    # # df.set_index('Timestamp', inplace=False)
    # i_df = df[['Timestamp', 'us-east-1a']]
    # i_df_index = i_df.set_index('Timestamp')
    # values = i_df_index.values
    #
    # # print(values)
    # # plt.plot(values)
    # # plt.show()
    #
    # rfs = train_spot_price_prediction_model(i_df_index)
    # predict(values, rfs[-1])

    # with open('price_prediction_full.json', 'r') as f:
    #     region_pred = json.load(f)
    # f.close()
    # for region in region_pred:
    #     print(region)
    #     for system in region_pred[region]:
    #         print(system)
    #         for ins in region_pred[region][system]:
    #             print(int)
    #             break
    #         break
    #     break

