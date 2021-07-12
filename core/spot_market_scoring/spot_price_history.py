#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""spot_price_history.py

    1. fetch spot price history by region and by instance type (386)
    2. write to local files
    3. read from local
    4. calculate average savings scores by region/systems
    5. save scores

    Functions:
        get_spot_price_history
        get_spot_price_history_in_all_region
        upload_all_spot_price_history_to_s3
        get_savings_statistics_by_region
        generate_spot_instance_list
        read_spot_price_history

    Helper Functions:

"""
from concurrent_task import *
from mappings import *
from utils import *
from config import conf
import pandas as pd
import datetime
import boto3
import os
import json
import time


def get_spot_price_history(client, region, days_back: int = 30,
                           productDescription: str = None,
                           availabilityZone: str = None,
                           instanceType: str = None) -> dict:
    """
    Get spot price history for up to 90 days back in client's region
    :param client: ec2 client
    :param days_back: [0,90]
    :param availabilityZone: azs in clients's region
    :param productDescription: operation systems,
    """
    # set filters
    print(f'loading {region}, {productDescription} data from AWS')

    filters = {}
    start_time = datetime.datetime.now() - datetime.timedelta(days=days_back)
    filters['StartTime'] = start_time
    if availabilityZone:
        filters['AvailabilityZone'] = availabilityZone
    if instanceType:
        filters['InstanceTypes'] = [instanceType]
    if productDescription:
        filters['ProductDescriptions'] = [productDescription]

    try:
        indices = None
        values = []
        for resp in paginate(client.describe_spot_price_history, **filters):
            if indices is None:
                indices = list(resp.keys())
            values.append(list(resp.values()))

    except Exception as e:
        print(f'[ERR] {region}, {productDescription}: {e}')


    return values


def read_from_local(path, region,
                    system='Windows (Amazaon VPC)',
                    instanceType='t3.micro', year: int = 2021) -> pd.DataFrame:
    i_path = os.path.join(path, 'spot_price_history',
                          'Region=' + region,
                          'ProductDescription=' + ParquetTranscoder.encode(system),
                          'InstanceType=' + instanceType,
                          f'{year}.csv')
    return pd.read_csv(i_path)


def read_from_s3(s3client, region: str = 'us-east-1',
                 system: str = SYSTEM_LIST[0],
                 year: int = 2021, days_back=90):
    try:
        key = (f'ec2/spot_price_history/Region={region}/'
               f'ProductDescription={ParquetTranscoder.encode(system)}/'
               f'{year}.csv')

        response = s3client.get_object(
            Bucket='stompy-aws-dataset',
            Key=key
        )

        df = pd.read_csv(response.get('Body'))
        df['Timestamp'] = df['Timestamp'].apply(pd.Timestamp)
        start_day = datetime.datetime.today()-datetime.timedelta(days=days_back)
        df = df.loc[df.Timestamp >= start_day].copy()
        df['SpotPrice'] = df['SpotPrice'].astype('float')

        # spot_prices.set_index("Timestamp", inplace=True)
        # df["SpotPrice"] = df.SpotPrice.astype(float)
        # df.set_index("Timestamp", inplace=True)
        # df.sort_index(inplace=True)

        return df
    except Exception as e:
        raise


def write_to_s3(df,s3client,region:str,system:str,year:int):

    try:
        key = (f'ec2/spot_price_history/Region={region}/'
               f'ProductDescription={ParquetTranscoder.encode(system)}/'
               f'{year}.csv')
        df['SpotPrice'] = df['SpotPrice'].astype('string')
        from io import StringIO
        # df['Timestamp'] = df.index
        csv_buffer = StringIO()
        df.to_csv(csv_buffer,index=False)
        s3client.put_object(
            Bucket='stompy-aws-dataset',
            Key=key,
            Body=csv_buffer.getvalue()
        )

        return True
    except Exception as e:
        raise


def update_spot_price_history_in_all_region(clients: [boto3.client],
                                            path: str, year: int, days_back=30,
                                            s3client: boto3.client = None, local: bool = True):
    """
    Get spot price history for all regions and save to local path
    :param clients: list of ec2 clients in all region
    :param days_back: [0,90]
    :param path: files will be saved in path/spot_price_history/..
    :param year: current year, corresponding to different file
    """
    if not os.path.exists(path):
        print(f"{path} does not exists")

    print('----Fetching data from AWS boto3----')
    start = time.time()
    regions = sorted(REGION_CODE_MAP.keys())
    executor = ThreadPoolExecutor()
    response = ConcurrentTaskPool(executor).add([
        ConcurrentTask(executor, task=update_spot_price_history,
                       t_args=(clients[region], s3client,region,
                               prod,days_back,year))
        for prod in SYSTEM_LIST for region in regions
    ]).get_results()
    end = time.time()
    print(f'Time used: {end - start}')
    print("Finished saving all spot price history data into S3 bucket")
    # update SI_List
    # generate_spot_instance_list(response)
    return response


def update_spot_price_history(client, s3client, region, system,days_back,year):
    """
    upload local data to s3 bucket
    with key: regions/productdescription/instancttype/year.csv
    :param client: s3 clients
    :param path: read file from path/spot_price_history
    """

    response = get_spot_price_history(client,region,days_back,system)
    df = to_dataframe(response)
    # print(f'Appending {region}, {system}')
    # df['Timestamp'] = df.index


    try:
        prev_df = read_from_s3(s3client, region, system, year)

        df = pd.concat((prev_df,df))
        df['SpotPrice'] = df['SpotPrice'].astype('float')
        df.sort_values(['InstanceType', 'AvailabilityZone', 'Timestamp'],inplace=True)
        df.drop_duplicates(inplace=True)
        df.reset_index(drop=True,inplace=True)

    except Exception as e:
        print(f'[ERR] {region} {system} : {e}')
        # print(f'{region} {system} data may not exist, please update data first')


    write_to_s3(df, s3client, region, system, year)
    print(f"{region} {system} updated")

    return response


def get_savings_statistics(region: str, system:None,
                           year: int=2021, period="Month",
                           s3client=None, days_back=90) -> (dict, dict):


    avg_entries, std_entries = {}, {}
    df = read_from_s3(s3client=s3client, region=region, system=system, year=year)
    df.sort_values(['InstanceType', 'AvailabilityZone', 'Timestamp'], inplace=True)
    df.drop_duplicates(inplace=True)
    df.set_index("Timestamp", inplace=True)
    ins_dict = {}
    for ins, i_df in df.groupby(by='InstanceType'):
        try:

            i_df = reformat(i_df)
            # i_df['Timestamp'] = i_df.index.date
            ins_dict[ins] = i_df
            response_avg, response_std = get_mean_and_std(i_df, period)
            avg_entries[ins] = response_avg
            std_entries[ins] = response_std
        except Exception as e:
            print(f'[ERR] {region} {system} {ins}: {e}')
            continue
    return avg_entries, std_entries, ins_dict



def get_spot_instance_list(s3client):
    try:
        key = 'ec2/spot_instance_list.json'

        response = s3client.get_object(
            Bucket='stompy-aws-dataset',
            Key=key,
        )['Body']
        si_list = json.loads(response.read())

        return si_list
    except Exception as e:
        print(f'[ERR] : {e}')
        raise


def generate_spot_instance_list(response) -> dict:
    """
    read or generate spot instance list based on saved local directories.
    :param path: path ends with ../spot_price_history
    :param local: read from local or remote
    """

    # file_path = os.path.join(path, 'spot_price_history')
    #
    # if not os.path.exists(file_path):
    #     print(f"{file_path} not found, please set local to False or",
    #           f"run get_spot_price_history_in_all_region first")
    #     raise FileNotFoundError
    if not response:
        print('Empty Response')
        return
    else:
        region_dict, system_dict = {}, {}
        for res in response:
            df = to_dataframe(res)
            region = df['AvailabilityZone'][0][:-1]
            system = df['ProductDescription'][0]
            system = system+' (Amazon VPC)'
            ins = df['InstanceType'].unique().tolist()

            system_dict[system] = ins
            region_dict[region] = system_dict
        try:
            key = 'ec2/spot_instance_list.json'

            s3client.put_object(
                Bucket='stompy-aws-dataset',
                Key=key,
                Body=json.dumps(region_dict)
            )

        except Exception as e:
            print(f'[ERR] {region} {system} : {e}')
            raise

        return region_dict



def to_dataframe(response: dict) -> pd.DataFrame:
    indices = ['AvailabilityZone', 'InstanceType', 'ProductDescription', 'SpotPrice', 'Timestamp']
    if not response:
        print("Empty Response")
        return
    spot_prices = pd.DataFrame(response, columns=indices)
    # set data format
    spot_prices["Timestamp"] = spot_prices['Timestamp'].astype('datetime64[ns]')
    # spot_prices.set_index("Timestamp", inplace=True)
    #spot_prices["SpotPrice"] = spot_prices.SpotPrice.astype(float)

    # spot_prices.set_index("Timestamp", inplace=True)
    # spot_prices.sort_index(inplace=True)

    return spot_prices


def reformat(spot_prices: pd.DataFrame, resolution: str = '1D') -> pd.DataFrame:
    try:
        az_df_dict = {az: pd.Series(df['SpotPrice'], name=az) for az, df in spot_prices.groupby("AvailabilityZone")}
        spot_prices = pd.concat(list(az_df_dict.values()), axis=1)
        spot_prices.loc[pd.Timestamp.now()] = spot_prices.tail(1).copy().iloc[0]
        spot_prices.fillna(method='ffill', inplace=True)
        spot_prices.fillna(method='bfill', inplace=True)

        spot_prices = spot_prices.resample(resolution).ffill()
        spot_prices.fillna(method='bfill', inplace=True)
        # spot_prices.columns = [f'{instanceType}-{c}' for c in spot_prices]
    except Exception as e:
        raise
    return spot_prices


def get_mean_and_std(df, period: str) -> (dict, dict):
    """
    helper func: read file and get mean/std from timeseries data
    :param filepath: path that contains file
    :param period: time window to calculate mean/std of price by
    return two statistics as dict
    """
    period_candidates = {'Day': 1,
                         'Week': 7,
                         '2 Weeks': 14,
                         'Month': 30,
                         '3 Months': 90}

    if period not in period_candidates.keys():
        print("Value must be from one of the following: /n", period_candidates.keys())
        return

    if period_candidates[period] < len(df):
        idx = len(df)
    else:
        idx = period_candidates[period]

    scores = df.iloc[-idx:]
    # print(scores.mean(axis=0),scores.std(axis=0))
    return scores.mean(axis=0).to_dict(), scores.std(axis=0).to_dict()


# function for writing timestamps to files in case you forgot to save it
# def add_datetime(path: str, year: int, region, product_description, start_time='2021-03-27',
#                  end_time=pd.Timestamp.today()):
#     index = pd.date_range(start_time, end_time, freq='D')
#     sys = product_description
#     i_path = os.path.join(path,
#                           'Region=' + region,
#                           'ProductDescription=' + ParquetTranscoder.encode(sys))
#
#     for f0 in listdir_nohidden(i_path):
#
#         filepath = os.path.join(i_path, f0, str(year) + '.csv')
#
#         i_df = pd.read_csv(filepath)
#         if len(i_df) != 91:
#             print('modified "%s"' % filepath)
#             print(len(i_df))
#             i_df['Timestamp'] = index[-len(i_df):]
#         else:
#             i_df['Timestamp'] = index
#
#         i_df.to_csv(filepath, index=False)

# def rename_datetime(path: str,year: str=2021):
#     path = os.path.join(path, 'spot_price_history')
#     for f0 in listdir_nohidden(path):
#         for f1 in listdir_nohidden(os.path.join(path,f0)):
#             for f2 in listdir_nohidden(os.path.join(path, f0, f1)):
#                 filepath = os.path.join(path, f0, f1,f2,str(year) + '.csv')
#                 i_df = pd.read_csv(filepath)
#                 i_df.rename(columns={"TimeStamp": "Timestamp"}, inplace=True)
#                 i_df.to_csv(filepath, index=False)

if __name__ == '__main__':

    from user import get_client_list

    clients = get_client_list(**conf.SUB_CREDENTIALS)

    region = 'ap-east-1'
    client = clients[region]

    data_path = conf.DATA_PATH

    # if not os.path.exists(data_path):
    #     os.mkdir(data_path)
    #

    # print("----Example for reading spot price from AWS----")
    # response = get_spot_price_history(client, days_back=1, region=region
    #                                   # instanceType='p3.16xlarge',
    #                                   #availabilityZone='us-east-1a',
    #                                    #productDescription='Windows (Amazon VPC)'
    #                                   )
    # df = to_dataframe(response)
    # print(df)

    # #i_df = reformat(df)
    # print(df)
    #
    # print("----Example for reading local price files----")
    # df = read_from_local(data_path, 'af-south-1', 'Linux/UNIX (Amazon VPC)',
    #                              't3.micro',2021)
    #
    # print(df)

    #

    s3client = boto3.client('s3', **conf.SUB_CREDENTIALS)
    # update_spot_price_history_in_all_region(clients, data_path, 2021, days_back=90, s3client=s3client, local=False)
    # read_from_s3(s3client,'ap-east-1',SYSTEM_LIST[0],2021)
    # upload_all_spot_price_history_to_s3(s3client,data_path)
    # print("----Example for generating a spot instance list----")
    # si_list = generate_spot_instance_list(data_path)

    print("----Example for getting statistics from s3 price files----")
    for region in ['ap-east-1','ap-northeast-3']:
        for system in SYSTEM_LIST:
            avg_res, std_res, ins_dict = get_savings_statistics(region=region,system=system,
                                                                    year=2021, period="Month",
                                                                    s3client=s3client, days_back=30)
    #         print('finished')
    # print(ins_dict['t3.micro'])
    # print(pd.DataFrame(avg_res))
