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
import os
import boto3
import pandas as pd
from datetime import datetime, timezone,timedelta
from spot_market_scoring.concurrent_task import *
from spot_market_scoring.mappings import *
from spot_market_scoring.utils import *


def get_spot_price_history(client, region, days_back: int = 30,
                           productDescription: str = None,
                           availabilityZone: str = None,
                           instanceType: str = None) -> list:
    """
    Get spot price history for up to 90 days back in client's region
    :param client: ec2 client
    :param days_back: [0,90]
    :param availabilityZone: azs in clients's region
    :param productDescription: operation systems,
    """
    # set filters

    filters = {}
    start_time = datetime.now(timezone.utc) - timedelta(hours=24*days_back)
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
        return values

    except Exception as e:
        print(f'[ERR] {region}, {productDescription}: {e}')





def read_from_local(path, region,
                    system='Windows (Amazaon VPC)',
                    instanceType='t3.micro', year: int = 2021) -> pd.DataFrame:
    i_path = os.path.join(path, 'spot_price_history',
                          'Region=' + region,
                          'ProductDescription=' + ParquetTranscoder.encode(system),
                          'InstanceType=' + instanceType,
                          f'{year}.csv')
    return pd.read_csv(i_path)


def update_spot_price_history_in_all_region(clients: [boto3.client],
                                             year: int, days_back=30,
                                            s3client: boto3.client = None,dbclient=None,path: str="", local: bool = True):
    """
    Get spot price history for all regions and save to local path
    :param clients: list of ec2 clients in all region
    :param days_back: [0,90]
    :param path: files will be saved in path/spot_price_history/..
    :param year: current year, corresponding to different file
    """
    # if not os.path.exists(path):
    #     print(f"{path} does not exists")

    print('----Fetching data from AWS boto3----')
    regions = sorted(REGION_CODE_MAP.keys())
    executor = ThreadPoolExecutor()
    response = ConcurrentTaskPool(executor).add([
        ConcurrentTask(executor, task=update_spot_price_history,
                       t_args=(clients[region], s3client,dbclient,region,
                               prod,days_back,year))
        for prod in SYSTEM_LIST for region in regions
    ]).get_results()
    return response


def update_spot_price_history(client, s3client, dbclient, region, system,days_back,year):
    """
    upload local data to s3 bucket
    with key: regions/productdescription/instancttype/year.csv
    :param client: s3 clients
    :param
    """
    indices = ['AvailabilityZone', 'InstanceType', 'ProductDescription', 'SpotPrice', 'Timestamp']
    response = get_spot_price_history(client,region,days_back,system)
    df = pd.DataFrame(response, columns=indices)

    try:
        prev_df = read_from_s3(s3client, region, system, year)

        df = pd.concat((prev_df,df))
        df['SpotPrice'] = df['SpotPrice'].astype('float')
        df.sort_values(['InstanceType', 'AvailabilityZone', 'Timestamp'],inplace=True)
        df.drop_duplicates(inplace=True)
        df.reset_index(drop=True,inplace=True)

        write_to_s3(df, s3client, region, system, year)
        # print(f"{region} {system} updated")
    except Exception as e:
        print(f'[ERR]: {e}')
        print(f'{region} {system} update failed')
        # print(f'{region} {system} data may not exist, please update data first')

    db = dbclient['aws_data']
    ins_list = df['InstanceType'].unique().tolist()
    filter = {
        "region": region,
        "system": system
    }
    update = {
        "$set": {"InstanceList": ins_list}
    }
    db.ec2_spot_instances.update(filter, update, upsert=True)

    return response


def get_savings_statistics(region: str, system:str,
                           year: int=2021, period="Month",
                           s3client=None, days_back=90):

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


def get_spot_instance_list(dbclient, region, system):
    try:
        db = dbclient['spot_market_scores']
        response = db.spot_instance_list.find_one({'region': region,
                                                   'system': system},
                                                  {'_id': 0, 'InstanceList':1})
        return response
    except Exception as e:
        print(f'[ERR] : {e}')
        raise


def to_dataframe(response: dict) :
    indices = ['AvailabilityZone', 'InstanceType', 'ProductDescription', 'SpotPrice', 'Timestamp']
    if not response:
        print("Empty Response")
        return
    spot_prices = pd.DataFrame(response, columns=indices)

    return spot_prices


def reformat(spot_prices: pd.DataFrame, resolution: str = '1D') -> pd.DataFrame:
    try:
        az_df_dict = {az: pd.Series(df['SpotPrice'], name=az) for az, df in spot_prices.groupby("AvailabilityZone")}
        spot_prices = pd.concat(list(az_df_dict.values()), axis=1)
        spot_prices.loc[pd.Timestamp.now(tz='UTC')] = spot_prices.tail(1).copy().iloc[0]
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


def write_to_mongo(df,dbclient,region:str,system:str,year:int):
    try:
        df['SpotPrice'] = df['SpotPrice'].astype('string')
        sph = dbclient['spot-market-scores']
        collections = sph['spot_price_history']

        data_dict = df.to_dict(orient='records')
        data = {
            'region': region,
            'system': system,
            'year': year,
            'data': data_dict
        }
        collections.insert_one(data)

        return True
    except Exception as e:
        raise


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
        start_day = datetime.now(timezone.utc)-timedelta(days=days_back)
        df = df.loc[df.Timestamp >= start_day].copy()
        df['SpotPrice'] = df['SpotPrice'].astype('float')

        return df
    except Exception as e:
        print(e)
        raise


def read_from_mongo(dbclient, region: str = 'us-east-1',
                 system: str = SYSTEM_LIST[0],
                 year: int = 2021, days_back=90):
    try:

        sph = dbclient['spot-market-scores']
        collections = sph['spot_price_history']

        filter = {
            'region': region,
            'system': system,
            'year': year
        }

        response = collections.find_one(filter)

        df = pd.Dataframe(response['data'])
        df['Timestamp'] = df['Timestamp'].apply(pd.Timestamp)
        start_day = datetime.today()-timedelta(days=days_back)
        df = df.loc[df.Timestamp >= start_day].copy()
        df['SpotPrice'] = df['SpotPrice'].astype('float')

        return df
    except Exception as e:
        raise

if __name__ == '__main__':
    import boto3
    from spot_market_scoring.config import conf

    from spot_market_scoring.user import get_client_list
    clients = get_client_list(**conf.AWS_CREDENTIALS)
    #clients = get_client_list(**settings.AWS_CREDENTIALS)

    region = 'ap-east-1'
    client = clients[region]

    # print("----Example for reading spot price from AWS----")
    # response = get_spot_price_history(client, days_back=1, region=region,
    #                                   instanceType='p3.16xlarge',
    #                                   availabilityZone='us-east-1a',
    #                                    productDescription='Windows (Amazon VPC)'
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
    from pymongo import MongoClient
    s3client = boto3.client('s3', **conf.AWS_CREDENTIALS)
    dbclient = MongoClient(conf.MONGODB_CONNECTION)
    # update_spot_price_history_in_all_region(clients, year=2021, days_back=30,
    #                                         s3client=s3client, dbclient=dbclient, local=False)

    # read_from_s3(s3client,'ap-east-1',SYSTEM_LIST[0],2021)
    # upload_all_spot_price_history_to_s3(s3client,data_path)
    # print("----Example for generating a spot instance list----")
    # si_list = generate_spot_instance_list(data_path)

    print("----Example for getting statistics from s3 price files----")
    for region in ['af-south-1','ap-east-1','ap-northeast-3']:
        for system in SYSTEM_LIST:
            avg_res, std_res, ins_dict = get_savings_statistics(region=region,system=system,
                                                                    year=2021, period="Month",
                                                                    s3client=s3client, days_back=30)
    #         print('finished')
    # print(ins_dict['t3.micro'])
    # print(pd.DataFrame(avg_res))
