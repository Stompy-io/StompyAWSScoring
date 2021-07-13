#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""spot_advisor.py

    Handles Spot Advisor relevant actions, including data read/write,
    filter results, calculate average and stuff
    Functions:
        get_spot_advisor_data(path, file_date) -> response
        filter_spot_price(response,region,product_description,instance_type)
        fetch_scores(response, region, product_description, instance_type, alt=False, policy=None)

    Helper Functions:
        model(response)
        alt_model(response)
        flatten(response)
        average_by_columns

"""

from django.conf import settings
from .mappings import SYSTEM_MAP
from datetime import datetime
from .utils import listdir_nohidden
from .utils import normalize_by_columns
import requests
import json
import os
import pandas as pd
import numpy as np
from datetime import date
from datetime import datetime

balanced_optim = {'savings_weight': 1,
                  'interruptions_weight': 1}




def get_spot_advisor_data(path=None, file_date: [str, date] = None, dbclient = None, local=False) -> dict:
    """
    load spot_advisor data from path, if file does not exist,
    download from url and save in path
    :param path: read/load data from path/spot_advisor/advisor-date.json
    :param file_date: default to read today's data, return today's data
                    if date not available
    :return dict -> region -> [Linux/Windows] -> InstanceType -> r: 0-4, s: 0-100
    """
    date_format = "%Y-%m-d"

    if isinstance(file_date, str):
        try:
            datetime.strptime(file_date, date_format)
        except ValueError:
            print("Date format should be YYYY-MM-DD")
    if file_date is None:
        file_date = date.today().strftime("%Y-%m-%d")

    if not local and not dbclient:
        print("Please either set local to True or pass in mongodb client")
        return
    try:
        if local:
            spot_advisor = read_from_local(path,file_date)
            return spot_advisor
        else:
            spot_advisor = read_from_mongo(dbclient, file_date)
            return spot_advisor
    except Exception as e:
        print('Reading failed, downloading current date data')

        url = "https://spot-bid-advisor.s3.amazonaws.com/spot-advisor-data.json"
        response = requests.get(url=url)
        response = response.text
        # spot_advisor = json.loads(response.text)['spot_advisor']
        if local:
            write_to_local(path,response)
        else:
            write_to_mongo(dbclient,response, file_date)
        return spot_advisor


def read_from_s3(s3client, file_date):
    try:
        key = (f'ec2/spot_advisor/'
               f'advisor-{file_date}.json')
        print(key)
        response = s3client.get_object(
            Bucket='stompy-aws-dataset',
            Key=key
        )['Body']
        return json.loads(response.read())
    except Exception as e:
        print(f'[ERR] : {e}')
        raise


def read_from_local(path,file_date):
    file_path = os.path.join(path, 'spot_advisor', f'advisor-{file_date}.json')
    try:
        print(f"Reading file from {file_path}")
        with open(file_path, 'r') as f:
            spot_advisor = json.loads(f.read())
        return spot_advisor
    except Exception as e:
        print(f"File location: {file_path} does not exists")
        raise


def write_to_local(path, spot_advisor):
    file_date = date.today().strftime("%Y-%m-%d")
    file_path = os.path.join(path, 'spot_advisor', f'advisor-{file_date}.json')
    print(f'Downloading file to {file_path}')
    with open(file_path, 'w') as f:
        f.write(json.dumps(spot_advisor))
    return


def write_to_s3(s3client, spot_advisor, file_date):
    try:
        key = (f'ec2/spot_advisor/'
                f'advisor-{file_date}.json')

        s3client.put_object(
            Bucket='stompy-aws-dataset',
            Key=key,
            Body=json.dumps(spot_advisor)
        )

        return True
    except Exception as e:
        print(f'[ERR] : {e}')
        raise


def fetch_scores(response: dict, region: [list, str] = None,
                 product_description: [list, str] = None,
                 instance_type: [list, str] = None,
                 alt=False, policy=None) -> dict:
    if instance_type is None:
        instance_type = '*'
    if product_description is None:
        product_description = '*'
    if region is None:
        region = '*'
    if isinstance(instance_type, str):
        instance_type = [instance_type]
    if isinstance(product_description, str):
        product_description = [product_description]
    if isinstance(region, str):
        region = [region]

    product_description_mapped = [SYSTEM_MAP[prod] for prod in product_description]


    if policy is None:
        policy = {'savings_weight': 1,
                  'interruptions_weight': 1,
                  'ins_weight': 1,
                  'reg_weight': 0.1}
    if alt:
        df = alt_model(response, policy['savings_weight'], policy['interruptions_weight'])
    else:
        df = model(response, **policy)

    r_df = df.loc[(df['Region'].isin(region)) &
                  (df['ProductDescription'].isin(product_description_mapped)) &
                  (df['InstanceType'].isin(instance_type))]

    return r_df.to_dict(orient='records')


def filter_spot_price(response: dict, region: [list, str] = None,
                      product_description: [list, str] = None,
                      instance_type: [list, str] = None) -> dict:
    if instance_type is None:
        instance_type = '*'
    if product_description is None:
        product_description = '*'
    if region is None:
        region = '*'

    if isinstance(instance_type, str):
        instance_type = [instance_type]
    if isinstance(product_description, str):
        product_description = [product_description]
    if isinstance(region, str):
        region = [region]

    df = pd.DataFrame(flatten(response))
    no_filter_r = region == ['*']
    no_filter_p = product_description == ['*']
    no_filter_i = instance_type == ['*']

    r_df = df.loc[((no_filter_r == 1) | (df['Region'].isin(region) & (no_filter_r == 0))) &
                  ((no_filter_p == 1) | (df['ProductDescription'].isin(product_description) & (no_filter_p == 0))) &
                  ((no_filter_i == 1) | (df['InstanceType'].isin(instance_type) & (no_filter_i == 0)))]
    return r_df.to_dict(orient='records')


def model(response: dict, savings_weight=1, interruptions_weight=1,
          ins_weight=1, reg_weight=1) -> pd.DataFrame:
    r, s, i, g = interruptions_weight, savings_weight, ins_weight, reg_weight

    flatten_response = flatten(response)
    scores = score_by_operating_system(response)

    for x in flatten_response:
        region, op, instance = x['Region'], x['ProductDescription'], x['InstanceType']

        score = r * g * (scores[op]['Region']['IRL_0'][region]) + s * g * (scores[op]['Region']['Savings'][region]
        ) + r * i * (scores[op]['InstanceType']['IRL_0'][instance]
                ) + +s * i * (scores[op]['InstanceType']['Savings'][instance])
        x['Score'] = score

    df = pd.DataFrame.from_dict(flatten_response)
    df = normalize_by_columns(df, ['Score'])
    df.drop(['InterruptionRate'], axis=1, inplace=True)
    df.drop(['SavingsOverOnDemand'], axis=1, inplace=True)

    return df


def alt_model(response: dict, savings_weight=1, interruptions_weight=1) -> pd.DataFrame:
    # y = r*exp(2-ir/2) + s*7*sv
    s, r = savings_weight, interruptions_weight

    flatten_response = flatten(response)

    for x in flatten_response:
        x['Score'] = s * 7 * (x['SavingsOverOnDemand']) + r * np.exp(2 - x['InterruptionRate'] / 2)

    df = pd.DataFrame.from_dict(flatten_response)
    df = normalize_by_columns(df, ['Score'])
    df.drop(['InterruptionRate'], axis=1, inplace=True)
    df.drop(['SavingsOverOnDemand'], axis=1, inplace=True)

    return df


def flatten(response: dict)-> dict:
    """
    flatten to [Region', 'ProductDescription', 'InstanceType', 'InterruptionRate', 'SavingsOverOnDemand']
    :param response: json file loaded from file/online
    :return flattened response
    """
    records = []
    for region in list(response):
        for system in list(response[region]):
            for ins in list(response[region][system]):
                records.append(dict(zip(
                    ('Region', 'ProductDescription', 'InstanceType', 'InterruptionRate', 'SavingsOverOnDemand'),
                    (region, system, ins, response[region][system][ins]['r'],
                     response[region][system][ins]['s'])
                )))

    return records


def average_by_columns(df, columns: []) -> dict:
    """
    calculate IR/SV scores based on columns of df
    :param columns: to calculate by
    :param df: data
    :return: score_list based on different columns
    """
    score_list = {}
    for col in columns:

        scores = {}

        for name, g in df.groupby(col):
            # percentage of IR == 0 per group
            interruption_score = (g[g['InterruptionRate'] < 2].size / g.size)

            # average of Savings per group
            savings_score = g['SavingsOverOnDemand'].mean()

            scores[name] = [interruption_score, savings_score]
        score_list[col] = scores
    return score_list


def score_by_operating_system(response: dict) -> dict:
    """
    Split by Operating Systems and then get average IR and Savings
    across Region and Instance Type
    """
    df = pd.DataFrame(flatten(response))
    op_score = {}

    for op, op_df in df.groupby('ProductDescription'):

        cols = ['Region', 'InstanceType']
        score_list = average_by_columns(op_df,cols)

        for c in cols:
            sc = pd.DataFrame.from_dict(score_list[c], orient='index', columns=['IRL_0', 'Savings'])
            sc = normalize_by_columns(sc, sc.columns)
            score_list[c] = sc
        op_score[op] = score_list

    return op_score


def write_to_mongo(dbclient, response, file_date):
    db = dbclient['spot-market-scores']

    spot_advisor = {
        'date': file_date,
        'data': response
    }
    db.spot_advisor.insert_one(spot_advisor)
    return

def read_from_mongo(dbclient, file_date):
    db = dbclient['spot-market-scores']
    collection = db['spot_advisor']

    response = collection.find_one({'date': file_date},{'_id': 0, 'data':1})

    spot_advisor = json.loads(response['data'])['spot_advisor']
    return spot_advisor

if __name__ == '__main__':
    pd.set_option('display.max_rows', None)
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', 1000)


    # response = get_spot_advisor_data(conf.DATA_PATH)  #date format YYYY-MM-DD

    # rates = {0: "<5%", 1: "5-10%", 2: "10-15%", 3: "15-20%", 4: ">20%"}
    # df['InterruptionRate'] = df['InterruptionRate'].apply(lambda x: rates[x])
    # df['SavingsOverOnDemand'] = df['SavingsOverOnDemand'].apply(lambda x: round(x / 100, 2))
    import boto3
    s3client = boto3.client('s3', **settings.AWS_CREDENTIALS)
    response = get_spot_advisor_data(s3client=s3client, local=False)
    data = fetch_scores(response, region=['ap-northeast-1', 'ap-northeast-2'],
                      product_description=['Linux/UNIX (Amazon VPC)'],
                      instance_type=['m4.2xlarge', 'm4.4xlarge', 'm4.16xlarge'],
                      alt=True)

    # heatmap Demo
    # to matrix
    df = pd.DataFrame.from_dict(data)
    print(df)

    score_mx = df.pivot(index='Region', columns='InstanceType', values='Score')

    ax = sns.heatmap(score_mx, vmin=0, vmax=1, cmap="YlGnBu", annot=True)

    # client = boto3.client('s3', **settings.AWS_CREDENTIALS)
    # for policy in strategies:
    #     df = sa.model(**policy)
    #     # sns.displot(df['Score'])
