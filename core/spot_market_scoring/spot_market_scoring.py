import numpy as np
import boto3
import pandas as pd
import spot_price_history as sph
import spot_advisor as sa
import random_forest as rdf
import user, ec2
from config import conf
from mappings import *
from utils import normalize_by_columns,ParquetTranscoder
from concurrent_task import *



def scale_to_100(df, columns):
    for col in columns:
        df[col] = round(100 * df[col])

    return df


def calculate_scores(df, r_col, az_cols):
    for az in az_cols:
        df[az] = df[r_col] + df[az]

    return df


def get_scores(ondemand, sa_response, s3client):
    start = time.time()
    regions = sorted(REGION_CODE_MAP.keys())
    executor = ThreadPoolExecutor()

    response = ConcurrentTaskPool(executor).add([
        ConcurrentTask(executor, task=get_scores_helper,
                       t_args=(ondemand,sa_response, region, system, s3client))
        for system in SYSTEM_LIST  for region in regions
    ])

    end = time.time()
    # write to database
    print(f'Total Time used: {end - start}')
    print("Finished saving all spot price history data into S3 bucket")
    return True


def get_scores_helper(ondemand,sa_response, region, system, s3client):
    ## region -> sys -> ins
    columns = ['Region', 'System', 'InstanceType', 'InstanceScores', 'AZScores']
    start = time.time()
    # some instance are ignored if missing On Demand/Spot Advisor data
    # iterate through price history's list

    print(f'----calculating for {region} {system}----')
    scores = []
    res_avg, res_std, ins_dict = sph.get_savings_statistics(region=region, days_back=30,
                                                            system=system, s3client=s3client, year=2021,
                                                            period="Month")
    ins_pred = rdf.get_predicted_price(ins_dict, 2021)
    ins_price = ondemand[(ondemand['Region'] == region)
                         & (ondemand['OperatingSystem'] == system)]

    for ins in ins_price['InstanceType'].unique():
        ins_scores = {}
        # on demand price
        od_price = ins_price[ins_price['InstanceType'] == ins]['OnDemand']
        od_price = float(od_price)

        try:
            # random forest ->get prediction price
            pred_savings = {az: 1 - (ins_pred[ins][az] / od_price) for az in ins_pred[ins]}

            # spot_price_history
            savings_rate = {az: 1 - (res_avg[ins][az] / od_price) for az in res_avg[ins]}
            std = {az: res_std[ins][az] for az in res_std[ins]}

            # combine predicted savings and previous savings to instance score
            avg_savings = {az: (pred_savings[az]+savings_rate[az])/2 for az in res_avg[ins]}
            ins_az_scores = {az: 1 * avg_savings[az] - std[az] for az in savings_rate}


            # spot advisor data -> get interruption/saving's data
            ins_scores['InstanceScores'] = sa_response[region][SYSTEM_MAP[system]][ins]
            ins_scores['AZScores'] = ins_az_scores
            scores.append(dict(zip(columns,
                                   [region, system, ins, ins_scores['InstanceScores'],
                                    ins_scores['AZScores']])))
        except Exception as e:
            print(e)
            print(region, system, ins)
        continue

    scores_df = pd.DataFrame(scores)
    scores_df = pd.concat([scores_df, scores_df['InstanceScores'].apply(pd.Series)], axis=1).drop('InstanceScores',axis=1)
    scores_df = pd.concat([scores_df, scores_df['AZScores'].apply(pd.Series)], axis=1).drop('AZScores', axis=1)
    scores_df.fillna(0, inplace=True)
    scores_df['r'] = scores_df['r'].apply(lambda x: np.exp(1 - x))
    scores_df = normalize_by_columns(scores_df, scores_df.columns[3:])
    scores_df = calculate_scores(scores_df, 'r', scores_df.columns[5:])

    scores_df = scale_to_100(normalize_by_columns(scores_df, scores_df.columns[5:]), scores_df.columns[5:])
    df = scores_df.melt(id_vars=["Region", "System", "InstanceType", 'r', 's'],
                   var_name="AvailabilityZone",
                   value_name="Score")
    df.drop(columns=['r', 's'], inplace=True)

    write_to_s3(s3client,df,region,system)
    end = time.time()
    print(f'{region} {system} Finished with: {end - start} seconds')
    return scores_df.to_dict(orient='records')


def write_to_s3(s3client, df,region,system):

    try:
        key = (f'ec2/spot_market_scores/'
               f'{region}_{ParquetTranscoder.encode(system)}.csv')
        df['Score'] = df['Score'].astype('string')
        from io import StringIO
        csv_buffer = StringIO()
        df.to_csv(csv_buffer, index=False)
        s3client.put_object(
            Bucket='stompy-aws-dataset',
            Key=key,
            Body=csv_buffer.getvalue()
        )
        return True
    except Exception as e:
        raise

    return True



if __name__ == '__main__':

    pricing_client = boto3.client('pricing', region_name='us-east-1', **conf.SUB_CREDENTIALS)
    s3client = boto3.client('s3', **conf.SUB_CREDENTIALS)
    clients = user.get_client_list(**conf.SUB_CREDENTIALS)

    # ec2.update_ondemand_price(pricing_client,s3client)
    ondemand = ec2.get_ondemand_price_list(s3client)
    ondemand.reset_index(drop=True,inplace=True)

    sa_response = sa.get_spot_advisor_data(s3client=s3client, local=False)
    response = get_scores(ondemand, sa_response, s3client)
