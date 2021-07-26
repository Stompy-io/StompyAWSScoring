import numpy as np
import pandas as pd
from spot_market_scoring import spot_price_history as sph
from spot_market_scoring import random_forest as rdf
from spot_market_scoring.mappings import *
from spot_market_scoring.utils import normalize_by_columns,ParquetTranscoder
from spot_market_scoring.concurrent_task import *

def scale_to_100(df, columns):
    for col in columns:
        df[col] = round(100 * df[col])

    return df


def calculate_scores(df, r_col, az_cols):
    for az in az_cols:
        df[az] = df[r_col] + df[az]

    return df


def get_scores(ondemand, sa_response, s3client, dbclient):

    regions = sorted(REGION_CODE_MAP.keys())
    executor = ThreadPoolExecutor()

    ConcurrentTaskPool(executor).add([
        ConcurrentTask(executor, task=get_scores_helper,
                       t_args=(ondemand,sa_response, region, system, s3client,dbclient))
        for system in SYSTEM_LIST for region in regions
    ])


    return True


def get_scores_helper(ondemand,sa_response, region, system, s3client, dbclient):
    ## region -> sys -> ins
    columns = ['Region', 'System', 'InstanceType', 'InstanceScores', 'AZScores']
    start = time.time()
    # some instance are
    # ignored if missing On Demand/Spot Advisor data
    # iterate through price history's list

    #print(f'----calculating for {region} {system}----')
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
        except KeyError:
            pass
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

    # write_to_s3(s3client,df,region,system)
    write_to_mongo(dbclient,df,region,system)
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


def read_from_s3(s3client, region, system):
    try:
        key = (f'ec2/spot_market_scores/'
               f'{region}_{ParquetTranscoder.encode(system)}.csv')

        response = s3client.get_object(
            Bucket='stompy-aws-dataset',
            Key=key
        )
        df = pd.read_csv(response.get('Body'))

        df['Score'] = df['Score'].astype('int64')
        return df.to_dict(orient='records')
    except Exception as e:
        raise



def read_from_mongo(dbclient, region=None, system=None, azs=None, instanceTypes=None):
    db = dbclient['spot_market_scores']
    scores = db['scores']

    filters = {}
    if region:
        filters['Region'] = region
    if system:
        filters['System'] = system

    if isinstance(azs,list):
        filters['AvailabilityZone'] = {"$in":azs}
    if isinstance(instanceTypes, list):
        filters['InstanceType'] = {"$in": instanceTypes}

    cursor = scores.find(filters,{'_id':0})
    result=[]
    for x in cursor:
        result.append(x)

    result = {'result': result}
    return result


def write_to_mongo(dbclient, df, region, system):
    db = dbclient['spot_market_scores']
    db.scores.delete_many({"Region": region, "System": system})

    data_dict = df.to_dict(orient='records')

    db.scores.insert_many(data_dict)
    return
