import os
import time
import boto3
from pymongo import MongoClient
from spot_market_scoring import spot_market_scoring as sms
from spot_market_scoring import spot_price_history as sph
from spot_market_scoring import user, ec2, pricing
from spot_market_scoring import spot_advisor as sa

AWS_CREDENTIALS = {'aws_access_key_id': os.getenv('ALGO_AWS_CREDENTIALS_ACCESS_KEY_ID'),
                   'aws_secret_access_key': os.getenv('ALGO_AWS_CREDENTIALS_SECRET_ACCESS_KEY')}

MONGODB_CONNECTION = os.getenv('ALGO_MONGODB_CONNECTION')

if __name__ == '__main__':
    pricing_client = boto3.client('pricing', region_name='us-east-1', **AWS_CREDENTIALS)
    clients = user.get_client_list(**AWS_CREDENTIALS)
    dbclient = MongoClient(MONGODB_CONNECTION)
    s3client = boto3.client('s3', **AWS_CREDENTIALS)


    start = time.time()
    ec2.update_instance_types(clients,dbclient)
    print(f'update instance types time used: {time.time()-start}') #305.0582730770111
    start = time.time()
    ec2.update_prod_info(dbclient)
    print(f'update prod info time used: {time.time()-start}') #9.181091070175171
    start = time.time()
    pricing.update_ondemand_price(pricing_client,dbclient)
    print(f'update ondemand price time used: {time.time()-start}') #957.0628590583801
    start = time.time()
    pricing.update_instance_list_by_family(dbclient)
    print(f'update instance list time used: {time.time()-start}') #264.9353218078613
    start = time.time()
    # days_back set for 2 days for now, should be set 1 after schedule to run daily
    sph.update_spot_price_history_in_all_region(clients, year=2021, days_back=5,
                                                s3client=s3client, dbclient=dbclient, local=False)
    print(f'update spot price history used: {time.time() - start}') # 1375.139800786972

    # can take some time to run, can be schedule to update weekly?
    ondemand = pricing.get_ondemand_price_list(dbclient)
    ondemand.reset_index(drop=True, inplace=True)
    # print(ondemand.head())
    sa_response = sa.get_spot_advisor_data(dbclient=dbclient, local=False)
    start = time.time()
    response = sms.get_scores(ondemand, sa_response, s3client, dbclient)
    print(f'update score time used: {time.time() - start}')
