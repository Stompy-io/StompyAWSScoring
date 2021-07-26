from pymongo import MongoClient
from django.conf import settings
from django.core.management.base import BaseCommand
from core.spot_market_scoring import spot_market_scoring as sms
from core.spot_market_scoring import spot_price_history as sph
from core.spot_market_scoring import user, ec2,pricing
from core.spot_market_scoring import spot_advisor as sa
import boto3


class Command(BaseCommand):

    def handle(self, *args, **options):
        import time
        pricing_client = boto3.client('pricing', region_name='us-east-1', **settings.AWS_CREDENTIALS)
        clients = user.get_client_list(**settings.AWS_CREDENTIALS)
        dbclient = MongoClient(settings.MONGODB_CONNECTION)
        s3client = boto3.client('s3', **settings.AWS_CREDENTIALS)

        start = time.time()
        ec2.update_instance_types(clients,dbclient)
        print(f'update instance types time used: {time.time()-start}') # 160

        start = time.time()
        ec2.update_prod_info(dbclient)
        print(f'update prod info time used: {time.time()-start}') #4
        start = time.time()
        pricing.update_ondemand_price(pricing_client,dbclient)
        print(f'update ondemand price time used: {time.time()-start}') #466
        start = time.time()
        pricing.update_instance_list_by_family(dbclient)
        print(f'update instance list time used: {time.time()-start}') #25
        start = time.time()
        # days_back set for 2 days for now, should be set 1 after schedule to run daily
        sph.update_spot_price_history_in_all_region(clients, year=2021, days_back=5,
                                                    s3client=s3client, dbclient=dbclient, local=False)
        print(f'update spot price history used: {time.time() - start}') # 873

        # can take some time to run, can be schedule to update weekly?
        ondemand = pricing.get_ondemand_price_list(dbclient)
        ondemand.reset_index(drop=True, inplace=True)
        # print(ondemand.head())
        sa_response = sa.get_spot_advisor_data(dbclient=dbclient, local=False)
        start = time.time()
        response = sms.get_scores(ondemand, sa_response, s3client, dbclient)
        print(f'update score time used: {time.time() - start}')

