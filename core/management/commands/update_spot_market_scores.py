from pymongo import MongoClient
from django.conf import settings
from django.core.management.base import BaseCommand
from core.spot_market_scoring import spot_market_scoring as sms
from core.spot_market_scoring import spot_price_history as sph
from core.spot_market_scoring import user, ec2
from core.spot_market_scoring import spot_advisor as sa
import boto3
import time

class Command(BaseCommand):

    def handle(self, *args, **options):
        pricing_client = boto3.client('pricing', region_name='us-east-1', **settings.AWS_CREDENTIALS)
        s3client = boto3.client('s3', **settings.AWS_CREDENTIALS)
        clients = user.get_client_list(**settings.AWS_CREDENTIALS)
        dbclient = MongoClient(settings.MONGODB_CONNECTION)
        import pdb;
        pdb.set_trace()

        db = dbclient['spot-market-scores']
        collection = db['spot_price_history']

        collection.delete_many({})


        sph.update_spot_price_history_in_all_region(clients, year=2021, days_back=7,
                                                    s3client=s3client, dbclient=dbclient, local=False)

        ec2.update_ondemand_price(pricing_client,s3client)
        ondemand = ec2.get_ondemand_price_list(s3client)
        ondemand.reset_index(drop=True, inplace=True)

        sa_response = sa.get_spot_advisor_data(dbclient=dbclient, local=False)
        response = sms.get_scores(ondemand, sa_response, s3client)

