from pymongo import MongoClient
from django.conf import settings
from django.core.management.base import BaseCommand
from core.spot_market_scoring import spot_market_scoring as sms
from core.spot_market_scoring import spot_price_history as sph
from core.spot_market_scoring import user, ec2,pricing
from core.spot_market_scoring import spot_advisor as sa
import boto3


class Command(BaseCommand):
    def update_aws_data(self):
        pricing_client = boto3.client('pricing', region_name='us-east-1', **settings.AWS_CREDENTIALS)
        clients = user.get_client_list(**settings.AWS_CREDENTIALS)
        dbclient = MongoClient(settings.MONGODB_CONNECTION)
        s3client = boto3.client('s3', **settings.AWS_CREDENTIALS)

        ec2.update_instance_types(clients,dbclient)
        ec2.update_prod_info(dbclient)

        pricing.update_ondemand_price(pricing_client,dbclient)
        pricing.update_instance_list_by_family(dbclient)

        # days_back set for 2 days for now, should be set 1 after schedule to run daily
        sph.update_spot_price_history_in_all_region(clients, year=2021, days_back=5,
                                                    s3client=s3client, dbclient=dbclient, local=False)

    def handle(self, *args, **options):
        dbclient = MongoClient(settings.MONGODB_CONNECTION)
        s3client = boto3.client('s3', **settings.AWS_CREDENTIALS)
        clients = user.get_client_list(**settings.AWS_CREDENTIALS)
        dbclient = MongoClient(settings.MONGODB_CONNECTION)

        # self.update_aws_data()

        # can take some time to run, can be schedule to update weekly?
        ondemand = pricing.get_ondemand_price_list(dbclient)
        ondemand.reset_index(drop=True, inplace=True)

        sa_response = sa.get_spot_advisor_data(dbclient=dbclient, local=False)
        response = sms.get_scores(ondemand, sa_response, s3client, dbclient)

