from pymongo import MongoClient
from django.conf import settings
from django.core.management.base import BaseCommand
import boto3
import time

class Command(BaseCommand):
    
    def save_data_to_mongo(self, region_code, data):
        dbclient = MongoClient(settings.MONGODB_CONNECTION)
        db = dbclient['aws_data']['instance_type']
        data_filter = {'region_code': region_code}
        update_data = {
            '$setOnInsert': {
                'region_code': region_code,
                'data': data
            }
        }
        db.updateMany(data_filter, update_data, upsert=True)
    
    def get_aws_data(self):
        pass        

    def handle(self, *args, **options):
        start = time.time()
        session = boto3.session.Session(region_name='us-east-1', **settings.AWS_CREDENTIALS)
        ec2_client = session.client('ec2')
        print(ec2_client.describe_instance_types())
        # pricing_client = boto3.client('pricing', region_name='us-east-1', **settings.AWS_CREDENTIALS)
        # s3client = boto3.client('s3', **settings.AWS_CREDENTIALS)
        # clients = user.get_client_list(**settings.AWS_CREDENTIALS)
        

        # # days_back set for 2 days for now, should be set 1 after schedule to run daily
        # sph.update_spot_price_history_in_all_region(clients, year=2021, days_back=2,
        #                                             s3client=s3client, dbclient=dbclient, local=False)

        # # can take some time to run, can be schedule to update weekly?
        # ec2.update_ondemand_price(pricing_client,s3client)

        # ondemand = ec2.get_ondemand_price_list(s3client)
        # ondemand.reset_index(drop=True, inplace=True)

        # sa_response = sa.get_spot_advisor_data(dbclient=dbclient, local=False)
        # print(sa_response)
        print(time.time() - start)

        # response = sms.get_scores(ondemand, sa_response, s3client, dbclient)

