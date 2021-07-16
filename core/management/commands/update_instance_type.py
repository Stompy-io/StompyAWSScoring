from pymongo import MongoClient
from django.conf import settings
from django.core.management.base import BaseCommand
from core.spot_market_scoring.mappings import REGION_CODE_MAP
import boto3
import time

class Command(BaseCommand):
    
    def save_data_to_mongo(self, region_code, data):
        dbclient = MongoClient(settings.MONGODB_CONNECTION)
        db = dbclient['aws-data']
        collection = db['instance_type']
        data_filter = {'region_code': region_code}
        update_data = {
            '$setOnInsert': {
                'region_code': region_code,
                'data': data
            }
        }
        data = {
            'region_code': region_code,
            'data': data
        }
        # collection.update_many(data_filter, update_data, upsert=True)
        collection.insert_one(data)
    
    def get_aws_data(self):
        pass        

    def handle(self, *args, **options):
        start = time.time()
        for region in REGION_CODE_MAP.keys():
            session = boto3.session.Session(region_name=region, **settings.AWS_CREDENTIALS)
            ec2_client = session.client('ec2')

            response = ec2_client.describe_instance_types()
            results = []
            results.extend(response['InstanceTypes'])
            while 'NextToken' in response:
                response = ec2_client.describe_instance_types(NextToken=response['NextToken'])
                results.extend(response['InstanceTypes'])

            self.save_data_to_mongo(region_code=region,data=results)
        # print(results)
        print(time.time() - start)
