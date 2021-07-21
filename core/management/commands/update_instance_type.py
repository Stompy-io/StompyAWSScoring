from pymongo import MongoClient
from django.conf import settings
from django.core.management.base import BaseCommand
from core.spot_market_scoring.mappings import REGION_CODE_MAP
import boto3
import time

class Command(BaseCommand):
    
    def save_data_to_mongo(self, region_code, data):
        dbclient = MongoClient(settings.MONGODB_CONNECTION)
        db = dbclient['instances']
        collection = db[region_code]
        collection.remove({})
        collection.insert_many(data)

    def handle(self, *args, **options):
        start = time.time()
        for region in REGION_CODE_MAP.keys():
            temp_time = time.time()
            session = boto3.session.Session(region_name=region, **settings.AWS_CREDENTIALS)
            ec2_client = session.client('ec2')

            response = ec2_client.describe_instance_types()
            results = []
            results.extend(response['InstanceTypes'])
            while 'NextToken' in response:
                response = ec2_client.describe_instance_types(NextToken=response['NextToken'])
                results.extend(response['InstanceTypes'])

            self.save_data_to_mongo(region_code=region,data=results)
            print(region, time.time() - temp_time)
        print(time.time() - start)
