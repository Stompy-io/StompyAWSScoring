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
        print(time.time() - start)
