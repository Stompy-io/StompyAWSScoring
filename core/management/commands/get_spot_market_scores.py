from pymongo import MongoClient
from django.core.management.base import BaseCommand
from core.spot_market_scoring.spot_market_scoring import read_from_mongo
from django.conf import settings



class Command(BaseCommand):

    def handle(self, *args, **options):
        dbclient = MongoClient(settings.MONGODB_CONNECTION)
        region = 'ap-southeast-1'
        system = 'Linux/UNIX (Amazon VPC)'
        azs = ['ap-southeast-1a', 'ap-southeast-1b']
        instanceTypes = ['c5.large', 'c5.xlarge']
        print(read_from_mongo(dbclient, region, system, azs, instanceTypes))
