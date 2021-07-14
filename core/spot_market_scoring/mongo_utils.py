from pymongo import MongoClient
from . import spot_price_history as sph
from . import spot_market_scoring as sms
from . import spot_advisor as sa
import boto3
#from config import conf
from .mappings import *
from django.conf import settings

def get_spot_instance_list(dbclient, region=None, system=None):
    db = dbclient['spot-market-scores']
    instance_list = db['spot_instance_list']

    filters = {}
    if region:
        filters['region'] = region
    if system:
        filters['system'] = system

    cursor = instance_list.find(filters,{'_id':0, 'instanceList':1})
    result=[]
    for x in cursor:
        result.extend(x['instanceList'])
    return result


def update_spot_instance_list(s3client, dbclient):
    si = sph.get_spot_instance_list(s3client)
    db = dbclient['spot-market-scores']
    db.spot_instance_list.remove({})

    for region, region_dict in si.items():
        for sys, sys_dict in si[region].items():

            db.spot_instance_list.insert_one({
                "region": region,
                "system": sys,
                "instanceList": sys_dict})



if __name__ == '__main__':
    dbclient = MongoClient(settings.MONGODB_CONNECTION)
    # print(get_spot_instance_list(dbclient, region='us-west-1', system='Linux/UNIX (Amazon VPC)'))

    s3client = boto3.client('s3',**settings.SUB_CREDENTIALS)
    # update_spot_instance_list(s3client, dbclient)

    # update_spot_market_scores(s3client,dbclient)

    dbclient = MongoClient(settings.MONGODB_CONNECTION)
    region='ap-southeast-1'
    system='Linux/UNIX (Amazon VPC)'
    azs = ['ap-southeast-1a', 'ap-southeast-1b']
    instanceTypes = ['c5.large','c5.xlarge']
    print(get_spot_market_scores(dbclient,region,system,azs,instanceTypes))



