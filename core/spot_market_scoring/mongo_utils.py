from pymongo import MongoClient
from core.spot_market_scoring import spot_price_history as sph
from core.spot_market_scoring import spot_market_scoring as sms
from core.spot_market_scoring import spot_advisor as sa
import boto3
#from config import conf
from core.spot_market_scoring.mappings import *
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
    # dbclient = MongoClient(settings.MONGODB_CONNECTION)
    # # print(get_spot_instance_list(dbclient, region='us-west-1', system='Linux/UNIX (Amazon VPC)'))
    #
    # s3client = boto3.client('s3',**settings.SUB_CREDENTIALS)
    # # update_spot_instance_list(s3client, dbclient)
    #
    # # update_spot_market_scores(s3client,dbclient)
    #
    # dbclient = MongoClient(settings.MONGODB_CONNECTION)
    region='ap-southeast-1'
    system='Linux/UNIX (Amazon VPC)'
    # azs = ['ap-southeast-1a', 'ap-southeast-1b']
    # instanceTypes = ['c5.large','c5.xlarge']
    # print(get_spot_market_scores(dbclient,region,system,azs,instanceTypes))

    MONGODB_CONNECTION = "mongodb+srv://sophia:nMOz5qpTQ7uRXLqC@spot-market-scores.mr8yo.mongodb.net/spot-market-scores?retryWrites=true&w=majority&authSource=admin"
    from pymongo import MongoClient
    from core.spot_market_scoring import ec2
    dbclient = MongoClient(MONGODB_CONNECTION)

    db = dbclient['product-info']
    # print(db.ec2_product.distinct("instanceFamily"))

    # ec2.update_instance_list_by_family(dbclient)