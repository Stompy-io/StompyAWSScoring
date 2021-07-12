from pymongo import MongoClient
import spot_price_history as sph
import boto3
from config import conf


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
    db.spot_instance_list.remove( { } )

    for region, region_dict in si.items():
        for sys, sys_dict in si[region].items():

            db.spot_instance_list.insert_one({
                "region": region,
                "system": sys,
                "instanceList": sys_dict})

def update_spot_instance_scores(s3client, dbclient):


if __name__ == '__main__':
    dbclient = MongoClient(conf.MONGODB_CONNECTION)
    print(get_spot_instance_list(dbclient, region='us-west-1', system='Linux/UNIX (Amazon VPC)'))

    s3client = boto3.client('s3',**conf.SUB_CREDENTIALS)
    update_spot_instance_list(s3client, dbclient)
    # db.list_collection_names()



