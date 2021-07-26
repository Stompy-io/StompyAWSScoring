import boto3
from core.spot_market_scoring.mappings import REGION_CODE_MAP
from core.spot_market_scoring.concurrent_task import *
from django.conf import settings


def getRegionNames(ec2_client=None):
    # session = boto3.session.Session(profile_name=profile_name)
    # ec2_client = session.client('ec2')
    # exception handling
    regions_response = ec2_client.describe_regions()
    return [region['RegionName'] for region in regions_response['Regions']]


def getInstanceTypesInRegion(ec2_client=None, region='ap-southeast-1'):
    # session = boto3.session.Session(profile_name=profile_name, region_name=region)
    # ec2_client = session.client('ec2')
    instance_types_offering = ec2_client.describe_instance_type_offerings(LocationType='region')
    return [it['InstanceType'] for it in instance_types_offering['InstanceTypeOfferings']]


def getGlobalInstanceTypes(ec2_client=None):
    regions = getRegionNames(ec2_client)
    instance_type_set = set([])
    for region in regions:
        instances_types = getInstanceTypesInRegion(ec2_client, region=region)
        for instance_type in instances_types:
            instance_type_set.add(instance_type)
    return instance_type_set


def write_to_mongodb(dbclient, response):
    pass


def update_instance_types(clients, dbclient):
    aws_data = dbclient['aws_data']

    for region in REGION_CODE_MAP.keys():

        response = clients[region].describe_instance_types()
        results = []
        results.extend(response['InstanceTypes'])
        while 'NextToken' in response:
            response = clients[region].describe_instance_types(NextToken=response['NextToken'])
            results.extend(response['InstanceTypes'])


        filter = {'region': region}
        data = {
            '$set': {'data': results}
        }

        aws_data.ec2_instance_type.update_one(filter, data, upsert=True)

    update_prod_info(dbclient)

def update_prod_info(dbclient):
    aws_data = dbclient['aws_data']

    db_w = dbclient['product_info']

    for region in REGION_CODE_MAP.keys():

        filter = {
            'region':region
        }
        projection = {
            '_id': 0,
            'data.InstanceType': 1,
            'data.ProcessorInfo.SupportedArchitectures':1
        }
        response = aws_data.ec2_instance_type.find_one(filter, projection)
        response = response['data']
        results = []
        for x in response:
            ins_dict = {
                'InstanceType': x['InstanceType'],
                'Architectures': x['ProcessorInfo']['SupportedArchitectures']
            }

            results.append(ins_dict)
        data = {
            '$set': {'InstanceList': results}
        }

        db_w.instance_types.update({'Region':region},data,upsert=True)
    return






