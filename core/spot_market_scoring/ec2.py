import boto3
from core.spot_market_scoring.mappings import REGION_CODE_MAP
import json
import pandas as pd
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


def get_ondemand_price(data):
    data = data['terms']['OnDemand']
    description = ""
    for p in data:
        price = "";
        for pp in data[p]['priceDimensions']:
            if data[p]['priceDimensions'][pp]['unit'] == 'Hrs':
                price = data[p]['priceDimensions'][pp]['pricePerUnit']['USD']
                description = data[p]['priceDimensions'][pp]['description']

    return price, description

def get_EC2_product_info(data):
    """
    return product name and deploymentOption from response
    :param data: response['PriceList'] from pricing client.get_products()
    :return:
    """
    data = data['product']['attributes']
    #print(data)
    return data['instanceType']


def get_EC2_products(client,
                     instanceType=None,
                     location=None,
                     operatingSystem=None):
    """
    query RDS products pricelist with filters
    :param instanceType: ex 'db.r4.large'
            location: ex 'US East(N. Virginia)'
            databaseEngine: ex 'MySQL'
    :return:
    """

    filters = []
    filters.append({'Field': 'preInstalledSw', 'Type': 'TERM_MATCH', 'Value': 'NA'})
    # filters.append({'Field': 'currentGeneration','Type': 'TERM_MATCH','Value': 'Yes'})
    filters.append({'Field': 'capacitystatus', 'Type': 'TERM_MATCH', 'Value': 'Used'})
    filters.append({'Field': 'tenancy', 'Type': 'TERM_MATCH', 'Value': 'Shared'})
    filters.append({'Field': 'licenseModel', 'Type': 'TERM_MATCH', 'Value': "No License required"})

    if instanceType:
        filters.append({'Field': 'InstanceType', 'Type': 'TERM_MATCH', 'Value': instanceType})
    if location:
        filters.append({'Field': 'location', 'Type': 'TERM_MATCH', 'Value': REGION_CODE_MAP[location]})
    if operatingSystem:
        filters.append({'Field': 'operatingSystem', 'Type': 'TERM_MATCH', 'Value': operatingSystem})

    response = client.get_products(ServiceCode='AmazonEC2', Filters=filters)
    results = response['PriceList']

    while "NextToken" in response:
        response = client.get_products(NextToken=response['NextToken'],
                                       ServiceCode='AmazonEC2', Filters=filters)
        results.extend(response["PriceList"])

    return results


def write_to_s3(s3client, df):
    try:
        key = 'ec2/ondemand_price.csv'
        df['OnDemand'] = df['OnDemand'].astype('string')
        from io import StringIO
        csv_buffer = StringIO()
        df.to_csv(csv_buffer,index=False)
        s3client.put_object(
            Bucket='stompy-aws-dataset',
            Key=key,
            Body=csv_buffer.getvalue()
        )
        return True
    except Exception as e:
        raise


def write_to_mongodb(dbclient, df):
    pass


def get_ondemand_price_list(s3client):
    try:
        key = 'ec2/ondemand_price.csv'

        response = s3client.get_object(
            Bucket='stompy-aws-dataset',
            Key=key
        )
        ondemand = pd.read_csv(response.get('Body'))
        ondemand['OnDemand'].astype('float64')

        return ondemand
    except Exception as e:
        print(f'[ERR] : {e}')
        raise


def update_ondemand_price_helper(client,region,os):
    columns = ['Region', 'OperatingSystem', 'InstanceType', 'OnDemand']
    os_map = {
        'Linux': 'Linux/UNIX (Amazon VPC)',
        'RHEL': 'Red Hat Enterprise Linux (Amazon VPC)',
        'SUSE': 'SUSE Linux (Amazon VPC)',
        'Windows': 'Windows (Amazon VPC)'
    }
    response = get_EC2_products(client, location=region, operatingSystem=os)
    total_prod = []
    for res in response:
        data = json.loads(res)

        instanceType = get_EC2_product_info(data)
        ondemand, description = get_ondemand_price(data)

        prod_info = [region, os_map[os], instanceType, ondemand]

        entry = dict(zip(columns, prod_info))

        total_prod.append(entry)

    return total_prod


def update_ondemand_price(client, s3client):
    ## EC2 alt
    os_list = ['Linux', 'RHEL', 'SUSE', 'Windows']

    start = time.time()

    executor = ThreadPoolExecutor()
    response = ConcurrentTaskPool(executor).add([
        ConcurrentTask(executor, task=update_ondemand_price_helper,
                       t_args=(client, region, prod))
        for prod in os_list for region in REGION_CODE_MAP
    ]).get_results(merge=True)
    end = time.time()
    print(f'Time used: {end-start}')
    df = pd.DataFrame(response)
    df['OnDemand'] = df.OnDemand.astype(float)
    write_to_s3(s3client,df)
    return

if __name__ == '__main__':

    # use temporary credentials
    credentials = {'aws_access_key_id': "",
                   'aws_secret_access_key': "",
                   'aws_session_token': ""}

    client = boto3.client('ec2', **settings.AWS_CREDENTIALS)

    print(getRegionNames(client))
    print(getGlobalInstanceTypes(client))

    credentials = settings.AWS_CREDENTIALS
    credentials['region_name'] = 'us-east-1'
    client = boto3.client('pricing', **credentials)
    s3client = boto3.client('s3', **settings.AWS_CREDENTIALS)

    # update_ondemand_price(client,s3client)
    df = get_ondemand_price_list(s3client)
    print(df.head())



