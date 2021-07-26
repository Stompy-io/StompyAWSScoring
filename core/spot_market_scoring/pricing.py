from core.spot_market_scoring.mappings import REGION_CODE_MAP
import json
import pandas as pd
from core.spot_market_scoring.concurrent_task import *

OS_MAP = {
    'Linux': 'Linux/UNIX (Amazon VPC)',
    'RHEL': 'Red Hat Enterprise Linux (Amazon VPC)',
    'SUSE': 'SUSE Linux (Amazon VPC)',
    'Windows': 'Windows (Amazon VPC)'
}

OS_CODE_MAP = {v: k for k, v in OS_MAP.items()}


def get_ondemand_price(data):
    data = data['terms']['OnDemand']
    description = ""
    price = ""
    for p in data:
        price = ""
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
    # print(data)
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


def update_ondemand_price_helper(client, dbclient, region, os):
    columns = ['Region', 'OperatingSystem', 'InstanceType', 'OnDemand']

    response = get_EC2_products(client, location=region, operatingSystem=os)
    aws_data = dbclient['aws_data']
    filter = {
        'location': REGION_CODE_MAP[region],
        'operatingSystem': os
    }
    aws_data.pricing_product.delete_many(filter)

    total_prod = []
    for res in response:
        data = json.loads(res)
        data_att = data['product']['attributes']
        aws_data.pricing_product.insert_one(data_att)
        instanceType = data_att['instanceType']

        ondemand, description = get_ondemand_price(data)

        prod_info = [region, OS_MAP[os], instanceType, ondemand]

        entry = dict(zip(columns, prod_info))

        total_prod.append(entry)

    return total_prod


def update_ondemand_price(client, dbclient):
    # EC2 alt
    os_list = OS_MAP.keys()
    executor = ThreadPoolExecutor()
    response = ConcurrentTaskPool(executor).add([
        ConcurrentTask(executor, task=update_ondemand_price_helper,
                       t_args=(client, dbclient, region, prod))
        for prod in os_list for region in REGION_CODE_MAP
    ]).get_results(merge=True)

    df = pd.DataFrame(response)
    df['OnDemand'] = df.OnDemand.astype(float)

    db = dbclient['spot_market_scores']
    db.ondemand.delete_many({})
    data = {
        'response': df.to_dict(orient='records')
    }
    db.ondemand.insert_one(data)

    return


def update_instance_list_by_family(dbclient):
    aws_data = dbclient['aws_data']
    projection = {
        '_id': 0,
        'instanceType': 1,
        'vcpu': 1,
        'memory': 1,
        'storage': 1,
        'instanceFamily': 1
    }

    key_map = {
        'instanceType': 'InstanceType',
        'vcpu': 'VCPU',
        'memory': 'Memory',
        'storage': 'Storage',
        'instanceFamily': 'InstanceFamily'
    }
    product_info = dbclient['product_info']
    spot_market_scores = dbclient['spot_market_scores']
    # TODO replace with s3 info

    for region in REGION_CODE_MAP.keys():
        os_dict = {}
        for os in OS_MAP.keys():
            spot_list = aws_data.ec2_spot_instances.find_one({
                'region': region, 'system': OS_MAP[os]
            }, {
                '_id': 0, 'InstanceList': 1})['InstanceList']

            filters = {
                'location': REGION_CODE_MAP[region],
                'operatingSystem': os,
                'instanceType': {'$in': spot_list}
            }

            cursor = aws_data.pricing_product.find(filters, projection)

            result = []
            for x in cursor:
                y = {key_map[k]: v for k, v in x.items()}
                result.append(y)

            os_dict[os.lower()] = result

        os_dict['Region'] = region
        query = {
            'Region': region
        }
        data = {
            '$set': os_dict
        }
        product_info.spot_types.update_one(query, data, upsert=True)

    return


def get_ondemand_price_list(dbclient):
    try:
        db = dbclient['spot_market_scores']
        response = db.ondemand.find_one({}, {'_id': 0, })
        df = pd.DataFrame(response['response'])
        return df
    except Exception as e:
        print(f'[ERR] : {e}')
        raise
