from mappings import REGION_CODE_MAP
from config import conf
import boto3


def get_subnet_AZ(ec2_client):
    output = {}
    for instance in ec2_client.instances.all():
        for iface in instance.network_interfaces:
            output[instance.id] = {
                'Instance ID': instance.id,
                'Subnet ID': iface.subnet_id,
                'AZ': iface.subnet.availability_zone
            }

    return output


def get_region_list(ec2_client):
    response = ec2_client.describe_regions()
    region_list = []
    for region in response['Regions']:
        region_list.append(region['RegionName'])
    return region_list


def get_availability_zones(ec2_client=None):
    # ec2_client = session.client('ec2')
    # exception handling

    az_response = ec2_client.describe_availability_zones()
    return [az['ZoneName'] for az in az_response['AvailabilityZones']]


def get_az_list(clients=None, **credentials):
    regions = REGION_CODE_MAP.keys()
    AZ = {}

    if not clients:
        clients = get_client_list(regions, **credentials)

    for region in regions:
        AZ[region] = get_availability_zones(clients[region])
    return AZ


def get_client_list(**credentials):
    regions = REGION_CODE_MAP.keys()
    clients = {}
    for region in regions:
        clients[region] = boto3.client('ec2', region_name=region, **credentials)
    return clients


if __name__ == '__main__':
    ec2 = boto3.client('ec2', **conf.SUB_CREDENTIALS)

    # Retrieves all regions/endpoints that work with EC2
    regions = get_region_list(ec2)
    print('Regions:', regions)

    # Retrieves availability zones only for region of the ec2 object
    az = get_availability_zones(ec2)
    print('Availability Zones:', az)
