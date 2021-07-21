import json
from pymongo import MongoClient

from django.conf import settings
from rest_framework import status
from rest_framework.views import APIView
from rest_framework.response import Response

SYSTEM_MAP = {
    'linux': 'Linux/UNIX (Amazon VPC)', 
    'suse': 'SUSE Linux (Amazon VPC)',
    'rhel': 'Red Hat Enterprise Linux (Amazon VPC)',
    'windows': 'Windows (Amazon VPC)'
}


class BaseConnectView(APIView):

    @property
    def get_connection(self):
        return MongoClient(settings.MONGODB_CONNECTION)


class ScoringView(BaseConnectView):

    def post(self, request):
        filter_field = {
            'region_code': ['Region', ''],
            'os': ['System', {}],
            'az': ['AvailabilityZone', []],
            'spot_types': ['InstanceType', []],
        }
        
        filter_params = {}
        for field, detail in filter_field.items():
            field_data = request.data.get(field)
            if field_data and isinstance(detail[1], str):
                filter_params[detail[0]] = field_data
            if field_data and isinstance(detail[1], list):
                try:
                    field_data = json.loads(field_data)
                except Exception as err:
                    return Response({'error': 'Illegal parameter <{}>. Detail: {}'.format(field, str(err))},status=status.HTTP_400_BAD_REQUEST)
                filter_params[detail[0]] = {"$in": field_data}
            if field_data and isinstance(detail[1], dict):
                filter_params[detail[0]] = SYSTEM_MAP[field_data]

        db = self.get_connection['spot-market-scores']
        score_collection = db['scores']
        result = score_collection.find(filter_params, {'_id': 0})
        results = []
        for item in result:
            results.append(item)
        return Response({'results': results})



class InstanceView(BaseConnectView):

    def get(self, request):
        filter_params = {}
        region_code = request.GET.get('region_code')
        instance_type = request.GET.get('instance_types')
        if instance_type:
            try:
                field_data = json.loads(instance_type)
            except Exception as err:
                return Response({'error': 'Illegal parameter <instance_types>. Detail: {}'.format(str(err))},status=status.HTTP_400_BAD_REQUEST)
            filter_params['InstanceType'] = {"$in": field_data}
        db = self.get_connection['instances']
        score_collection = db[region_code]
        result = score_collection.find(filter_params, {'_id': 0})
        # result = score_collection.find(filter_params, {
        #     '_id': 0,
        #     'InstanceType': 1,
        #     'SupportedUsageClasses': 1,
        #     'ProcessorInfo': 1,
        #     'VCpuInfo': 1,
        #     'MemoryInfo': 1,
        #     'SupportedRootDeviceTypes': 1,
        #     'InstanceStorageSupported': 1,
        #     'InstanceStorageInfo': 1,
        #     'EbsInfo': 1,
        # })
        results = []
        for item in result:
            results.append(item)
        return Response({'results': results})
