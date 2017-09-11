import unittest
import boto3
import json
import sys
import os
from moto import mock_dynamodb2

sys.path.append(os.path.join(os.getcwd() + '/../code'))

from hippolyte.dynamodb_backup import get_table_descriptions
from test_utils import create_test_table, load_backup_metadata


class TestDynamoDbBackup(unittest.TestCase):
    @mock_dynamodb2
    def test_get_table_descriptions(self):
        dynamodb_client = boto3.client('dynamodb', region_name='eu-west-1')

        backup_metadata = load_backup_metadata()
        table_descriptions = json.loads(backup_metadata)['Tables']

        always_backup = ['prd-mol-euw1-fluxcapacitor-fluxcapacitor-alertdefinition',
                         'prd-mol-euw1-fluxcapacitor-fluxcapacitor-alertdefinitionhistory',
                         'prd-mol-euw1-fluxcapacitor-fluxcapacitor-dashboard',
                         'prd-mol-euw1-fluxcapacitor-fluxcapacitor-eventtype',
                         'prd-mol-euw1-fluxcapacitor-fluxcapacitor-fluxcontext',
                         'prd-mol-euw1-fluxcapacitor-fluxcapacitor-kinesisstream',
                         'prd-mol-euw1-fluxcapacitor-fluxcapacitor-metricdefinition',
                         'prd-mol-euw1-fluxcapacitor-fluxcapacitor-metricdefinitionhistory',
                         'prd-mol-euw1-fluxcapacitor-fluxcapacitor-pagerdutyServiceIntegration',
                         'prd-mol-euw1-fluxcapacitor-fluxcapacitor-snssubscription',
                         'prd-mol-euw1-fluxcapacitor-fluxcapacitor-system',
                         'prd-mol-euw1-fluxcapacitor-fluxcapacitor-systemaccessrule',
                         'prd-mol-euw1-fluxcapacitor-fluxcapacitor-useraccessrule'
                         ]

        for table_name in always_backup + [
            'prd-cymes-euw1-commsflux--alertdefinition',
            'prd-cymes-euw1-fluxcapacitor-prd-cymes-euw1-fluxcapacitor-alert',
            'prd-cymes-euw1-fluxcapacitor-prd-cymes-euw1-storepick-flux-events-192.168.1.39-kinesis-consumer',
            'prd-shd-euw1-flux-table1',
            'prd-shd-euw1-smth-flux',
            'prd-mol-euw1-storepickreporting-ContainerReport-snapshots',
            'prd-mol-euw1-will-be-backed-up',
            'prd-mol-euw1-will-be-backed-up-snapshots'
        ]:
            create_test_table(dynamodb_client, table_name, table_descriptions[0]['Table'])

        exclude_from_backup = [
            '.*flux.*',
            '.*storepickreporting-.*-snapshots'
        ]

        table_descriptions = get_table_descriptions(exclude_from_backup, always_backup)
        included_tables = map(lambda x: x['Table']['TableName'], table_descriptions)
        included_tables.sort()

        expected_tables = always_backup + [
            'prd-mol-euw1-will-be-backed-up',
            'prd-mol-euw1-will-be-backed-up-snapshots'
        ]
        expected_tables.sort()

        self.assertListEqual(included_tables, expected_tables)
