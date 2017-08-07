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

        for table_name in ['prd-shd-euw1-flux-table1',
                           'prd-mol-euw1-storepickreporting-ContainerReport-snapshots',
                           'prd-mol-euw1-will-be-backed-up']:
            create_test_table(dynamodb_client, table_name, table_descriptions[0]['Table'])

        exclude_from_backup = [
            '.*flux.*',
            '.*storepickreporting-.*-snapshots'
        ]

        table_descriptions = get_table_descriptions(exclude_from_backup)
        self.assertEqual(len(table_descriptions), 1)
        self.assertEqual(table_descriptions[0]['Table']['TableName'], 'prd-mol-euw1-will-be-backed-up')
