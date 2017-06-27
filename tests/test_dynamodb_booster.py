import unittest
import boto3
from copy import deepcopy
import json
import sys
import os
from moto import mock_s3, mock_datapipeline, mock_dynamodb2
from mock import patch

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from hippolyte.dynamodb_booster import DynamoDbBooster


TABLE_NAME = 'prd-shd-euw1-scotty_audit-actions'


def create_test_table(dynamodb_client, table_name, table):
    _table = deepcopy(table)
    del _table['ProvisionedThroughput']["LastIncreaseDateTime"]
    del _table['ProvisionedThroughput']["LastDecreaseDateTime"]
    del _table['ProvisionedThroughput']["NumberOfDecreasesToday"]

    dynamodb_client.create_table(TableName=table_name,
                                 AttributeDefinitions=_table['AttributeDefinitions'],
                                 KeySchema=_table['KeySchema'],
                                 ProvisionedThroughput=_table['ProvisionedThroughput']
                                 )


def load_backup_metadata():
    metadata_file = os.path.join(os.path.dirname(__file__), 'resources/test_backup_metadata.json')
    with open(metadata_file) as f:
        return f.read()


def create_backup_metadata(s3_client, bucket, key, body):
    s3_client.create_bucket(Bucket=bucket)
    s3_client.put_object(Bucket=bucket, Key=key, Body=body)


def get_old_rcu_and_boost(table_descriptions, new_read_capacity):
    old = table_descriptions[0]['Table']['ProvisionedThroughput']['ReadCapacityUnits']
    table_descriptions[0]['Table']['ProvisionedThroughput']['ReadCapacityUnits'] = new_read_capacity

    return old


class TestDynamoDbBooster(unittest.TestCase):
    @mock_dynamodb2
    @mock_datapipeline
    @mock_s3
    @patch("hippolyte.config_util.ConfigUtil.list_backed_up_tables", return_value=TABLE_NAME)
    def test_restore_throughput(self, config_mock):
        dynamodb_client = boto3.client('dynamodb', region_name='eu-west-1')
        s3_client = boto3.client('s3')

        backup_metadata = load_backup_metadata()
        table_descriptions = json.loads(backup_metadata)['Tables']
        create_test_table(dynamodb_client, TABLE_NAME, table_descriptions[0]['Table'])
        old_rcu = get_old_rcu_and_boost(table_descriptions, 1000)

        bucket = 'euw1-dynamodb-backups-prd-480503113116'
        booster = DynamoDbBooster(table_descriptions, bucket, 0.5)
        create_backup_metadata(s3_client, bucket, 'backup_metadata-2099-06-06-00-00-01', backup_metadata)

        booster.restore_throughput()

        table = dynamodb_client.describe_table(TableName=TABLE_NAME)
        self.assertEqual(table['Table']['ProvisionedThroughput']['ReadCapacityUnits'], old_rcu)
