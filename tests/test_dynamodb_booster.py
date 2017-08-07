import unittest
import boto3
import json
import sys
import os
from botocore.exceptions import ClientError
from moto import mock_s3, mock_datapipeline, mock_dynamodb2
from mock import patch, Mock

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import hippolyte.dynamodb_booster
import hippolyte.aws_utils
from test_utils import create_test_table, load_backup_metadata

TABLE_NAME = 'prd-shd-euw1-scotty_audit-actions'


def create_backup_metadata(s3_client, bucket, key, body):
    s3_client.create_bucket(Bucket=bucket)
    s3_client.put_object(Bucket=bucket, Key=key, Body=body)


def get_old_rcu_and_boost(table_descriptions, new_read_capacity):
    old = table_descriptions[0]['Table']['ProvisionedThroughput']['ReadCapacityUnits']
    table_descriptions[0]['Table']['ProvisionedThroughput']['ReadCapacityUnits'] = new_read_capacity

    return old


class FakeApplicationAutoscalingClient():
    def __init__(self):
        self.scalable_targets = []
        self.scaling_policies = []

    def get_paginator(self, paginator_name):
        _list = []
        _key = ''
        if paginator_name == 'describe_scalable_targets':
            _list = self.scalable_targets
            _key = 'ScalableTargets'
        elif paginator_name == 'describe_scaling_policies':
            _list = self.scaling_policies
            _key = 'ScalingPolicies'

        paginator = Mock()
        paginator.paginate = Mock(return_value=[{
            _key: _list
        }])

        return paginator

    def delete_scaling_policy(self, PolicyName, ServiceNamespace, ResourceId, ScalableDimension):
        before_delete = len(self.scaling_policies)
        self.scaling_policies = filter(
            lambda x: x['PolicyName'] != PolicyName or x['ScalableDimension'] != ScalableDimension,
            self.scaling_policies)

        if len(self.scaling_policies) == before_delete:
            raise ClientError(
                {
                    'Error':
                        {
                            'Code': 'ObjectNotFoundException',
                            'Message': 'No scaling policy found for service namespace: dynamodb, resource ID: {},'
                                       ' scalable dimension: {}: ObjectNotFoundException'.format(ResourceId,
                                                                                                 ScalableDimension)
                        }
                },
                'DeleteScalingPolicy'
            )

    def deregister_scalable_target(self, ServiceNamespace, ResourceId, ScalableDimension):
        before_delete = len(self.scalable_targets)
        self.scalable_targets = filter(
            lambda x: x['ResourceId'] != ResourceId or x['ScalableDimension'] != ScalableDimension,
            self.scalable_targets)

        if len(self.scalable_targets) == before_delete:
            raise ClientError(
                {
                    'Error':
                        {
                            'Code': 'ObjectNotFoundException',
                            'Message': 'No scalable target found for service namespace: dynamodb, resource ID: {},'
                                       ' scalable dimension: {}: ObjectNotFoundException'.format(ResourceId,
                                                                                                 ScalableDimension)
                        }
                },
                'DeregisterScalableTarget'
            )

    def put_scaling_policy(self, PolicyName, ServiceNamespace, ResourceId, ScalableDimension,
                           PolicyType, TargetTrackingScalingPolicyConfiguration):
        self.scaling_policies.append({
            "PolicyName": PolicyName,
            "ServiceNamespace": ServiceNamespace,
            "ResourceId": ResourceId,
            "ScalableDimension": ScalableDimension,
            "PolicyType": PolicyType,
            "TargetTrackingScalingPolicyConfiguration": TargetTrackingScalingPolicyConfiguration
        })

    def register_scalable_target(self, ServiceNamespace, ResourceId, ScalableDimension,
                                 MinCapacity, MaxCapacity, RoleARN):
        self.scalable_targets.append({
            "ServiceNamespace": ServiceNamespace,
            "ResourceId": ResourceId,
            "ScalableDimension": ScalableDimension,
            "MinCapacity": MinCapacity,
            "MaxCapacity": MaxCapacity,
            "RoleARN": RoleARN
        })


class TestDynamoDbBooster(unittest.TestCase):
    @mock_dynamodb2
    @mock_datapipeline
    @mock_s3
    @patch("hippolyte.config_util.ConfigUtil.list_backed_up_tables", return_value=TABLE_NAME)
    @patch("hippolyte.aws_utils.ApplicationAutoScalingUtil._init_client",
           return_value=FakeApplicationAutoscalingClient())
    def test_restore_throughput(self, config_mock, autoscaling_mock):
        dynamodb_client = boto3.client('dynamodb', region_name='eu-west-1')
        s3_client = boto3.client('s3')

        backup_metadata = load_backup_metadata()
        table_descriptions = json.loads(backup_metadata)['Tables']
        create_test_table(dynamodb_client, TABLE_NAME, table_descriptions[0]['Table'])
        old_rcu = get_old_rcu_and_boost(table_descriptions, 1000)

        bucket = 'euw1-dynamodb-backups-prd-480503113116'
        booster = hippolyte.dynamodb_booster.DynamoDbBooster(table_descriptions, bucket, 0.5)
        create_backup_metadata(s3_client, bucket, 'backup_metadata-2099-06-06-00-00-01', backup_metadata)

        booster.restore_throughput()

        table = dynamodb_client.describe_table(TableName=TABLE_NAME)
        self.assertEqual(table['Table']['ProvisionedThroughput']['ReadCapacityUnits'], old_rcu)

    @mock_dynamodb2
    @mock_datapipeline
    @mock_s3
    @patch("hippolyte.aws_utils.ApplicationAutoScalingUtil._init_client",
           return_value=FakeApplicationAutoscalingClient())
    def test_autoscaling_support(self, autoscaling_mock):
        backup_metadata = load_backup_metadata()
        backup_metadata_dict = json.loads(backup_metadata)
        table_descriptions = backup_metadata_dict['Tables']
        scaling_policies = backup_metadata_dict['ScalingPolicies']
        scalable_targets = backup_metadata_dict['ScalableTargets']

        booster = hippolyte.dynamodb_booster.DynamoDbBooster(table_descriptions, 'foo', 0.5)
        autoscaling_util = booster.application_auto_scaling_util

        for policy in scaling_policies:
            autoscaling_util.put_scaling_policy(policy['PolicyName'],
                                                policy['ServiceNamespace'],
                                                policy['ResourceId'],
                                                policy['ScalableDimension'],
                                                policy['PolicyType'],
                                                policy['TargetTrackingScalingPolicyConfiguration'])

        for target in scalable_targets:
            autoscaling_util.register_scalable_target(target['ServiceNamespace'],
                                                      target['ResourceId'],
                                                      target['ScalableDimension'],
                                                      target['MinCapacity'],
                                                      target['MaxCapacity'],
                                                      target['RoleARN'])

        scaling_policies_before = autoscaling_util.describe_scaling_policies("dynamodb").get('ScalingPolicies')
        scalable_targets_before = autoscaling_util.describe_scalable_targets("dynamodb").get('ScalableTargets')

        booster.disable_auto_scaling(scaling_policies, scalable_targets)

        self.assertFalse(autoscaling_util.describe_scaling_policies("dynamodb").get('ScalingPolicies'))
        self.assertFalse(autoscaling_util.describe_scalable_targets("dynamodb").get('ScalableTargets'))

        booster.reenable_auto_scaling(backup_metadata_dict)

        scaling_policies_after = autoscaling_util.describe_scaling_policies("dynamodb").get('ScalingPolicies')
        scalable_targets_after = autoscaling_util.describe_scalable_targets("dynamodb").get('ScalableTargets')

        self.assertListEqual(scaling_policies_before, scaling_policies_after)
        self.assertListEqual(scalable_targets_before, scalable_targets_after)

    @mock_dynamodb2
    @mock_datapipeline
    @mock_s3
    @patch('hippolyte.dynamodb_booster.logger')
    @patch("hippolyte.aws_utils.ApplicationAutoScalingUtil._init_client",
           return_value=FakeApplicationAutoscalingClient())
    def test_disable_autoscaling_warns_on_missing_resources(self, logger_mock, autoscaling_mock):
        backup_metadata = load_backup_metadata()
        backup_metadata_dict = json.loads(backup_metadata)
        table_descriptions = backup_metadata_dict['Tables']
        scaling_policies = backup_metadata_dict['ScalingPolicies']
        scalable_targets = backup_metadata_dict['ScalableTargets']

        hippolyte.dynamodb_booster.logger = logger_mock
        booster = hippolyte.dynamodb_booster.DynamoDbBooster(table_descriptions, 'foo', 0.5)
        autoscaling_util = booster.application_auto_scaling_util

        for policy in scaling_policies:
            autoscaling_util.put_scaling_policy(policy['PolicyName'],
                                                policy['ServiceNamespace'],
                                                policy['ResourceId'],
                                                'NonExisting',
                                                policy['PolicyType'],
                                                policy['TargetTrackingScalingPolicyConfiguration'])

        for target in scalable_targets:
            autoscaling_util.register_scalable_target(target['ServiceNamespace'],
                                                      target['ResourceId'],
                                                      'NonExisting',
                                                      target['MinCapacity'],
                                                      target['MaxCapacity'],
                                                      target['RoleARN'])

        booster.disable_auto_scaling(scaling_policies, scalable_targets)
        self.assertEqual(logger_mock.warn.call_count, 2)
