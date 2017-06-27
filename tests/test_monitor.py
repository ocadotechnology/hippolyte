import unittest
import boto3
import sys
import os
from moto import mock_s3, mock_datapipeline, mock_sns
from datetime import datetime, timedelta

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from hippolyte.monitor import Monitor, is_backup_from_current_batch


class TestESMonitor(unittest.TestCase):
    @mock_sns
    @mock_datapipeline
    @mock_s3
    def test_success_on_large_number_of_backup_files(self):
        bucket = 'euw1-dynamodb-backups-prd-480503113116'
        log_bucket = 'euw1-infrastructure_logs-prd-480503113116'
        s3 = boto3.client('s3', region_name='eu-west-1')
        s3.create_bucket(Bucket=bucket)

        for day in range(1, 30):
            key = 'prd-shd-euw1-scotty_audit-events/2099-05-{}-00-10-38/'.format(str(day).zfill(2))
            for file_name in range(0, 100):
                s3.put_object(Bucket=bucket, Key=key + str(file_name), Body='')

            s3.put_object(Bucket=bucket, Key=key + '_SUCCESS', Body='')

        dummy_pipeline = {
            "definition": {
                "objects": [
                    {
                        "directoryPath": "s3://euw1-dynamodb-backups-prd-480503113116/prd-shd-euw1-scotty_audit-events/#{format(@scheduledStartTime, 'YYYY-MM-dd-HH-mm-ss')}"
                    }
                ]
            }
        }

        monitor = Monitor('480503113116', log_bucket, bucket, 'dummy_sns')
        failed_tables = monitor.extract_failed_tables(dummy_pipeline)

        self.assertFalse(failed_tables)

    def test_is_backup_from_current_batch_success(self):
        last_modified = datetime.utcnow()
        self.assertTrue(is_backup_from_current_batch({'LastModified': last_modified}))

    def test_is_backup_from_current_batch_failure(self):
        last_modified = datetime.utcnow() - timedelta(hours=26)
        self.assertFalse(is_backup_from_current_batch({'LastModified': last_modified}))