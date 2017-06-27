from __future__ import print_function

import json
import logging
import math
import os
import pystache

from hippolyte.utils import EMR_BOOTSTRAP_TIME, MAX_DURATION_SEC, ACTIVITY_BOOTSTRAP_TIME, \
    MAX_TABLES_PER_PIPELINE, INITIAL_READ_THROUGHPUT_PERCENT, estimate_backup_duration, get_date_suffix

logger = logging.getLogger()
logger.setLevel(logging.INFO)


CLUSTER_CONFIGS = [
    {
        'masterInstanceType': 'm1.medium',
        'coreInstanceType': 'm1.medium',
        'coreInstanceCount': 1,
        'clusterMemory': '--yarn-key-value,yarn.nodemanager.resource.memory-mb=2048,'
                         '--yarn-key-value,yarn.scheduler.maximum-allocation-mb=2048,'
                         '--yarn-key-value,yarn.scheduler.minimum-allocation-mb=256,'
                         '--yarn-key-value,yarn.app.mapreduce.am.resource.mb=1024,'
                         '--mapred-key-value,mapreduce.map.memory.mb=768,'
                         '--mapred-key-value,mapreduce.map.java.opts=-Xmx512M,'
                         '--mapred-key-value,mapreduce.reduce.memory.mb=1024,'
                         '--mapred-key-value,mapreduce.reduce.java.opts=-Xmx768m,'
                         '--mapred-key-value,mapreduce.map.speculative=false',
        'maxTotalDynamoDbSizeBytes': 597688320  # 570MB
    },
    {
        'masterInstanceType': 'm3.xlarge',
        'coreInstanceType': 'm3.xlarge',
        'coreInstanceCount': 1,
        'clusterMemory': '--yarn-key-value,yarn.nodemanager.resource.memory-mb=11520,'
                         '--yarn-key-value,yarn.scheduler.maximum-allocation-mb=11520,'
                         '--yarn-key-value,yarn.scheduler.minimum-allocation-mb=1440,'
                         '--yarn-key-value,yarn.app.mapreduce.am.resource.mb=2880,'
                         '--mapred-key-value,mapreduce.map.memory.mb=5760,'
                         '--mapred-key-value,mapreduce.map.java.opts=-Xmx4608M,'
                         '--mapred-key-value,mapreduce.reduce.memory.mb=2880,'
                         '--mapred-key-value,mapreduce.reduce.java.opts=-Xmx2304m,'
                         '--mapred-key-value,mapreduce.map.speculative=false',
        'maxTotalDynamoDbSizeBytes': 1099511627776000  # 1PB
    }
]


class Scheduler(object):
    def __init__(self, table_descriptions, template_file, subnet_id, region,
                 s3_backup_bucket, s3_pipeline_log_bucket, max_retries=2,
                 read_throughput_percent=INITIAL_READ_THROUGHPUT_PERCENT):
        """
        :param table_descriptions: descriptions, as returned from DynamoDBUtil.describe_tables()
        :param template_file: path to template file
        :param read_throughput_percent: how much read throughput should be used for backing up, ex. 0.5 - 50%
        :param subnet_id: EMR subnet
        :param region:
        :param s3_backup_bucket: S3 location, where backup files go
        :param s3_pipeline_log_bucket: S3 location, where pipeline logs go
        :param max_retries: how many times to retry pipeline execution on error, before giving up
        :return:
        """
        self.table_descriptions = table_descriptions
        self.template_file = template_file
        self.read_throughput_percent = read_throughput_percent
        self.subnet_id = subnet_id
        self.region = region
        self.s3_backup_bucket = s3_backup_bucket
        self.s3_pipeline_log_bucket = s3_pipeline_log_bucket
        self.max_retries = max_retries
        self.s3_log_location = '{}/logs/{}'.format(s3_pipeline_log_bucket, get_date_suffix())
        self.terminate_after = int(math.ceil(MAX_DURATION_SEC / 3600.0)) + 1

    def build_pipeline_definitions(self):
        """
        Creates list of pipeline definitions, which could be use to populate data pipelines.
        Does it by combining template with parameter list.
        :return: list of pipeline definitions
        """
        data_pipelines = []

        template = self.read_template()
        dp_parameters = self.build_parameters()

        for parameters in dp_parameters:
            data_pipelines.append(json.loads(pystache.render(template, parameters)))

        return data_pipelines

    def read_template(self):
        template_file = os.path.join(os.path.dirname(__file__), self.template_file)
        with open(template_file, "r") as f:
            return f.read()

    def build_parameters(self):
        """
        Builds list of parameters, describing dynamo db backup process on a single data pipeline.
        Performs scheduling, in terms of what tables to assign to pipelines, to achieve close execution time,
        on all pipelines.
        :return: list of parameters for single data pipeline
        """
        data_pipeline_parameters = []
        total_duration = EMR_BOOTSTRAP_TIME
        backups = []
        table_counter = 0
        table_index = 0
        total_table_size = 0
        table_backup_durations = self.build_table_backup_durations()

        for table_name, backup_duration, table_size_bytes in table_backup_durations:
            total_duration += backup_duration

            backups.append(self.create_backup_parameters(table_counter, table_name))
            table_counter += 1
            total_table_size += table_size_bytes

            if not self.should_add_more_tables(table_index, total_duration,
                                               table_backup_durations, backups):
                backups = self.normalize_backup_parameters(backups)

                data_pipeline_parameters.append(self.create_pipeline_parameters(backups, total_table_size))

                logger.info('Total estimated duration of pipeline execution: {}'.format(total_duration))

                total_duration = EMR_BOOTSTRAP_TIME
                backups = []
                table_counter = 0
                total_table_size = 0

            table_index += 1

        return data_pipeline_parameters

    def build_table_backup_durations(self):
        """
        Describes dynamo db tables in the account and assigns estimated duration time to each one of those.
        Tables with 0 size will not be backed up.
        :return: list of dynamo db table descriptions, sorted by ascending estimated backup duration
        """
        table_backup_duration = []
        for description in self.table_descriptions:
            table = description['Table']
            duration = self.estimate_duration(table)

            if table['TableSizeBytes']:
                table_backup_duration.append((table['TableName'], duration, table['TableSizeBytes']))
            else:
                logger.info("Skipping {} as it appears to be empty.".format(table["TableName"]))

        return sorted(table_backup_duration, key=lambda x: x[1])

    def create_pipeline_parameters(self, backups, total_table_size):
        """
        :param backups: list of elements, as returned from create_backup_parameters
        :param max_table_size: sizew in bytes of the biggest table currently backed up
        :return: list of parameters needed for data pipeline Config and EMRCluster nodes
        """
        cluster_config = None

        for config in CLUSTER_CONFIGS:
            if total_table_size < config['maxTotalDynamoDbSizeBytes']:
                cluster_config = config
                break

        return {
            'subnetId': '{}'.format(self.subnet_id),
            'coreInstanceCount': cluster_config['coreInstanceCount'],
            'coreInstanceType': cluster_config['coreInstanceType'],
            'masterInstanceType': cluster_config['masterInstanceType'],
            'clusterMemory': cluster_config['clusterMemory'],
            'region': '{}'.format(self.region),
            'terminateAfter': '{} Hour'.format(self.terminate_after),
            's3BackupBucket': '{}'.format(self.s3_backup_bucket),
            's3PipelineLogBucket': '{}'.format(self.s3_log_location),
            'backups': backups
        }

    def create_backup_parameters(self, table_counter, table_name):
        """
        :param table_counter:
        :param table_name:
        :return: list of parameters, needed for backing up single dynamo db table.
        """
        return {'dbSourceTableReadThroughputPercent': '{}'.format(self.read_throughput_percent),
                'dbSourceTableName': 'DDBSourceTable{}'.format(table_counter),
                'dbSourceTableId': 'DDBSourceTable{}'.format(table_counter),
                'dynamoDBTableName': table_name,
                's3BackupLocationId': 'S3BackupLocation{}'.format(table_counter),
                's3BackupLocationName': 'S3BackupLocation{}'.format(table_counter),
                'tableBackupActivityMaximumRetries': '{}'.format(self.max_retries),
                'tableBackupActivityName': 'TableBackupActivity{}'.format(table_counter),
                'tableBackupActivityId': 'TableBackupActivity{}'.format(table_counter),
                'region': '{}'.format(self.region),
                'comma': True}

    def normalize_backup_parameters(self, backups):
        if backups:
            backups[-1]['comma'] = False

        return backups

    def should_add_more_tables(self, table_index, total_duration, table_backup_durations, backups):
        """
        Checks whether or not more tables should be backed up on current data pipeline
        :param table_index:
        :param total_duration:
        :param table_backup_durations:
        :param backups:
        :return:
        """
        add_more_tables = True

        if table_index + 1 < len(table_backup_durations):
            if total_duration + table_backup_durations[table_index + 1][1] >= MAX_DURATION_SEC:
                add_more_tables = False

            if len(backups) >= MAX_TABLES_PER_PIPELINE:
                add_more_tables = False
        else:
            add_more_tables = False

        return add_more_tables

    def estimate_duration(self, data):
        """
        Gives rough estimate, on how long backing up dynamo db table will take.
        :param data: dynamic dynamo db table definition
        :return: Estimated time in seconds.
        """
        table_size_bytes = data.get('TableSizeBytes', 0)
        read_capacity_units = data['ProvisionedThroughput']['ReadCapacityUnits']

        return estimate_backup_duration(self.read_throughput_percent, table_size_bytes,
                                        read_capacity_units) + ACTIVITY_BOOTSTRAP_TIME
