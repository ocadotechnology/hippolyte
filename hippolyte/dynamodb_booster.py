from __future__ import print_function
import logging
from botocore.exceptions import ClientError
from hippolyte.aws_utils import DataPipelineUtil, DynamoDBUtil
from hippolyte.config_util import ConfigUtil
from hippolyte.utils import ACTIVITY_BOOTSTRAP_TIME, EMR_BOOTSTRAP_TIME, MAX_DURATION_SEC, \
    MAX_ALLOWED_PROVISIONED_READ_THROUGHPUT, INITIAL_READ_THROUGHPUT_PERCENT, \
    estimate_backup_duration, compute_required_throughput

logger = logging.getLogger()
logger.setLevel(logging.INFO)


class DynamoDbBooster(object):
    def __init__(self, table_descriptions, backup_bucket, read_throughput_percent):
        self.table_descriptions = table_descriptions
        self.backup_bucket = backup_bucket
        self.read_throughput_percent = read_throughput_percent
        self.dynamo_db_util = DynamoDBUtil()
        self.config_util = ConfigUtil()
        self.data_pipeline_util = DataPipelineUtil()

    def boost_throughput(self, pipeline_descriptions, desired_backup_duration):
        self.config_util.save_configuration(pipeline_descriptions, self.backup_bucket, self.table_descriptions)
        limits = self.dynamo_db_util.describe_limits()
        total_increase = 0

        pipeline_definitions = map(lambda x: x['definition'], pipeline_descriptions)

        for nodes in pipeline_definitions:
            total_increase += self._boost_single_pipeline(nodes.get('objects'), desired_backup_duration, limits)

        logger.info("Total throughput increase: {}".format(total_increase))

    def restore_throughput(self):
        last_configuration = self.config_util.load_configuration(self.backup_bucket)

        if not last_configuration:
            logger.error("Couldn't find configuration file. Stopping throughput restore process.")
            return

        pipelines = last_configuration['Pipelines']
        tables = last_configuration['Tables']

        backed_up_tables = self.config_util.list_backed_up_tables(pipelines, self.backup_bucket)
        previous_table_state = filter(lambda x: 'TableArn' in x['Table'], tables)
        current_table_state = filter(lambda x: 'TableArn' in x['Table'], self.table_descriptions)

        logger.debug("Previous table state: {}".format(str(previous_table_state)))
        logger.debug("Current table state: {}".format(str(current_table_state)))

        for previous_state in previous_table_state:
            previous_name, previous_throughput = self._get_name_and_capacity(previous_state)

            if previous_name not in backed_up_tables:
                continue

            for current_state in current_table_state:
                current_name, current_throughput = self._get_name_and_capacity(current_state)
                logger.debug("current_name:{}, current_throughput:{}, previous_name:{}, previous_throughput:{}"
                             .format(current_name, current_throughput, previous_name, previous_throughput))

                if current_name == previous_name and current_throughput != previous_throughput:
                    logger.info("Decreasing throughput of {} from {} to {}.".format(
                        current_name, current_throughput, previous_throughput))

                    try:
                        self.dynamo_db_util.change_capacity_units(current_name, previous_throughput)
                    except ClientError as e:
                        if 'decreased' in e.message:
                            logger.error("Can't decrease throughput of {}, max number of decreases for 24h reached."
                                  .format(current_name))
                        else:
                            logger.error("Can't decrease throughput of {}, reason: ".format(e.message))

    def _boost_single_pipeline(self, nodes, desired_backup_duration, limits):
        dynamo_db_nodes = filter(lambda x: 'tableName' in x, nodes)
        bootstrap_duration = EMR_BOOTSTRAP_TIME + ACTIVITY_BOOTSTRAP_TIME * len(dynamo_db_nodes)
        max_backup_duration = MAX_DURATION_SEC - bootstrap_duration
        total_backup_duration = 0
        table_durations = []
        total_increase = 0

        for node in dynamo_db_nodes:
            table_description = filter(lambda x: x.get('Table', {}).get('TableName') == node['tableName'],
                                       self.table_descriptions)[0]
            table_size = table_description.get('Table', {}).get('TableSizeBytes')
            read_capacity_units = table_description.get('Table', {}).get('ProvisionedThroughput', {}) \
                .get('ReadCapacityUnits', {})
            duration = estimate_backup_duration(self.read_throughput_percent, table_size, read_capacity_units)
            table_durations.append((node, table_description, read_capacity_units, duration))
            total_backup_duration += duration

        if total_backup_duration <= max_backup_duration:
            return total_increase

        for node, description, read_capacity_units, duration in table_durations:
            target_duration = float(duration) * desired_backup_duration / total_backup_duration
            new_read_capacity_units, new_throughput_percent = compute_required_throughput(
                duration, target_duration, read_capacity_units, INITIAL_READ_THROUGHPUT_PERCENT)

            read_limit = min(MAX_ALLOWED_PROVISIONED_READ_THROUGHPUT, limits['TableMaxReadCapacityUnits'])

            if new_read_capacity_units > read_limit:
                logger.error("Can't meet RTO for {} as max table read capacity limit is {}, conntact aws support, "
                      "to increase it. ".format(node['tableName'], read_limit))
                new_read_capacity_units = read_limit

            logger.info("Increasing throughput of {} from {} to {}.".format(
                node['tableName'], read_capacity_units, new_read_capacity_units))
            node['readThroughputPercent'] = str(new_throughput_percent)

            try:
                self.dynamo_db_util.change_capacity_units(node['tableName'], new_read_capacity_units)
            except ClientError as e:
                if e.error_code == 'LimitExceededException':
                    logger.error("Can't meet RTO for {} as max account read capacity limit exceeded. Details: {}"
                          .format(node['tableName'], e.error_message))
                    new_read_capacity_units = read_capacity_units

            total_increase += new_read_capacity_units - read_capacity_units

        return total_increase

    def _get_name_and_capacity(self, state):
        table = state.get('Table', {})
        name = table.get('TableName', '')
        throughput = table.get('ProvisionedThroughput', {}).get('ReadCapacityUnits')

        return name, throughput
