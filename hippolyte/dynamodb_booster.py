from __future__ import print_function
import logging
from botocore.exceptions import ClientError
from hippolyte.aws_utils import ApplicationAutoScalingUtil, DataPipelineUtil, DynamoDBUtil
from hippolyte.config_util import ConfigUtil
from hippolyte.utils import ACTIVITY_BOOTSTRAP_TIME, EMR_BOOTSTRAP_TIME, MAX_DURATION_SEC, \
    MAX_ALLOWED_PROVISIONED_READ_THROUGHPUT, INITIAL_READ_THROUGHPUT_PERCENT, \
    estimate_backup_duration, compute_required_throughput, get_first_element_in_the_list_with

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
        self.application_auto_scaling_util = ApplicationAutoScalingUtil()

    def boost_throughput(self, pipeline_descriptions, desired_backup_duration):
        scaling_policies = self.list_dynamodb_scaling_policies()
        scalable_targets = self.list_dynamodb_scalable_targets()
        self.config_util.save_configuration(pipeline_descriptions, self.backup_bucket, self.table_descriptions,
                                            scaling_policies, scalable_targets)
        self.disable_auto_scaling(scaling_policies, scalable_targets)

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

        self._restore_all_tables(last_configuration)

        self.reenable_auto_scaling(last_configuration)

    def _restore_all_tables(self, last_configuration):
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

    def disable_auto_scaling(self, scaling_policies, scalable_targets):
        logger.info("Disabling autoscaling on backed up tables, for backup duration.")

        for table in self.table_descriptions:
            table_name = table.get('Table', {}).get('TableName')
            resource_id = "table/{}".format(table_name)

            read_scaling_policy = get_first_element_in_the_list_with(scaling_policies, 'ResourceId', resource_id)

            if read_scaling_policy:
                logger.info("Removing scaling policy: {}".format(read_scaling_policy['PolicyName']))
                self.application_auto_scaling_util. \
                    delete_scaling_policy(read_scaling_policy['PolicyName'], "dynamodb",
                                          resource_id, "dynamodb:table:ReadCapacityUnits")

            read_scalable_target = get_first_element_in_the_list_with(scalable_targets, 'ResourceId', resource_id)

            if read_scalable_target:
                logger.info("Removing scalable target for: {}".format(resource_id))
                self.application_auto_scaling_util. \
                    deregister_scalable_target("dynamodb", resource_id, "dynamodb:table:ReadCapacityUnits")

    def reenable_auto_scaling(self, last_configuration):
        logger.info("Reenabling autoscaling tables after backup.")
        scalable_targets = last_configuration['ScalableTargets']
        scaling_policies = last_configuration['ScalingPolicies']

        for target in scalable_targets:
            logger.info("Adding scalable target for: {}".format(target['ResourceId']))
            self.application_auto_scaling_util.register_scalable_target(target['ServiceNamespace'],
                                                                        target['ResourceId'],
                                                                        target['ScalableDimension'],
                                                                        target['MinCapacity'],
                                                                        target['MaxCapacity'],
                                                                        target['RoleARN'])

        for policy in scaling_policies:
            logger.info("Adding scaling policy: {}".format(policy['PolicyName']))
            self.application_auto_scaling_util.put_scaling_policy(policy['PolicyName'],
                                                                  policy['ServiceNamespace'],
                                                                  policy['ResourceId'],
                                                                  policy['ScalableDimension'],
                                                                  policy['PolicyType'],
                                                                  policy['TargetTrackingScalingPolicyConfiguration'])

    def list_dynamodb_scalable_targets(self):
        targets = self.application_auto_scaling_util \
            .describe_scalable_targets("dynamodb").get('ScalableTargets', [])
        return self._only_return_rcu_dimension(targets)

    def list_dynamodb_scaling_policies(self):
        policies = self.application_auto_scaling_util \
            .describe_scaling_policies("dynamodb").get('ScalingPolicies', [])
        return self._only_return_rcu_dimension(policies)

    def _only_return_rcu_dimension(self, _list):
        return filter(lambda x: x.get('ScalableDimension') == 'dynamodb:table:ReadCapacityUnits', _list)
