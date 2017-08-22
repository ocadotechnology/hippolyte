from __future__ import print_function
import logging
import re

from botocore.exceptions import ClientError
from hippolyte.aws_utils import DataPipelineUtil, DynamoDBUtil
from hippolyte.config_util import ConfigUtil
from hippolyte.monitor import Monitor
from hippolyte.pipeline_scheduler import Scheduler
from hippolyte.dynamodb_booster import DynamoDbBooster
from hippolyte.utils import MAX_DURATION_SINGLE_PIPELINE, INITIAL_READ_THROUGHPUT_PERCENT, list_tables_in_definition
from hippolyte.project_config import ACCOUNT_CONFIGS

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def _extract_from_arn(arn, position):
    """
    Helper Function to extract part of an ARN

    :param arn: Arn to extract from
    :param position: Position in Arn of interest
    :return: String containing value at requested position
    """

    return re.findall("(.*?):", arn)[position]


def get_table_descriptions(exclude_from_backup):
    dynamo_db_util = DynamoDBUtil()
    table_names = dynamo_db_util.list_tables()
    tables_filtered = set()
    patterns = map(lambda x: re.compile(x), exclude_from_backup)

    for table_name in table_names:
        should_be_added = True
        for pattern in patterns:
            m = pattern.match(table_name)

            if m:
                should_be_added = False
                break

        if should_be_added:
            tables_filtered.add(table_name)

    return dynamo_db_util.describe_tables(tables_filtered)


def get_account(context):
    return _extract_from_arn(context.invoked_function_arn, 4)


def get_sns_endpoint(context):
    region = _extract_from_arn(context.invoked_function_arn, 3)
    return 'arn:aws:sns:{}:{}:hippolyte-backup-monitoringbackup'.format(region, get_account(context))


def detect_action(event):
    resources = event.get("resources", [])

    for resource in resources:
        if resource.endswith('monitor-dynamodb-backup'):
            return monitor

    return backup


def backup(**kwargs):
    logger.info("Performing full DynamoDB backup task.")
    logger.info("Building pipeline definitions")
    scheduler = Scheduler(kwargs['table_descriptions'], 'multiple.template', kwargs['emr_subnet'],
                          kwargs['region'], kwargs['backup_bucket'], kwargs['log_bucket'])
    pipeline_definitions = scheduler.build_pipeline_definitions()

    logger.info("Creating pipelines.")
    pipeline_descriptions = []
    for definition in pipeline_definitions:
        created = True

        try:
            response = kwargs['pipeline_util'].create_pipeline()
        except ClientError as e:
            if e.message == 'LimitExceededException':
                logger.warn("Can't create more pipelines, as account limit exceeded. Details: {}"
                      .format(e.message))

            created = False
            logger.warn("Can't create more pipelines. Details: {}".format(e.message))

        if created:
            pipeline_descriptions.append(
                {
                    'pipeline_id': response.get("pipelineId"),
                    'backed_up_tables': list_tables_in_definition(definition),
                    'definition': definition
                }
            )

    logger.info("Updating throughputs, to meet Time Point Objective.")
    kwargs['dynamodb_booster'].boost_throughput(pipeline_descriptions, MAX_DURATION_SINGLE_PIPELINE)

    for description in pipeline_descriptions:
        pipeline_id = description["pipeline_id"]
        pipeline_definition = description["definition"]

        logger.info("Deploying pipeline definition to {}".format(pipeline_id))
        kwargs['pipeline_util'].put_pipeline_definition(pipeline_id, pipeline_definition)

        logger.info("Activating pipeline: {}".format(pipeline_id))
        kwargs['pipeline_util'].activate_pipeline(pipeline_id, pipeline_definition)

    logger.info("Finished dynamo db backup.")


def monitor(**kwargs):
    logger.info("Performing monitoring only this time.")
    logger.info("Restoring original throughputs.")
    kwargs['dynamodb_booster'].restore_throughput()

    finished_pipelines = ConfigUtil().list_finished_pipelines(kwargs['backup_bucket'])
    for pipeline_id in finished_pipelines:
        logger.info("Deleting finished pipeline: {}".format(pipeline_id))
        kwargs['pipeline_util'].delete_pipeline(pipeline_id)

    logger.info("Looking for failed backups.")
    monitor = Monitor(kwargs['account'], kwargs['log_bucket'], kwargs['backup_bucket'], kwargs['sns_endpoint'])
    monitor.notify_about_failures(finished_pipelines)


def lambda_handler(event, context):
    account_id = get_account(context)

    if account_id not in ACCOUNT_CONFIGS:
        logger.error("Couldn't find configuration for {} in project_config.py.".format(account_id))
        return

    account_config = ACCOUNT_CONFIGS[account_id]
    exclude_from_backup = account_config['exclude_from_backup']

    logger.info("Describing tables in the account.")
    table_descriptions = get_table_descriptions(exclude_from_backup)

    action = detect_action(event)
    action(**{
        'table_descriptions': table_descriptions,
        'pipeline_util': DataPipelineUtil(),
        'dynamodb_booster': DynamoDbBooster(table_descriptions,
                                            account_config['backup_bucket'],
                                            INITIAL_READ_THROUGHPUT_PERCENT),
        'account': account_id,
        'log_bucket': account_config['log_bucket'],
        'sns_endpoint': get_sns_endpoint(context),
        'backup_bucket': account_config['backup_bucket'],
        'emr_subnet': account_config['emr_subnet'],
        'region': _extract_from_arn(context.invoked_function_arn, 3)
    })

# Uncomment to test monitor phase:
# class Context(object):
#     def __init__(self):
#         self.invoked_function_arn = "a:b:c:eu-west-1:274670120741:e"
#
# lambda_handler({'resources': ['monitor-dynamodb-backup']}, Context())

# Uncomment to test backup phase
# lambda_handler({}, Context())
