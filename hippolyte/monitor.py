from __future__ import print_function
from datetime import datetime
import logging
from hippolyte.aws_utils import S3Util, SnsUtil
from hippolyte.config_util import ConfigUtil
from hippolyte.utils import TIME_IN_BETWEEN_BACKUPS

logger = logging.getLogger()
logger.setLevel(logging.INFO)


class Monitor(object):
    def __init__(self, account, log_bucket, backup_bucket, sns_endpoint):
        self.account = account
        self.log_bucket = log_bucket
        self.backup_bucket = backup_bucket
        self.sns_endpoint = sns_endpoint
        self.config_util = ConfigUtil()
        self.s3_util = S3Util()
        self.sns_util = SnsUtil()

    def notify_about_failures(self, pipelines):
        configuration = self.config_util.load_configuration(self.backup_bucket)

        if not configuration:
            logger.info("Couldn't find configuration file. Stopping throughput restore process, sending email.")

            email_body = all_failed_backup_email_template.format(
                account=self.account,
                log_bucket = self.log_bucket
            )
            self.send_notification_email(email_body)

        pipeline_failed_tables = {}

        for pipeline_id in pipelines:
            finished_pipeline = filter(lambda x: x['pipeline_id'] == pipeline_id, configuration['Pipelines'])
            failed_tables = []

            if finished_pipeline:
                failed_tables = self.extract_failed_tables(finished_pipeline[0])

            if failed_tables:
                pipeline_failed_tables[finished_pipeline[0]['pipeline_id']] = failed_tables

        if pipeline_failed_tables:
            logger.info('Some tables were not backed up properly: {}'.format(str(pipeline_failed_tables)))
            logger.info('Sending sns notification about failures.')

            email_body = failed_table_backup_email_template.format(
                account=self.account,
                description=create_description(pipeline_failed_tables),
                log_bucket = self.log_bucket
            )
            self.send_notification_email(email_body)

    def extract_failed_tables(self, pipeline):
        objects = pipeline.get('definition', {'objects': []}).get('objects', [])
        s3_attributes = filter(lambda x: 'directoryPath' in x, objects)
        failed_tables = []

        for s3_attribute in s3_attributes:

            protocol, _, bucket, table_name, timestamp = s3_attribute['directoryPath'].split('/')

            backup_archive = self.s3_util.list_objects(
                bucket, table_name
            ).get("Contents", [])

            backup_archive = sorted(backup_archive, key=lambda x: x['LastModified'], reverse=True)

            if not backup_archive:
                failed_tables.append(table_name)
                continue

            success_flag = get_first_success_flag(backup_archive)

            if not success_flag:
                failed_tables.append(table_name)
                continue

            if not is_backup_from_current_batch(success_flag):
                failed_tables.append(table_name)
                continue

        return failed_tables

    def send_notification_email(self, email_body):
        email_subject = email_subject_template.format(account=self.account)
        self.sns_util.publish(self.sns_endpoint, email_subject, email_body)


def get_first_success_flag(backup_dir_contents):
    for content in backup_dir_contents:
        if content['Key'].endswith('_SUCCESS'):
            return content

    return None


failed_table_backup_email_template = """
Hello

You have been notified, as some of tables in {account} account were not backed up in last 24h.
Please find details below:

Pipeline Id: Failed tables

{description}

Please check logs in: {log_bucket} for details.

Best regards,
Hippolyte
"""

email_subject_template = "Failed to backup DynamoDB tables in {account} account."

all_failed_backup_email_template = """
Hello

You have been notified, as DynamoDB backup failed completely in {account}.
I couldn't even find a backup_metadata* file in {log_bucket}.

Best regards,
Hippolyte
"""


def is_backup_from_current_batch(backup_dir):
    last_modified = backup_dir['LastModified']
    return (datetime.now(tz=last_modified.tzinfo) - last_modified).total_seconds() <= TIME_IN_BETWEEN_BACKUPS


def create_description(pipeline_failed_tables):
    table = ""
    for pipeline_id in pipeline_failed_tables:
        table += "{}: {}\n".format(pipeline_id, ",".join(pipeline_failed_tables[pipeline_id]))

    return table
