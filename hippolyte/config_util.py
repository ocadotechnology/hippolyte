__author__ = "roman.subik"

from hippolyte.aws_utils import S3Util, DataPipelineUtil
from hippolyte.utils import get_date_suffix
import logging

COMMON_PREFIX = 'backup_metadata'
DONE_STATES = ["CANCELED", "CASCADE_FAILED", "FAILED", "FINISHED", "INACTIVE", "PAUSED", "SKIPPED", "TIMEDOUT"]

logger = logging.getLogger()
logger.setLevel(logging.INFO)


class ConfigUtil(object):
    def __init__(self):
        self.s3_util = S3Util()
        self.data_pipeline_util = DataPipelineUtil()

    def save_configuration(self, pipeline_definitions, backup_bucket, table_descriptions,
                           scaling_policies, scalable_targets):
        self.s3_util.put_json(
            backup_bucket, self._get_metadata_file_name(),
            {
                "Tables": table_descriptions,
                "Pipelines": pipeline_definitions,
                "ScalingPolicies": scaling_policies,
                "ScalableTargets": scalable_targets
            }
        )

    def load_configuration(self, backup_bucket):
        contents = self.s3_util.list_objects(
            backup_bucket, COMMON_PREFIX
        ).get("Contents", [])

        contents = sorted(contents, key=lambda x: x['LastModified'], reverse=True)

        if contents:
            return self.s3_util.get_json(
                backup_bucket, contents[0].get('Key')
            )
        else:
            return

    def _get_metadata_file_name(self):
        return '{}-{}'.format(COMMON_PREFIX, get_date_suffix())

    def list_backed_up_tables(self, pipelines, backup_bucket):
        finished_pipelines = self.list_finished_pipelines(backup_bucket, pipelines)
        backed_up_tables = []

        for pipeline in pipelines:
            if pipeline['pipeline_id'] in finished_pipelines:
                backed_up_tables += pipeline['backed_up_tables']

        return backed_up_tables

    def list_finished_pipelines(self, backup_bucket=None, backup_pipelines=None):
        if not backup_pipelines:
            last_configuration = self.load_configuration(backup_bucket)

            if last_configuration:
                backup_pipelines = last_configuration['Pipelines']

        if not backup_pipelines:
            logger.error("Couldn't find any backed up tables. Has your backup ran?")
            return []

        backup_pipeline_names = map(lambda x: x['pipeline_id'], backup_pipelines)
        pipelines = self.data_pipeline_util.describe_pipelines()
        finished_pipelines = []

        for pipeline in pipelines:
            fields = pipeline["fields"]
            pipeline_id = pipeline["pipelineId"]

            logger.info("Checking pipeline {}".format(str(pipeline_id)))

            if pipeline_id not in backup_pipeline_names:
                continue

            for field in fields:
                if field["key"] != "@pipelineState":
                    continue
                if field["stringValue"] in DONE_STATES:
                    logger.info("Pipeline {} state is in DONE_STATES.".format(str(pipeline_id)))
                    logger.debug(str(pipeline))
                    finished_pipelines.append(pipeline_id)

        return finished_pipelines
