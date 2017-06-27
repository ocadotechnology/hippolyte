from datetime import datetime
import json
from uuid import uuid4
import boto3
from botocore.exceptions import ClientError
from retrying import retry
import hippolyte.pipeline_translator as pipeline_translator
from hippolyte.utils import chunks


def retry_if_throttling_error(exception):
    if isinstance(exception, ClientError):
        return 'Throttling' in exception.message or 'limit exceeded' in exception.message

    return False


class DataPipelineUtil(object):
    def __init__(self):
        self.client = boto3.client('datapipeline')

    @retry(retry_on_exception=retry_if_throttling_error,
           wait_exponential_multiplier=1000,
           stop_max_attempt_number=5)
    def create_pipeline(self, name=None):
        if not name:
            name = "dynamodb-backup-" + datetime.now().strftime("%Y-%m-%d-%H-%M-%S")

        return self.client.create_pipeline(
            name=name,
            uniqueId=str(uuid4()),
            description="Used to do automatic DynamoDB backups.",
            tags=[
                {
                    "key": "app",
                    "value": "hippolyte-datapipeline"
                }
            ]
        )

    @retry(retry_on_exception=retry_if_throttling_error,
           wait_exponential_multiplier=1000,
           stop_max_attempt_number=5)
    def put_pipeline_definition(self, pipeline_id, definition):
        return self.client.put_pipeline_definition(
            pipelineId=pipeline_id,
            pipelineObjects=pipeline_translator.definition_to_api_objects(definition),
            parameterObjects=pipeline_translator.definition_to_api_parameters(definition),
            parameterValues=pipeline_translator.definition_to_parameter_values(definition)
        )

    @retry(retry_on_exception=retry_if_throttling_error,
           wait_exponential_multiplier=1000,
           stop_max_attempt_number=5)
    def activate_pipeline(self, pipeline_id, definition):
        return self.client.activate_pipeline(
            pipelineId=pipeline_id,
            parameterValues=pipeline_translator.definition_to_parameter_values(definition)
        )

    @retry(retry_on_exception=retry_if_throttling_error,
           wait_exponential_multiplier=1000,
           stop_max_attempt_number=5)
    def list_pipelines(self):
        pipelines = []
        paginator = self.client.get_paginator('list_pipelines')
        page_iterator = paginator.paginate(PaginationConfig={
            'MaxItems': 1000
        })

        for page in page_iterator:
            pipelines += page.get("pipelineIdList", [])

        return pipelines

    @retry(retry_on_exception=retry_if_throttling_error,
           wait_exponential_multiplier=1000,
           stop_max_attempt_number=5)
    def describe_pipelines(self):
        pipeline_list = self.list_pipelines()
        pipeline_ids = map(lambda x: x['id'], pipeline_list)
        pipeline_ids_chunked = list(chunks(pipeline_ids, 25))
        pipeline_descriptions = []

        for pipeline_id in pipeline_ids_chunked:
            descriptions = self.client.describe_pipelines(pipelineIds=pipeline_id)
            pipeline_descriptions += descriptions['pipelineDescriptionList']

        return pipeline_descriptions

    @retry(retry_on_exception=retry_if_throttling_error,
           wait_exponential_multiplier=1000,
           stop_max_attempt_number=5)
    def delete_pipeline(self, pipeline_id):
        self.client.delete_pipeline(pipelineId=pipeline_id)


class DynamoDBUtil(object):
    def __init__(self):
        self.client = boto3.client('dynamodb')

    @retry(retry_on_exception=retry_if_throttling_error,
           wait_exponential_multiplier=1000,
           stop_max_attempt_number=5)
    def list_tables(self):
        tables = []
        paginator = self.client.get_paginator('list_tables')
        page_iterator = paginator.paginate(PaginationConfig={
            'MaxItems': 10000,
            'PageSize': 100
        })

        for page in page_iterator:
            tables += page.get("TableNames", [])

        return tables

    @retry(retry_on_exception=retry_if_throttling_error,
           wait_exponential_multiplier=1000,
           stop_max_attempt_number=5)
    def describe_table(self, table_name):
        return self.client.describe_table(TableName=table_name)

    def describe_tables(self, table_names):
        table_descriptions = []

        for table_name in table_names:
            table_descriptions.append(self.describe_table(table_name))

        return table_descriptions

    @retry(retry_on_exception=retry_if_throttling_error,
           wait_exponential_multiplier=1000,
           stop_max_attempt_number=5)
    def describe_limits(self):
        return self.client.describe_limits()

    @retry(retry_on_exception=retry_if_throttling_error,
           wait_exponential_multiplier=1000,
           stop_max_attempt_number=5)
    def batch_write_items(self, table_name, items):
        table = self.client.Table(TableName=table_name)

        with table.batch_writer() as batch:
            for item in items:
                batch.put_item(Item=item)

    @retry(retry_on_exception=retry_if_throttling_error,
           wait_exponential_multiplier=1000,
           stop_max_attempt_number=5)
    def update_item(self, table_name, key, update_expression, expression_attribute_values):
        table = self.client.Table(TableName=table_name)
        table.update_item(Key=key, UpdateExpression=update_expression,
                          ExpressionAttributeValues=expression_attribute_values)

    @retry(retry_on_exception=retry_if_throttling_error,
           wait_exponential_multiplier=1000,
           stop_max_attempt_number=5)
    def change_capacity_units(self, table_name, new_read_throughput=None, new_write_throughput=None):
        table_description = self.describe_table(table_name).get('Table', {})

        throughput, requires_update = self._get_adjusted_throughput(table_description,
                                                                    new_read_throughput, new_write_throughput)

        if requires_update:
            self.client.update_table(TableName=table_name, ProvisionedThroughput=throughput)

    def _get_adjusted_throughput(self, table_description, new_read_throughput, new_write_throughput):
        current_throughput = table_description.get('ProvisionedThroughput')
        current_read_throughput = current_throughput.get('ReadCapacityUnits')
        current_write_throughput = current_throughput.get('WriteCapacityUnits')

        throughput = {
            'ReadCapacityUnits': current_read_throughput,
            'WriteCapacityUnits': current_write_throughput
        }

        requires_update = False

        if new_read_throughput and (current_read_throughput != new_read_throughput):
            throughput['ReadCapacityUnits'] = new_read_throughput
            requires_update = True

        if new_write_throughput and (current_write_throughput != new_write_throughput):
            throughput['WriteCapacityUnits'] = new_write_throughput
            requires_update = True

        return throughput, requires_update


class S3Util(object):
    def __init__(self):
        self.client = boto3.client('s3')

    @retry(retry_on_exception=retry_if_throttling_error,
           wait_exponential_multiplier=1000,
           stop_max_attempt_number=5)
    def put_json(self, bucket, key, json_file):
        body = json.dumps(json_file, default=lambda o: str(o), sort_keys=True, indent=4)
        self.client.put_object(Bucket=bucket, Key=key, Body=body)

    @retry(retry_on_exception=retry_if_throttling_error,
           wait_exponential_multiplier=1000,
           stop_max_attempt_number=5)
    def get_json(self, bucket, key):
        obj = self.client.get_object(Bucket=bucket, Key=key)
        return json.loads(obj.get('Body').read().decode('utf-8'))

    @retry(retry_on_exception=retry_if_throttling_error,
           wait_exponential_multiplier=1000,
           stop_max_attempt_number=5)
    def list_objects(self, bucket, prefix):
        paginator = self.client.get_paginator('list_objects')
        contents = []
        response = None

        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            response = page
            contents += page.get('Contents', [])

        if response:
            response['Contents'] = contents

        return response

    @retry(retry_on_exception=retry_if_throttling_error,
           wait_exponential_multiplier=1000,
           stop_max_attempt_number=5)
    def object_exists(self, bucket, key):
        try:
            self.client.get_object(Bucket=bucket, Key=key)
        except ClientError as ce:
            if ce.response['Error']['Code'] == "404":
                return False

        return True


class SnsUtil(object):
    def __init__(self):
        self.client = boto3.client('sns')

    @retry(retry_on_exception=retry_if_throttling_error,
           wait_exponential_multiplier=1000,
           stop_max_attempt_number=5)
    def publish(self, sns_topic, subject, message):
        self.client.publish(
            TopicArn=sns_topic,
            Message=message,
            Subject=subject
        )
