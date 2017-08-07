import os
from copy import deepcopy


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